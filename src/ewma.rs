//! `ewma` policy.
//!
//! Peak-EWMA + Power-of-Two-Choices, in the spirit of ingress-nginx's
//! Lua implementation. Each peer carries an exponentially-weighted
//! moving average of its observed RTT (decayed continuously toward
//! zero with a 10-second time constant). On every request we sample
//! two random eligible peers and route to the one with the lower
//! score; the winner's EWMA is updated when the request completes.
//!
//! State lives in our own heap allocations: per-cycle `EwmaSlot`
//! tables hung off `BalancerConfig.ewma` (allocated from `cf->pool`
//! during `init_upstream`), plus a per-request wrapper around
//! `round_robin`'s `rr_peer_data_t` (allocated from `r->pool`).
//! EWMA history is per worker process and does not survive a config
//! reload — out of scope for v1.

use core::{ffi::c_void, ptr};

use ngx::{
    core::{Pool, Status},
    ffi::{
        ngx_cached_time, ngx_conf_t, ngx_current_msec, ngx_http_add_variable,
        ngx_http_upstream_free_round_robin_peer, ngx_http_upstream_get_round_robin_peer,
        ngx_http_upstream_init_pt, ngx_http_upstream_init_round_robin,
        ngx_http_upstream_init_round_robin_peer, ngx_http_upstream_rr_peer_data_t,
        ngx_http_upstream_rr_peer_t, ngx_http_upstream_rr_peers_t, ngx_http_upstream_srv_conf_t,
        ngx_http_variable_t, ngx_int_t, ngx_module_t, ngx_msec_t, ngx_peer_connection_t,
        ngx_random, ngx_shared_memory_add, ngx_shm_zone_t, ngx_slab_calloc, ngx_slab_calloc_locked,
        ngx_slab_free, ngx_slab_pool_t, ngx_str_t, ngx_uint_t, ngx_variable_value_t, sockaddr,
        socklen_t, time_t, NGX_PEER_FAILED,
    },
    http::{HttpModuleServerConf, Request},
    http_upstream_init_peer_pt, http_variable_get, ngx_log_debug_http, ngx_log_debug_mask,
    ngx_string,
};

use crate::{
    peer::{
        busy_with_primary_name, clear_cached_connection, config_mismatch, peer_available_ref,
        peers_single, select_peer, switch_peers_and_clear_tried, sync_config_generation,
        PeersWriteGuard,
    },
    policy::BalancingPolicy,
    Balancer, PolicyImpl,
};

/// EWMA decay time constant in milliseconds. A sample 10s after the
/// previous one contributes ~63% to the new score; one a millisecond
/// later moves the score by ~0.01%. Matches ingress-nginx's
/// `DECAY_TIME = 10` (seconds) — converted once here so the hot
/// `decay_score` path stays in `f64` ms.
const DECAY_MSEC: f64 = 10_000.0;

#[derive(Clone, Copy, Debug)]
#[repr(C)]
pub struct Ewma {
    config: *mut EwmaConfig,
}

impl Ewma {
    pub(crate) fn new() -> Self {
        Self {
            config: ptr::null_mut(),
        }
    }
}

impl BalancingPolicy for Ewma {
    fn init_upstream() -> ngx_http_upstream_init_pt {
        Some(init_upstream)
    }
}

#[derive(Clone, Copy)]
#[repr(C)]
struct EwmaSlot {
    /// Identity of the peer this slot belongs to. Sockaddr is the
    /// stable key — peer linked-list positions shift when zone-driven
    /// peer churn removes entries (see
    /// `ngx_http_upstream_zone_module.c:ngx_http_upstream_zone_remove_peer_locked`).
    /// Non-zone configs are static, so the pointer aliases the peer's
    /// own `sockaddr` for the cycle's lifetime.
    sockaddr: *mut sockaddr,
    socklen: socklen_t,
    ewma: f64,
    last_touched_msec: ngx_msec_t,
}

#[derive(Clone, Copy)]
#[repr(C)]
struct EwmaConfig {
    /// Stable handle on the primary peers list. Needed in `peer.free`
    /// after a backup-fallback may have swapped `(*rrp).peers` to
    /// the backup list, so we can still reach `peers->config` to
    /// detect a zone-driven peer-list reload.
    primary_peers: *mut ngx_http_upstream_rr_peers_t,
    primary_slots: *mut EwmaSlot,
    primary_len: ngx_uint_t,
    backup_slots: *mut EwmaSlot,
    backup_len: ngx_uint_t,
    /// Slab pool used for runtime slot-table reallocation during
    /// dynamic resync. Null in non-zone mode (where peer churn
    /// can't happen — `peers->config` is null and never increments).
    shpool: *mut ngx_slab_pool_t,
}

/// One eligible peer captured during `collect_available`. Carries
/// the peer pointer (for `select_peer`), its linked-list position
/// (for `round_robin`'s `tried[]` bitmap), and a direct slot pointer
/// so `peer.get` doesn't have to re-resolve the slot for the score
/// comparison.
#[derive(Clone, Copy)]
#[repr(C)]
struct AvailEntry {
    peer: *mut ngx_http_upstream_rr_peer_t,
    peer_index: ngx_uint_t,
    slot: *mut EwmaSlot,
}

#[derive(Clone, Copy)]
#[repr(C)]
struct EwmaPeerData {
    rrp: *mut ngx_http_upstream_rr_peer_data_t,
    config: *mut EwmaConfig,
    avail_buf: *mut AvailEntry,
    pick_valid: ngx_uint_t,
    /// Direct pointer to the chosen peer's slot. Replaces the old
    /// `(pick_is_backup, pick_index)` pair — robust against future
    /// peer-list shifts (Phase 3) since slots are sockaddr-keyed.
    pick_slot: *mut EwmaSlot,
    pick_start_msec: ngx_msec_t,
    /// Decayed EWMA of the chosen peer at pick time, surfaced via
    /// `$balancer_ewma_score`. Meaningful only when `pick_valid != 0`.
    pick_score: f64,
}

/// Hard-coded size of the per-upstream shm zone we register in
/// zone mode. Holds `EwmaConfig` + slot tables + slab allocator
/// metadata + headroom for resync churn. ~64 KB is enough for
/// fleets up to a few hundred peers; configurable would be cleaner
/// but is deferred.
const EWMA_ZONE_SIZE: usize = 64 * 1024;

unsafe extern "C" fn init_upstream(
    cf: *mut ngx_conf_t,
    us: *mut ngx_http_upstream_srv_conf_t,
) -> ngx_int_t {
    let cf_ptr = cf;
    let us_ptr = us;
    let Some(cf) = (unsafe { cf_ptr.as_mut() }) else {
        return Status::NGX_ERROR.into();
    };
    let Some(us) = (unsafe { us_ptr.as_mut() }) else {
        return Status::NGX_ERROR.into();
    };

    ngx_log_debug_mask!(DebugMask::Http, cf.log, "balancer_rs: init ewma");

    if unsafe { ngx_http_upstream_init_round_robin(cf_ptr, us_ptr) } != Status::NGX_OK.into() {
        return Status::NGX_ERROR.into();
    }

    us.peer.init = Some(init_peer);

    if us.shm_zone.is_null() {
        return init_upstream_static(cf, us);
    }

    // round_robin's zone init runs *after* us and replaces
    // `us->peer.data` with shpool peers. Defer EwmaConfig allocation
    // to our own shm zone's init callback, which nginx fires after
    // round_robin's.
    register_ewma_zone(cf, us)
}

fn init_upstream_static(cf: &mut ngx_conf_t, us: &mut ngx_http_upstream_srv_conf_t) -> ngx_int_t {
    let primary_ptr = us.peer.data.cast::<ngx_http_upstream_rr_peers_t>();
    let Some(primary) = (unsafe { primary_ptr.as_ref() }) else {
        return Status::NGX_ERROR.into();
    };
    let pool = unsafe { Pool::from_ngx_pool(cf.pool) };

    let primary_len = primary.number;
    let Some(primary_slots) = alloc_slots(&pool, primary_len) else {
        return Status::NGX_ERROR.into();
    };
    stamp_slot_identities(primary, primary_slots, primary_len);

    let backup_ptr = primary.next;
    let (backup_len, backup_slots) = if backup_ptr.is_null() {
        (0, ptr::null_mut())
    } else {
        let backup = unsafe { &*backup_ptr };
        let n = backup.number;
        let Some(p) = alloc_slots(&pool, n) else {
            return Status::NGX_ERROR.into();
        };
        stamp_slot_identities(backup, p, n);
        (n, p)
    };

    let cfg = pool.calloc_type::<EwmaConfig>();
    let Some(cfg_ref) = (unsafe { cfg.as_mut() }) else {
        return Status::NGX_ERROR.into();
    };
    cfg_ref.primary_peers = primary_ptr;
    cfg_ref.primary_slots = primary_slots;
    cfg_ref.primary_len = primary_len;
    cfg_ref.backup_slots = backup_slots;
    cfg_ref.backup_len = backup_len;
    cfg_ref.shpool = ptr::null_mut();

    let ccf = Balancer::server_conf_mut(us).expect("balancer_rs srv conf");
    match &mut ccf.policy {
        PolicyImpl::Ewma(state) => state.config = cfg,
        _ => return Status::NGX_ERROR.into(),
    }
    Status::NGX_OK.into()
}

/// Prefix for the per-upstream `ngx_shm_zone_t` name. Concatenated
/// with `(*us).host` so multiple `balancer_rs ewma` upstreams in
/// the same process get distinct zones.
const ZONE_NAME_PREFIX: &[u8] = b"balancer_rs_ewma_";

/// Register a per-upstream `ngx_shm_zone_t` and set its init
/// callback. The callback runs in master after `round_robin`'s zone
/// init has populated shpool peers; allocations done there land in
/// shared memory and survive into every forked worker.
fn register_ewma_zone(cf: &mut ngx_conf_t, us: &mut ngx_http_upstream_srv_conf_t) -> ngx_int_t {
    let pool = unsafe { Pool::from_ngx_pool(cf.pool) };

    let host_len = us.host.len;
    let host_data = us.host.data;
    let total = ZONE_NAME_PREFIX.len() + host_len;
    let buf = pool.alloc_unaligned(total).cast::<u8>();
    if buf.is_null() {
        return Status::NGX_ERROR.into();
    }
    unsafe {
        ptr::copy_nonoverlapping(ZONE_NAME_PREFIX.as_ptr(), buf, ZONE_NAME_PREFIX.len());
        if host_len > 0 {
            ptr::copy_nonoverlapping(host_data, buf.add(ZONE_NAME_PREFIX.len()), host_len);
        }
    }
    let name_ptr = pool.alloc_type::<ngx_str_t>();
    if name_ptr.is_null() {
        return Status::NGX_ERROR.into();
    }
    unsafe {
        (*name_ptr).len = total;
        (*name_ptr).data = buf;
    }

    let module_ptr: *const ngx_module_t = &raw const crate::ngx_http_balancer_rs_module;
    let cf_ptr = ptr::from_mut(cf);
    let shm_zone = unsafe {
        ngx_shared_memory_add(
            cf_ptr,
            name_ptr,
            EWMA_ZONE_SIZE,
            module_ptr.cast_mut().cast(),
        )
    };
    let Some(shm_zone) = (unsafe { shm_zone.as_mut() }) else {
        return Status::NGX_ERROR.into();
    };
    shm_zone.init = Some(ewma_zone_init);
    shm_zone.data = ptr::from_mut(us).cast::<c_void>();
    Status::NGX_OK.into()
}

/// Init callback for our `ngx_shm_zone_t`. Runs in master, after
/// the `round_robin` zone module's init has copied peers into the
/// upstream's own shpool — so `(*us).peer.data` now points at the
/// shpool peers list and we can walk it to size + stamp our slots.
unsafe extern "C" fn ewma_zone_init(zone: *mut ngx_shm_zone_t, _data: *mut c_void) -> ngx_int_t {
    let Some(zone) = (unsafe { zone.as_mut() }) else {
        return Status::NGX_ERROR.into();
    };
    let us_ptr = zone.data.cast::<ngx_http_upstream_srv_conf_t>();
    let Some(us) = (unsafe { us_ptr.as_mut() }) else {
        return Status::NGX_ERROR.into();
    };
    // `shm.addr` is `*mut u_char` (align 1), but every shared zone
    // begins with a page-aligned `ngx_slab_pool_t` header — alignment
    // is guaranteed by nginx, so silence the strict-alignment lint.
    #[allow(clippy::cast_ptr_alignment)]
    let shpool = zone.shm.addr.cast::<ngx_slab_pool_t>();
    if shpool.is_null() {
        return Status::NGX_ERROR.into();
    }

    let primary_ptr = us.peer.data.cast::<ngx_http_upstream_rr_peers_t>();
    let Some(primary) = (unsafe { primary_ptr.as_ref() }) else {
        return Status::NGX_ERROR.into();
    };

    // The slab pool is uncontended at zone-init time — master is the
    // only process touching it before workers fork — so the unlocked
    // `_locked` variants are safe (they skip taking shpool->mutex).
    let primary_len = primary.number;
    let primary_slots = unsafe { alloc_slots_shpool(shpool, primary_len) };
    if primary_slots.is_null() && primary_len > 0 {
        return Status::NGX_ERROR.into();
    }
    stamp_slot_identities(primary, primary_slots, primary_len);

    let backup_ptr = primary.next;
    let (backup_len, backup_slots) = if backup_ptr.is_null() {
        (0, ptr::null_mut())
    } else {
        let backup = unsafe { &*backup_ptr };
        let n = backup.number;
        let p = unsafe { alloc_slots_shpool(shpool, n) };
        if p.is_null() && n > 0 {
            return Status::NGX_ERROR.into();
        }
        stamp_slot_identities(backup, p, n);
        (n, p)
    };

    let cfg = unsafe {
        ngx_slab_calloc_locked(shpool, core::mem::size_of::<EwmaConfig>()).cast::<EwmaConfig>()
    };
    let Some(cfg_ref) = (unsafe { cfg.as_mut() }) else {
        return Status::NGX_ERROR.into();
    };
    cfg_ref.primary_peers = primary_ptr;
    cfg_ref.primary_slots = primary_slots;
    cfg_ref.primary_len = primary_len;
    cfg_ref.backup_slots = backup_slots;
    cfg_ref.backup_len = backup_len;
    cfg_ref.shpool = shpool;

    // Stamp the EwmaConfig pointer onto the BalancerConfig before
    // fork. All workers will inherit the same value via fork CoW;
    // since the EwmaConfig itself lives in shared memory, every
    // worker dereferences to the same physical pages.
    let ccf = Balancer::server_conf_mut(us).expect("balancer_rs srv conf");
    match &mut ccf.policy {
        PolicyImpl::Ewma(state) => state.config = cfg,
        _ => return Status::NGX_ERROR.into(),
    }
    Status::NGX_OK.into()
}

/// Slab-allocate a zero-initialized `EwmaSlot` array. Caller is
/// responsible for ensuring the slab pool is either uncontended
/// (zone init) or that `shpool->mutex` is held (runtime resync).
unsafe fn alloc_slots_shpool(shpool: *mut ngx_slab_pool_t, len: ngx_uint_t) -> *mut EwmaSlot {
    if len == 0 {
        return ptr::null_mut();
    }
    let bytes = len.saturating_mul(core::mem::size_of::<EwmaSlot>());
    unsafe { ngx_slab_calloc_locked(shpool, bytes) }.cast::<EwmaSlot>()
}

/// Allocate a zero-initialized `EwmaSlot` array from `pool`. Returns
/// `None` only when the allocation fails for a non-zero count; a
/// zero-length list yields a null pointer (callers must gate reads
/// on `len > 0`).
fn alloc_slots(pool: &Pool, len: ngx_uint_t) -> Option<*mut EwmaSlot> {
    if len == 0 {
        return Some(ptr::null_mut());
    }
    let bytes = len.checked_mul(core::mem::size_of::<EwmaSlot>())?;
    let p = pool.calloc(bytes).cast::<EwmaSlot>();
    if p.is_null() {
        None
    } else {
        Some(p)
    }
}

/// Walk `peers` and copy each peer's `(sockaddr, socklen)` into the
/// matching slot. Slots and peers are 1:1 at init time; once peer
/// churn (Phase 3) shifts linked-list positions, slots are looked
/// up by sockaddr instead, so this initial alignment doesn't have
/// to hold forever.
fn stamp_slot_identities(
    peers: &ngx_http_upstream_rr_peers_t,
    slots: *mut EwmaSlot,
    len: ngx_uint_t,
) {
    if slots.is_null() {
        return;
    }
    let mut peer_ptr = peers.peer;
    let mut i: ngx_uint_t = 0;
    while let Some(peer) = unsafe { peer_ptr.as_ref() } {
        if i >= len {
            break;
        }
        unsafe {
            (*slots.add(i)).sockaddr = peer.sockaddr;
            (*slots.add(i)).socklen = peer.socklen;
        }
        peer_ptr = peer.next;
        i += 1;
    }
}

/// Linear scan for the slot owning `(sockaddr, socklen)`. Returns
/// null if nothing matches. O(N) per call; fleets are typically
/// <100 peers so this is fine.
fn find_slot_by_sockaddr(
    slots: *mut EwmaSlot,
    len: ngx_uint_t,
    sockaddr: *mut sockaddr,
    socklen: socklen_t,
) -> *mut EwmaSlot {
    if slots.is_null() || sockaddr.is_null() || socklen == 0 {
        return ptr::null_mut();
    }
    let table = unsafe { core::slice::from_raw_parts_mut(slots, len) };
    let n = socklen as usize;
    let needle = unsafe { core::slice::from_raw_parts(sockaddr.cast::<u8>(), n) };
    for slot in table {
        if slot.sockaddr.is_null() || slot.socklen != socklen {
            continue;
        }
        let haystack = unsafe { core::slice::from_raw_parts(slot.sockaddr.cast::<u8>(), n) };
        if haystack == needle {
            return slot;
        }
    }
    ptr::null_mut()
}

http_upstream_init_peer_pt!(
    init_peer,
    |request: &mut Request, us: *mut ngx_http_upstream_srv_conf_t| {
        ngx_log_debug_http!(request, "balancer_rs: init ewma peer");

        if unsafe { ngx_http_upstream_init_round_robin_peer(request.into(), us) }
            != Status::NGX_OK.into()
        {
            return Status::NGX_ERROR;
        }

        let Some(upstream_ptr) = request.upstream() else {
            return Status::NGX_ERROR;
        };
        let upstream = unsafe { &mut *upstream_ptr };

        let rrp = upstream
            .peer
            .data
            .cast::<ngx_http_upstream_rr_peer_data_t>();
        if unsafe { rrp.as_ref() }.is_none() {
            return Status::NGX_ERROR;
        }

        let Some(us) = (unsafe { us.as_ref() }) else {
            return Status::NGX_ERROR;
        };
        let ccf = Balancer::server_conf(us).expect("balancer_rs srv conf");
        let cfg = match ccf.policy {
            PolicyImpl::Ewma(state) => state.config,
            _ => return Status::NGX_ERROR,
        };
        let Some(cfg_ref) = (unsafe { cfg.as_ref() }) else {
            return Status::NGX_ERROR;
        };

        let pool = request.pool();
        let our = pool.calloc_type::<EwmaPeerData>();
        let Some(our_ref) = (unsafe { our.as_mut() }) else {
            return Status::NGX_ERROR;
        };

        let cap = cfg_ref.primary_len.max(cfg_ref.backup_len);
        let avail_buf = if cap == 0 {
            ptr::null_mut()
        } else {
            let bytes = cap * core::mem::size_of::<AvailEntry>();
            // `pool.alloc` (uninit) is sufficient: `collect_available`
            // writes every slot up to `count` before any read.
            let p = pool.alloc(bytes).cast::<AvailEntry>();
            if p.is_null() {
                return Status::NGX_ERROR;
            }
            p
        };

        our_ref.rrp = rrp;
        our_ref.config = cfg;
        our_ref.avail_buf = avail_buf;

        upstream.peer.data = our.cast::<c_void>();
        upstream.peer.get = Some(get_peer);
        upstream.peer.free = Some(free_peer);

        Status::NGX_OK
    }
);

/// `peer.get` — pick an available peer using power-of-two-choices on
/// the EWMA scores. Falls back to backup peers with the same `tried[]`
/// reset dance as `least_conn`.
#[allow(clippy::similar_names)]
unsafe extern "C" fn get_peer(pc: *mut ngx_peer_connection_t, data: *mut c_void) -> ngx_int_t {
    let pc_ptr = pc;
    let our_ptr = data.cast::<EwmaPeerData>();
    let Some(pc) = (unsafe { pc_ptr.as_mut() }) else {
        return Status::NGX_ERROR.into();
    };
    let Some(our) = (unsafe { our_ptr.as_mut() }) else {
        return Status::NGX_ERROR.into();
    };
    let rrp_ptr = our.rrp;
    let cfg_ptr = our.config;
    let Some(rrp) = (unsafe { rrp_ptr.as_mut() }) else {
        return Status::NGX_ERROR.into();
    };
    let Some(cfg) = (unsafe { cfg_ptr.as_mut() }) else {
        return Status::NGX_ERROR.into();
    };

    ngx_log_debug_mask!(
        DebugMask::Http,
        pc.log,
        "balancer_rs: get ewma peer, try: {}",
        pc.tries
    );

    let primary_peers = rrp.peers;

    if peers_single(primary_peers) {
        // `pick_valid` stays 0, so peer.free skips the EWMA update.
        return unsafe { ngx_http_upstream_get_round_robin_peer(pc_ptr, rrp_ptr.cast()) };
    }

    clear_cached_connection(pc);

    let now_sec = unsafe { (*ngx_cached_time).sec };
    let now_msec = unsafe { ngx_current_msec };

    loop {
        let peers_ptr = rrp.peers;
        if peers_ptr.is_null() {
            return busy_with_primary_name(pc, primary_peers);
        }

        // Hold nginx's upstream peers write lock while selecting from
        // this primary-or-backup peers list. EWMA selection reads the
        // peer eligibility fields and tried bitmap, may resync the slot
        // table when `peers->config` changes, may swap `rrp->peers` to
        // the backup list, and commits the selected peer through the
        // shared round-robin counters. In zone mode these structures are
        // shared across workers, so resync + collection + selection must
        // see one consistent peers generation. `PeersWriteGuard` releases
        // the lock from `Drop`; early `return` and backup `continue`
        // paths below rely on that RAII drop to unlock immediately when
        // the current loop iteration exits.
        let mut guard = unsafe { PeersWriteGuard::lock(peers_ptr) };
        let peers = guard.peers();

        let is_backup = peers_ptr != primary_peers;

        if config_mismatch(rrp, peers) {
            // Peer-list shifted under us. Resync if we have a slab
            // pool (zone mode); otherwise bail busy and let nginx
            // surface the error to the client.
            if cfg.shpool.is_null() || resync_slots(cfg, peers, is_backup, now_msec).is_err() {
                return busy_with_primary_name(pc, primary_peers);
            }
            sync_config_generation(rrp, peers);
            ngx_log_debug_mask!(
                DebugMask::Http,
                pc.log,
                "balancer_rs: get ewma peer, resynced slot table"
            );
        }

        let (slots, slots_len) = slots_for(cfg, is_backup);

        let count = collect_available(rrp, peers, now_sec, our, slots, slots_len);

        if count == 0 {
            ngx_log_debug_mask!(
                DebugMask::Http,
                pc.log,
                "balancer_rs: get ewma peer, no peer found"
            );

            let next = peers.next;
            if next.is_null() {
                return busy_with_primary_name(pc, primary_peers);
            }

            ngx_log_debug_mask!(
                DebugMask::Http,
                pc.log,
                "balancer_rs: get ewma peer, backup servers"
            );

            switch_peers_and_clear_tried(rrp, next);
            continue;
        }

        let (chosen, chosen_score) = if count == 1 {
            let entry = first_avail_entry(our);
            let score = decay_score(entry.slot, now_msec);
            (entry, score)
        } else {
            p2c_pick(our, count, now_msec)
        };

        unsafe {
            select_peer(
                ptr::from_mut(pc),
                ptr::from_mut(rrp),
                chosen.peer,
                chosen.peer_index,
                now_sec,
            );
        };

        our.pick_valid = 1;
        our.pick_slot = chosen.slot;
        our.pick_start_msec = now_msec;
        our.pick_score = chosen_score;

        return Status::NGX_OK.into();
    }
}

fn slots_for(cfg: &EwmaConfig, is_backup: bool) -> (*mut EwmaSlot, ngx_uint_t) {
    if is_backup {
        (cfg.backup_slots, cfg.backup_len)
    } else {
        (cfg.primary_slots, cfg.primary_len)
    }
}

fn first_avail_entry(our: &EwmaPeerData) -> AvailEntry {
    unsafe { *our.avail_buf }
}

/// `peer.free` — fold this attempt's RTT into the picked peer's EWMA
/// (when the attempt succeeded and the peer-list generation hasn't
/// shifted), then delegate connection-bookkeeping to `round_robin`.
#[allow(clippy::similar_names)]
unsafe extern "C" fn free_peer(
    pc: *mut ngx_peer_connection_t,
    data: *mut c_void,
    state: ngx_uint_t,
) {
    let our_ptr = data.cast::<EwmaPeerData>();
    let Some(our) = (unsafe { our_ptr.as_mut() }) else {
        return;
    };

    if our.pick_valid != 0 {
        let failed = state & (NGX_PEER_FAILED as ngx_uint_t) != 0;
        let cfg = unsafe { our.config.as_ref() };
        let rrp = unsafe { our.rrp.as_ref() };
        let primary_ptr = cfg.map_or(ptr::null_mut(), |cfg| cfg.primary_peers);

        if !primary_ptr.is_null() {
            // Hold the primary peers write lock while folding the
            // request RTT into the chosen EWMA slot. The slot table is
            // owned by the peers generation and may be reallocated by a
            // zone resync in `peer.get`; taking the same peers lock lets
            // us check `peers->config` and update `pick_slot` only if it
            // still belongs to the current generation. In non-zone mode
            // this lock is a no-op, but keeping the guard here preserves
            // the same code path. `PeersWriteGuard` unlocks on `Drop`,
            // so the lock is released before delegating to round-robin's
            // `free` callback and on every early exit from this block.
            let mut guard = unsafe { PeersWriteGuard::lock(primary_ptr) };
            let primary = guard.peers();

            // Stale-check under lock: a resync between get and free
            // would have either bumped `peers->config` or freed our
            // `pick_slot`. Either way, skip the update.
            let stale = rrp.is_none_or(|rrp| config_mismatch(rrp, primary));

            if !failed && !stale {
                let now_msec = unsafe { ngx_current_msec };
                let rtt_msec = now_msec.saturating_sub(our.pick_start_msec);
                ewma_update(our.pick_slot, rtt_msec, now_msec);
            }
        }

        // Clear the pick so a `proxy_next_upstream` retry doesn't
        // double-update if peer.free gets called again before the
        // next peer.get.
        our.pick_valid = 0;
    }

    let rrp = our.rrp;
    if !rrp.is_null() {
        unsafe { ngx_http_upstream_free_round_robin_peer(pc, rrp.cast(), state) };
    }
}

/// Walk `peers`, push every available peer (with its slot, found by
/// sockaddr) into `our.avail_buf`, and return the count. Caller
/// holds the peers wlock.
fn collect_available(
    rrp: &ngx_http_upstream_rr_peer_data_t,
    peers: &ngx_http_upstream_rr_peers_t,
    now_sec: time_t,
    our: &mut EwmaPeerData,
    slots: *mut EwmaSlot,
    slots_len: ngx_uint_t,
) -> ngx_uint_t {
    let buf = our.avail_buf;
    let cap = unsafe { our.config.as_ref() }.map_or(0, avail_cap);
    if buf.is_null() || cap == 0 {
        return 0;
    }

    let mut count: ngx_uint_t = 0;
    let mut peer_ptr = peers.peer;
    let mut index: ngx_uint_t = 0;
    while let Some(peer) = unsafe { peer_ptr.as_ref() } {
        if count >= cap {
            break;
        }
        if peer_available_ref(rrp, peer, index, now_sec) {
            let slot = find_slot_by_sockaddr(slots, slots_len, peer.sockaddr, peer.socklen);
            unsafe {
                *buf.add(count) = AvailEntry {
                    peer: peer_ptr,
                    peer_index: index,
                    slot,
                };
            }
            count += 1;
        }
        peer_ptr = peer.next;
        index += 1;
    }
    count
}

fn avail_cap(cfg: &EwmaConfig) -> ngx_uint_t {
    cfg.primary_len.max(cfg.backup_len)
}

/// Sample two distinct positions from `[0, count)` and return the
/// `AvailEntry` whose decayed EWMA is lower, along with that score.
/// Ties go to the first sample (`i`).
#[allow(clippy::cast_sign_loss, clippy::cast_possible_truncation)]
fn p2c_pick(our: &EwmaPeerData, count: ngx_uint_t, now_msec: ngx_msec_t) -> (AvailEntry, f64) {
    // `ngx_random()` is glibc's `random()` — always non-negative and
    // bounded by `RAND_MAX < 2^31`, so the sign-loss / truncation
    // lints are spurious here.
    let buf = our.avail_buf;
    let i = (ngx_random() as ngx_uint_t) % count;
    let mut j = (ngx_random() as ngx_uint_t) % (count - 1);
    if j >= i {
        j += 1;
    }
    let a = unsafe { *buf.add(i) };
    let b = unsafe { *buf.add(j) };
    let score_a = decay_score(a.slot, now_msec);
    let score_b = decay_score(b.slot, now_msec);
    if score_a <= score_b {
        (a, score_a)
    } else {
        (b, score_b)
    }
}

/// Read `slot` and return its EWMA decayed forward to `now`. A null
/// pointer or a freshly-zeroed slot scores 0.
#[allow(clippy::cast_precision_loss)]
fn decay_score(slot: *mut EwmaSlot, now_msec: ngx_msec_t) -> f64 {
    if slot.is_null() {
        return 0.0;
    }
    let s = unsafe { *slot };
    let td = now_msec.saturating_sub(s.last_touched_msec);
    let weight = (-(td as f64) / DECAY_MSEC).exp();
    s.ewma * weight
}

/// Average decayed EWMA across slots that have at least one
/// recorded sample (`last_touched_msec != 0`). Used as the
/// slow-start seed for newly-appearing peers — keeps P2C from
/// always-picking the new peer because its zero score makes it
/// look "fastest." Returns 0.0 when no slot has been touched yet.
#[allow(clippy::cast_precision_loss)]
fn slow_start_mean(slots: *mut EwmaSlot, len: ngx_uint_t, now_msec: ngx_msec_t) -> f64 {
    if slots.is_null() || len == 0 {
        return 0.0;
    }
    let table = unsafe { core::slice::from_raw_parts(slots, len) };
    let mut sum = 0.0f64;
    let mut count = 0u64;
    for s in table {
        if s.last_touched_msec == 0 {
            continue;
        }
        let td = now_msec.saturating_sub(s.last_touched_msec);
        let weight = (-(td as f64) / DECAY_MSEC).exp();
        sum += s.ewma * weight;
        count += 1;
    }
    if count == 0 {
        0.0
    } else {
        sum / count as f64
    }
}

/// Rebuild the slot table for `peers` (primary or backup) after a
/// peers-list mutation. Surviving sockaddrs keep their EWMA + last
/// touched time; new ones get seeded with
/// `slow_start_mean(old_slots)` and `now`. Caller holds
/// `peers->wlock`. Uses the suffix-free `ngx_slab_*` calls — those
/// are the ones that take `shpool->mutex` internally, which is a
/// different lock from `peers->wlock` and the only thing that
/// keeps concurrent workers from corrupting the slab pool.
fn resync_slots(
    cfg: &mut EwmaConfig,
    peers: &mut ngx_http_upstream_rr_peers_t,
    is_backup: bool,
    now_msec: ngx_msec_t,
) -> Result<(), ()> {
    let shpool = cfg.shpool;
    if shpool.is_null() {
        return Err(());
    }
    let new_count = peers.number;
    let (old_slots, old_len) = if is_backup {
        (cfg.backup_slots, cfg.backup_len)
    } else {
        (cfg.primary_slots, cfg.primary_len)
    };

    let mean = slow_start_mean(old_slots, old_len, now_msec);

    let new_slots = if new_count == 0 {
        ptr::null_mut()
    } else {
        let bytes = new_count
            .checked_mul(core::mem::size_of::<EwmaSlot>())
            .ok_or(())?;
        let p = unsafe { ngx_slab_calloc(shpool, bytes) }.cast::<EwmaSlot>();
        if p.is_null() {
            return Err(());
        }
        p
    };

    let mut peer_ptr = peers.peer;
    let mut i: ngx_uint_t = 0;
    while let Some(peer) = unsafe { peer_ptr.as_ref() } {
        if i >= new_count {
            break;
        }
        let new_slot = unsafe { new_slots.add(i) };
        unsafe {
            (*new_slot).sockaddr = peer.sockaddr;
            (*new_slot).socklen = peer.socklen;
        }

        let old = find_slot_by_sockaddr(old_slots, old_len, peer.sockaddr, peer.socklen);
        if !old.is_null() {
            unsafe {
                (*new_slot).ewma = (*old).ewma;
                (*new_slot).last_touched_msec = (*old).last_touched_msec;
            }
        } else if mean > 0.0 {
            unsafe {
                (*new_slot).ewma = mean;
                (*new_slot).last_touched_msec = now_msec;
            }
        }
        peer_ptr = peer.next;
        i += 1;
    }

    if !old_slots.is_null() {
        unsafe { ngx_slab_free(shpool, old_slots.cast()) };
    }

    if is_backup {
        cfg.backup_slots = new_slots;
        cfg.backup_len = new_count;
    } else {
        cfg.primary_peers = ptr::from_mut(peers);
        cfg.primary_slots = new_slots;
        cfg.primary_len = new_count;
    }
    Ok(())
}

/// Apply the standard EWMA recurrence `ewma = ewma*w + rtt*(1-w)` in
/// place, with `w = exp(-td/DECAY)`. Bumps `last_touched_msec` to now.
#[allow(clippy::cast_precision_loss)]
fn ewma_update(slot: *mut EwmaSlot, rtt_msec: ngx_msec_t, now_msec: ngx_msec_t) {
    if slot.is_null() {
        return;
    }
    let prev = unsafe { *slot };
    let td = now_msec.saturating_sub(prev.last_touched_msec);
    let weight = (-(td as f64) / DECAY_MSEC).exp();
    let new_ewma = prev.ewma * weight + (rtt_msec as f64) * (1.0 - weight);
    unsafe {
        (*slot).ewma = new_ewma;
        (*slot).last_touched_msec = now_msec;
    }
}

/// Static variable table populated in [`register_variables`] from the
/// module's `preconfiguration` hook. Mirrors the pattern used by
/// `ngx-rust/examples/httporigdst.rs:103`: the array carries a static
/// name and our get-handler; nginx's `ngx_http_add_variable` returns
/// a fresh slot we then patch with that handler.
static mut NGX_BALANCER_RS_EWMA_VARS: [ngx_http_variable_t; 1] = [ngx_http_variable_t {
    name: ngx_string!("balancer_ewma_score"),
    set_handler: None,
    get_handler: Some(ngx_http_balancer_rs_ewma_score_variable),
    data: 0,
    flags: 0,
    index: 0,
}];

/// Called from `Balancer::preconfiguration` (in `lib.rs`). Registers
/// every `$balancer_*` variable owned by this module.
pub(crate) unsafe fn register_variables(cf: *mut ngx_conf_t) -> ngx_int_t {
    for mut v in unsafe { NGX_BALANCER_RS_EWMA_VARS } {
        let added = unsafe { ngx_http_add_variable(cf, &raw mut v.name, v.flags) };
        let Some(added) = (unsafe { added.as_mut() }) else {
            return Status::NGX_ERROR.into();
        };
        added.get_handler = v.get_handler;
        added.data = v.data;
    }
    Status::NGX_OK.into()
}

http_variable_get!(
    ngx_http_balancer_rs_ewma_score_variable,
    |request: &mut Request, v: *mut ngx_variable_value_t, _: usize| {
        let Some(v) = (unsafe { v.as_mut() }) else {
            return Status::NGX_ERROR;
        };

        let Some(our) = ewma_pick_data_for(request) else {
            v.set_not_found(1);
            return Status::NGX_OK;
        };
        if our.pick_valid == 0 {
            // Single-peer fast path or no successful pick: no score.
            v.set_not_found(1);
            return Status::NGX_OK;
        }

        // 6 decimal places is enough resolution for an RTT-based EWMA
        // and keeps log lines tidy. The pool buffer outlives the
        // variable read (request pool, freed at request end).
        let s = format!("{:.6}", our.pick_score);
        let bytes = s.as_bytes();
        let pool = request.pool();
        let buf = pool.alloc_unaligned(bytes.len());
        if buf.is_null() {
            return Status::NGX_ERROR;
        }
        // Bounded by f64 formatting (`{:.6}` produces ~30 chars at most),
        // so the usize→u32 narrowing can't truncate in practice.
        #[allow(clippy::cast_possible_truncation)]
        let len_u32 = bytes.len() as u32;
        unsafe {
            core::ptr::copy_nonoverlapping(bytes.as_ptr(), buf.cast::<u8>(), bytes.len());
        }
        v.data = buf.cast::<u8>();
        v.set_len(len_u32);
        v.set_valid(1);
        v.set_no_cacheable(0);
        v.set_not_found(0);
        Status::NGX_OK
    }
);

/// Walk `r → upstream → peer.data` and return our wrapper if and only
/// if this request's upstream is using the `ewma` policy. Identifies
/// the policy via the upstream srv conf's `BalancerConfig.policy` —
/// canonical, and avoids the unstable function-pointer comparison
/// path that `peer.get == Some(get_peer)` would otherwise hit.
fn ewma_pick_data_for(request: &Request) -> Option<&EwmaPeerData> {
    let upstream_ptr = request.upstream()?;
    let upstream = unsafe { upstream_ptr.as_ref() }?;
    let uscf = unsafe { upstream.upstream.as_ref() }?;
    let ccf = Balancer::server_conf(uscf)?;
    if !matches!(ccf.policy, PolicyImpl::Ewma(_)) {
        return None;
    }
    unsafe { upstream.peer.data.cast::<EwmaPeerData>().as_ref() }
}
