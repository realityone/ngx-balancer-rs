//! `ewma` policy.
//!
//! Peak-EWMA + Power-of-Two-Choices, in the spirit of ingress-nginx's
//! Lua implementation. Each peer carries an exponentially-weighted
//! moving average of its observed RTT (decayed continuously toward
//! zero with a 10-second time constant). On every request we sample
//! two random eligible peers and route to the one with the lower
//! score; the winner's EWMA is updated when the request completes.
//!
//! State lives in nginx-owned memory: static upstreams allocate their
//! `EwmaStatus` tables from `cf->pool`, while zone upstreams keep slot
//! tables plus sockaddr indexes in our private EWMA slab zone. Each
//! request still gets a wrapper around `round_robin`'s `rr_peer_data_t`
//! from `r->pool`. EWMA history does not survive a config reload —
//! out of scope for v1.

mod slot;

use core::{ffi::c_void, ptr, ptr::NonNull};

use ngx::{
    core::{Pool, SlabPool, Status},
    ffi::{
        ngx_cached_time, ngx_conf_t, ngx_current_msec, ngx_http_add_variable,
        ngx_http_upstream_free_round_robin_peer, ngx_http_upstream_get_round_robin_peer,
        ngx_http_upstream_init_pt, ngx_http_upstream_init_round_robin,
        ngx_http_upstream_init_round_robin_peer, ngx_http_upstream_rr_peer_data_t,
        ngx_http_upstream_rr_peer_t, ngx_http_upstream_rr_peers_t, ngx_http_upstream_srv_conf_t,
        ngx_http_variable_t, ngx_int_t, ngx_module_t, ngx_msec_t, ngx_peer_connection_t,
        ngx_random, ngx_shared_memory_add, ngx_shm_zone_t, ngx_slab_alloc_locked, ngx_slab_pool_t,
        ngx_str_t, ngx_uint_t, ngx_variable_value_t, time_t, NGX_PEER_FAILED,
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

use self::slot::{
    alloc_indexed_slots_shpool, alloc_indexed_slots_shpool_locked, alloc_slots, decay_score,
    ewma_update, find_slot_by_sockaddr, free_slab_slot_table, slow_start_mean,
    stamp_slot_identities, EwmaSlotTable, EwmaStatus,
};

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
#[repr(transparent)]
struct PeersList {
    ptr: NonNull<ngx_http_upstream_rr_peers_t>,
}

impl PeersList {
    fn new(ptr: *mut ngx_http_upstream_rr_peers_t) -> Option<Self> {
        NonNull::new(ptr).map(|ptr| Self { ptr })
    }

    fn from_mut(peers: &mut ngx_http_upstream_rr_peers_t) -> Self {
        Self {
            ptr: NonNull::from(peers),
        }
    }

    fn as_ptr(self) -> *mut ngx_http_upstream_rr_peers_t {
        self.ptr.as_ptr()
    }
}

#[derive(Clone, Copy)]
#[repr(C)]
struct EwmaConfig {
    /// Stable handle on the primary peers list. Needed in `peer.free`
    /// after a backup-fallback may have swapped `(*rrp).peers` to
    /// the backup list, so we can still reach `peers->config` to
    /// detect a zone-driven peer-list reload.
    primary_peers: PeersList,
    primary_slots: EwmaSlotTable,
    backup_slots: EwmaSlotTable,
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
    slot: *mut EwmaStatus,
}

#[derive(Clone, Copy)]
#[repr(C)]
struct EwmaPeerData {
    rrp: *mut ngx_http_upstream_rr_peer_data_t,
    config: *mut EwmaConfig,
    avail_buf: *mut AvailEntry,
    pick_valid: ngx_uint_t,
    score_valid: ngx_uint_t,
    /// Direct pointer to the chosen peer's slot. Replaces the old
    /// `(pick_is_backup, pick_index)` pair — robust against future
    /// peer-list shifts (Phase 3) since slots are sockaddr-keyed.
    pick_slot: *mut EwmaStatus,
    pick_start_msec: ngx_msec_t,
    /// Decayed EWMA of the chosen peer at pick time, surfaced via
    /// `$balancer_ewma_score`. Meaningful only when `score_valid != 0`.
    pick_score: f64,
}

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
    let Some(primary_peers) = PeersList::new(primary_ptr) else {
        return Status::NGX_ERROR.into();
    };
    let pool = unsafe { Pool::from_ngx_pool(cf.pool) };

    let primary_len = primary.number;
    let Some(mut primary_slots) = alloc_slots(&pool, primary_len) else {
        return Status::NGX_ERROR.into();
    };
    if stamp_slot_identities(primary, &mut primary_slots).is_err() {
        return Status::NGX_ERROR.into();
    }

    let backup_ptr = primary.next;
    let backup_slots = if backup_ptr.is_null() {
        EwmaSlotTable::empty()
    } else {
        let backup = unsafe { &*backup_ptr };
        let n = backup.number;
        let Some(mut table) = alloc_slots(&pool, n) else {
            return Status::NGX_ERROR.into();
        };
        if stamp_slot_identities(backup, &mut table).is_err() {
            return Status::NGX_ERROR.into();
        }
        table
    };

    let cfg = pool.alloc_type::<EwmaConfig>();
    if cfg.is_null() {
        return Status::NGX_ERROR.into();
    }
    unsafe {
        cfg.write(EwmaConfig {
            primary_peers,
            primary_slots,
            backup_slots,
            shpool: ptr::null_mut(),
        });
    }

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
    let Some(upstream_zone) = (unsafe { us.shm_zone.as_ref() }) else {
        return Status::NGX_ERROR.into();
    };
    let zone_size = upstream_zone.shm.size;
    if zone_size == 0 {
        return Status::NGX_ERROR.into();
    }

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
    let shm_zone =
        unsafe { ngx_shared_memory_add(cf_ptr, name_ptr, zone_size, module_ptr.cast_mut().cast()) };
    let Some(shm_zone) = (unsafe { shm_zone.as_mut() }) else {
        return Status::NGX_ERROR.into();
    };
    shm_zone.init = Some(ewma_zone_init);
    // `ewma_zone_init` receives only the zone pointer for the current
    // cycle, so stash the current upstream srv-conf here long enough
    // for init to find `us->peer.data`.
    shm_zone.data = ptr::from_mut(us).cast::<c_void>();
    // The EWMA config stores pointers into the current cycle's
    // upstream peer zone. Allocate a fresh private EWMA zone on reload
    // so new workers get tables stamped against the new peer list while
    // old workers keep using the old mapping until they exit.
    shm_zone.noreuse = 1;
    Status::NGX_OK.into()
}

/// Init callback for our `ngx_shm_zone_t`. Runs in master, after
/// the `round_robin` zone module's init has copied peers into the
/// upstream's own shpool — so `(*us).peer.data` now points at the
/// shpool peers list and we can walk it to size + stamp our slots.
unsafe extern "C" fn ewma_zone_init(
    zone: *mut ngx_shm_zone_t,
    _old_data: *mut c_void,
) -> ngx_int_t {
    let Some(zone) = (unsafe { zone.as_mut() }) else {
        return Status::NGX_ERROR.into();
    };
    let us_ptr = zone.data.cast::<ngx_http_upstream_srv_conf_t>();
    let Some(us) = (unsafe { us_ptr.as_mut() }) else {
        return Status::NGX_ERROR.into();
    };
    // The current upstream pointer was only a bootstrap value for this
    // init call. Clear it after use so future reload logic cannot grow
    // a second state handoff path through `shm_zone->data`.
    zone.data = ptr::null_mut();

    // `shm.addr` is `*mut u_char` (align 1), but every shared zone
    // begins with a page-aligned `ngx_slab_pool_t` header — alignment
    // is guaranteed by nginx, so silence the strict-alignment lint.
    #[allow(clippy::cast_ptr_alignment)]
    let shpool = zone.shm.addr.cast::<ngx_slab_pool_t>();
    if shpool.is_null() {
        return Status::NGX_ERROR.into();
    }
    let Some(slot_alloc) = (unsafe { SlabPool::from_shm_zone(zone) }) else {
        return Status::NGX_ERROR.into();
    };

    let primary_ptr = us.peer.data.cast::<ngx_http_upstream_rr_peers_t>();
    let Some(primary) = (unsafe { primary_ptr.as_ref() }) else {
        return Status::NGX_ERROR.into();
    };
    let Some(primary_peers) = PeersList::new(primary_ptr) else {
        return Status::NGX_ERROR.into();
    };

    // The slab pool is uncontended at zone-init time — master is the
    // only process touching it before workers fork — so the unlocked
    // `_locked` variants are safe (they skip taking shpool->mutex).
    let primary_len = primary.number;
    let Some(mut primary_slots) =
        (unsafe { alloc_indexed_slots_shpool_locked(shpool, &slot_alloc, primary_len) })
    else {
        return Status::NGX_ERROR.into();
    };
    if stamp_slot_identities(primary, &mut primary_slots).is_err() {
        return Status::NGX_ERROR.into();
    }

    let backup_ptr = primary.next;
    let backup_slots = if backup_ptr.is_null() {
        let Some(table) = (unsafe { alloc_indexed_slots_shpool_locked(shpool, &slot_alloc, 0) })
        else {
            return Status::NGX_ERROR.into();
        };
        table
    } else {
        let backup = unsafe { &*backup_ptr };
        let n = backup.number;
        let Some(mut table) =
            (unsafe { alloc_indexed_slots_shpool_locked(shpool, &slot_alloc, n) })
        else {
            return Status::NGX_ERROR.into();
        };
        if stamp_slot_identities(backup, &mut table).is_err() {
            return Status::NGX_ERROR.into();
        }
        table
    };

    let cfg = unsafe {
        ngx_slab_alloc_locked(shpool, core::mem::size_of::<EwmaConfig>()).cast::<EwmaConfig>()
    };
    if cfg.is_null() {
        return Status::NGX_ERROR.into();
    }
    unsafe {
        cfg.write(EwmaConfig {
            primary_peers,
            primary_slots,
            backup_slots,
            shpool,
        });
    }

    activate_ewma_config(us, cfg, primary_peers)
}

fn activate_ewma_config(
    us: &mut ngx_http_upstream_srv_conf_t,
    cfg: *mut EwmaConfig,
    primary_peers: PeersList,
) -> ngx_int_t {
    let Some(cfg_ref) = (unsafe { cfg.as_mut() }) else {
        return Status::NGX_ERROR.into();
    };
    cfg_ref.primary_peers = primary_peers;

    // Stamp the EwmaConfig pointer onto the BalancerConfig before
    // fork. All workers will inherit the same value via fork CoW;
    // since the EwmaConfig itself lives in shared memory, every
    // worker dereferences to the same physical pages.
    let ccf = Balancer::server_conf_mut(us).expect("balancer_rs srv conf");
    match &mut ccf.policy {
        PolicyImpl::Ewma(state) => {
            state.config = cfg;
            Status::NGX_OK.into()
        }
        _ => Status::NGX_ERROR.into(),
    }
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

        let cap = avail_cap(cfg_ref);
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

        let slots = slots_for(cfg, is_backup);

        let count = collect_available(rrp, peers, now_sec, our, slots);

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
        our.score_valid = 1;
        our.pick_slot = chosen.slot;
        our.pick_start_msec = now_msec;
        our.pick_score = chosen_score;

        return Status::NGX_OK.into();
    }
}

fn slots_for(cfg: &EwmaConfig, is_backup: bool) -> EwmaSlotTable {
    if is_backup {
        cfg.backup_slots
    } else {
        cfg.primary_slots
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
        let primary_ptr = cfg.map_or(ptr::null_mut(), |cfg| cfg.primary_peers.as_ptr());

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
        // next peer.get. `score_valid` intentionally survives so
        // `$balancer_ewma_score` can still be read by the access log
        // after upstream finalization.
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
    slots: EwmaSlotTable,
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
            let slot = find_slot_by_sockaddr(slots, peer.sockaddr, peer.socklen);
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
    cfg.primary_slots.len().max(cfg.backup_slots.len())
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
    let old_slots = slots_for(cfg, is_backup);
    let Some(slot_alloc) = old_slots.index_allocator() else {
        return Err(());
    };

    let mean = slow_start_mean(old_slots, now_msec);

    let Some(mut new_slots) =
        (unsafe { alloc_indexed_slots_shpool(shpool, &slot_alloc, new_count) })
    else {
        return Err(());
    };

    let mut peer_ptr = peers.peer;
    let mut i = 0usize;
    while let Some(new_slot) = new_slots.get_mut(i) {
        let Some(peer) = (unsafe { peer_ptr.as_ref() }) else {
            break;
        };
        new_slot.sockaddr = peer.sockaddr;
        new_slot.socklen = peer.socklen;

        let old = find_slot_by_sockaddr(old_slots, peer.sockaddr, peer.socklen);
        if !old.is_null() {
            unsafe {
                new_slot.ewma = (*old).ewma;
                new_slot.last_touched_msec = (*old).last_touched_msec;
            }
        } else if mean > 0.0 {
            new_slot.ewma = mean;
            new_slot.last_touched_msec = now_msec;
        }
        if new_slots
            .index_slot_at(i, peer.sockaddr, peer.socklen)
            .is_err()
        {
            free_slab_slot_table(shpool, new_slots);
            return Err(());
        }
        peer_ptr = peer.next;
        i += 1;
    }

    if is_backup {
        cfg.backup_slots = new_slots;
    } else {
        cfg.primary_peers = PeersList::from_mut(peers);
        cfg.primary_slots = new_slots;
    }
    free_slab_slot_table(shpool, old_slots);
    Ok(())
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
        if our.score_valid == 0 {
            // Single-peer fast path or no EWMA pick: no score.
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
