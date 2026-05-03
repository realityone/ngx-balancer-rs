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
    core::Status,
    ffi::{
        NGX_PEER_FAILED, ngx_cached_time, ngx_conf_t, ngx_current_msec,
        ngx_http_upstream_free_round_robin_peer, ngx_http_upstream_get_round_robin_peer,
        ngx_http_upstream_init_pt, ngx_http_upstream_init_round_robin,
        ngx_http_upstream_init_round_robin_peer, ngx_http_upstream_rr_peer_data_t,
        ngx_http_upstream_rr_peer_t, ngx_http_upstream_rr_peers_t, ngx_http_upstream_srv_conf_t,
        ngx_int_t, ngx_msec_t, ngx_pcalloc, ngx_peer_connection_t, ngx_random, ngx_uint_t, time_t,
    },
    http::{HttpModuleServerConf, Request},
    http_upstream_init_peer_pt, ngx_log_debug_http, ngx_log_debug_mask,
};

use crate::{
    Balancer, PolicyImpl,
    peer::{
        PTR_BITS, busy_with_primary_name, peer_available, peers_wlock, peers_wunlock, select_peer,
    },
    policy::BalancingPolicy,
};

/// EWMA decay time constant in milliseconds. A sample 10s after the
/// previous one contributes ~63% to the new score; one a millisecond
/// later moves the score by ~0.01%. Matches ingress-nginx's
/// `DECAY_TIME = 10` (seconds) — converted once here so the hot
/// `decay_score` path stays in `f64` ms.
const DECAY_MSEC: f64 = 10_000.0;

/// Policy state held by `BalancerConfig::policy` for an `ewma`
/// upstream. Populated lazily — `commands_set` constructs `Ewma::new()`
/// (with a null `config` pointer), then `init_upstream` allocates
/// the `EwmaConfig` slot table and stores the pointer here.
#[derive(Clone, Copy, Debug)]
#[repr(C)]
pub struct Ewma {
    pub(crate) config: *mut EwmaConfig,
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
pub(crate) struct EwmaSlot {
    ewma: f64,
    last_touched_msec: ngx_msec_t,
}

#[derive(Clone, Copy)]
#[repr(C)]
pub(crate) struct EwmaConfig {
    primary_peers: *mut ngx_http_upstream_rr_peers_t,
    primary_slots: *mut EwmaSlot,
    primary_len: ngx_uint_t,
    backup_peers: *mut ngx_http_upstream_rr_peers_t,
    backup_slots: *mut EwmaSlot,
    backup_len: ngx_uint_t,
}

#[derive(Clone, Copy)]
#[repr(C)]
struct EwmaPeerData {
    rrp: *mut ngx_http_upstream_rr_peer_data_t,
    config: *mut EwmaConfig,
    /// Snapshot of `(*rrp).config` at init time. Compared against the
    /// current peers-list generation in `peer.free` so a zone-driven
    /// peer-list mutation between get and free can't mis-attribute
    /// the RTT to a slot that now belongs to a different peer.
    config_gen: ngx_uint_t,
    /// Pool buffer for available-peer indices, sized to
    /// `max(primary_len, backup_len)`. Reused across retries.
    avail_buf: *mut ngx_uint_t,
    avail_cap: ngx_uint_t,
    /// `pick_valid != 0` means a peer has been selected this attempt
    /// and `peer.free` should fold its RTT into the EWMA slot
    /// identified by the next three fields.
    pick_valid: ngx_uint_t,
    pick_is_backup: ngx_uint_t,
    pick_index: ngx_uint_t,
    pick_start_msec: ngx_msec_t,
}

unsafe extern "C" fn init_upstream(
    cf: *mut ngx_conf_t,
    us: *mut ngx_http_upstream_srv_conf_t,
) -> ngx_int_t {
    ngx_log_debug_mask!(
        DebugMask::Http,
        unsafe { (*cf).log },
        "balancer_rs: init ewma"
    );

    if unsafe { ngx_http_upstream_init_round_robin(cf, us) } != Status::NGX_OK.into() {
        return Status::NGX_ERROR.into();
    }

    let primary = unsafe { (*us).peer.data }.cast::<ngx_http_upstream_rr_peers_t>();
    if primary.is_null() {
        return Status::NGX_ERROR.into();
    }

    let primary_len = unsafe { (*primary).number };
    let Some(primary_slots) = (unsafe { alloc_slots((*cf).pool, primary_len) }) else {
        return Status::NGX_ERROR.into();
    };

    let backup = unsafe { (*primary).next };
    let (backup_len, backup_slots) = if backup.is_null() {
        (0, ptr::null_mut())
    } else {
        let n = unsafe { (*backup).number };
        let Some(p) = (unsafe { alloc_slots((*cf).pool, n) }) else {
            return Status::NGX_ERROR.into();
        };
        (n, p)
    };

    let cfg =
        unsafe { ngx_pcalloc((*cf).pool, core::mem::size_of::<EwmaConfig>()).cast::<EwmaConfig>() };
    if cfg.is_null() {
        return Status::NGX_ERROR.into();
    }
    unsafe {
        (*cfg).primary_peers = primary;
        (*cfg).primary_slots = primary_slots;
        (*cfg).primary_len = primary_len;
        (*cfg).backup_peers = backup;
        (*cfg).backup_slots = backup_slots;
        (*cfg).backup_len = backup_len;
    }

    let ccf = Balancer::server_conf_mut(unsafe { &*us }).expect("balancer_rs srv conf");
    match &mut ccf.policy {
        PolicyImpl::Ewma(state) => state.config = cfg,
        // Unreachable in practice — `commands_set` already installed
        // `PolicyImpl::Ewma(...)` before nginx invoked us.
        _ => return Status::NGX_ERROR.into(),
    }

    unsafe { (*us).peer.init = Some(init_peer) };
    Status::NGX_OK.into()
}

/// `ngx_pcalloc` an array of `EwmaSlot`. Returns `None` only when the
/// allocation fails for a non-zero count; a zero-length list yields
/// a null pointer (callers must gate on `len > 0`).
unsafe fn alloc_slots(pool: *mut ngx::ffi::ngx_pool_t, len: ngx_uint_t) -> Option<*mut EwmaSlot> {
    if len == 0 {
        return Some(ptr::null_mut());
    }
    let bytes = len.checked_mul(core::mem::size_of::<EwmaSlot>())?;
    let p = unsafe { ngx_pcalloc(pool, bytes) }.cast::<EwmaSlot>();
    if p.is_null() { None } else { Some(p) }
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

        let rrp = unsafe { (*upstream_ptr).peer.data }.cast::<ngx_http_upstream_rr_peer_data_t>();
        if rrp.is_null() {
            return Status::NGX_ERROR;
        }

        let ccf = Balancer::server_conf(unsafe { &*us }).expect("balancer_rs srv conf");
        let cfg = match ccf.policy {
            PolicyImpl::Ewma(state) => state.config,
            _ => return Status::NGX_ERROR,
        };
        if cfg.is_null() {
            return Status::NGX_ERROR;
        }

        let pool = request.pool();
        let our = pool.calloc_type::<EwmaPeerData>();
        if our.is_null() {
            return Status::NGX_ERROR;
        }

        let cap = unsafe { (*cfg).primary_len.max((*cfg).backup_len) };
        let avail_buf = if cap == 0 {
            ptr::null_mut()
        } else {
            let bytes = cap * core::mem::size_of::<ngx_uint_t>();
            let p = pool.calloc(bytes).cast::<ngx_uint_t>();
            if p.is_null() {
                return Status::NGX_ERROR;
            }
            p
        };

        unsafe {
            (*our).rrp = rrp;
            (*our).config = cfg;
            (*our).config_gen = (*rrp).config;
            (*our).avail_buf = avail_buf;
            (*our).avail_cap = cap;
            // pick_* fields zero-initialized by calloc.

            (*upstream_ptr).peer.data = our.cast::<c_void>();
            (*upstream_ptr).peer.get = Some(get_peer);
            (*upstream_ptr).peer.free = Some(free_peer);
        }

        Status::NGX_OK
    }
);

/// `peer.get` — pick an available peer using power-of-two-choices on
/// the EWMA scores. Falls back to backup peers with the same `tried[]`
/// reset dance as `least_conn`.
#[allow(clippy::similar_names)]
unsafe extern "C" fn get_peer(pc: *mut ngx_peer_connection_t, data: *mut c_void) -> ngx_int_t {
    let our = data.cast::<EwmaPeerData>();
    if our.is_null() {
        return Status::NGX_ERROR.into();
    }
    let rrp = unsafe { (*our).rrp };
    let cfg = unsafe { (*our).config };
    if rrp.is_null() || cfg.is_null() {
        return Status::NGX_ERROR.into();
    }

    ngx_log_debug_mask!(
        DebugMask::Http,
        unsafe { (*pc).log },
        "balancer_rs: get ewma peer, try: {}",
        unsafe { (*pc).tries }
    );

    let primary_peers = unsafe { (*rrp).peers };

    if !primary_peers.is_null() && unsafe { (*primary_peers).single() } != 0 {
        // Single-peer fast path — round_robin handles it. `pick_valid`
        // stays 0, so peer.free skips the EWMA update for this attempt.
        return unsafe { ngx_http_upstream_get_round_robin_peer(pc, rrp.cast()) };
    }

    unsafe {
        (*pc).set_cached(0);
        (*pc).connection = ptr::null_mut();
    }

    let now_sec = unsafe { (*ngx_cached_time).sec };
    let now_msec = unsafe { ngx_current_msec };

    loop {
        let peers_ptr = unsafe { (*rrp).peers };
        if peers_ptr.is_null() {
            return busy_with_primary_name(pc, primary_peers);
        }

        unsafe { peers_wlock(peers_ptr) };

        if !unsafe { (*peers_ptr).config }.is_null()
            && unsafe { (*rrp).config != *(*peers_ptr).config }
        {
            unsafe { peers_wunlock(peers_ptr) };
            return busy_with_primary_name(pc, primary_peers);
        }

        let is_backup = peers_ptr != primary_peers;
        let (slots, slots_len) = unsafe {
            if is_backup {
                ((*cfg).backup_slots, (*cfg).backup_len)
            } else {
                ((*cfg).primary_slots, (*cfg).primary_len)
            }
        };

        let count = unsafe { collect_available(rrp, peers_ptr, now_sec, our) };

        if count == 0 {
            ngx_log_debug_mask!(
                DebugMask::Http,
                unsafe { (*pc).log },
                "balancer_rs: get ewma peer, no peer found"
            );

            let next = unsafe { (*peers_ptr).next };
            if next.is_null() {
                unsafe { peers_wunlock(peers_ptr) };
                return busy_with_primary_name(pc, primary_peers);
            }

            ngx_log_debug_mask!(
                DebugMask::Http,
                unsafe { (*pc).log },
                "balancer_rs: get ewma peer, backup servers"
            );

            unsafe { (*rrp).peers = next };
            let n = unsafe { (*next).number };
            let words = n.div_ceil(PTR_BITS);
            for i in 0..words {
                unsafe { *(*rrp).tried.add(i) = 0 };
            }
            unsafe { peers_wunlock(peers_ptr) };
            continue;
        }

        let chosen_idx = if count == 1 {
            unsafe { *(*our).avail_buf }
        } else {
            unsafe { p2c_pick(our, count, slots, slots_len, now_msec) }
        };

        let chosen_peer = unsafe { peer_at(peers_ptr, chosen_idx) };
        if chosen_peer.is_null() {
            unsafe { peers_wunlock(peers_ptr) };
            return busy_with_primary_name(pc, primary_peers);
        }

        unsafe { select_peer(pc, rrp, chosen_peer, chosen_idx, now_sec) };

        unsafe {
            (*our).pick_valid = 1;
            (*our).pick_is_backup = ngx_uint_t::from(is_backup);
            (*our).pick_index = chosen_idx;
            (*our).pick_start_msec = now_msec;
        }

        unsafe { peers_wunlock(peers_ptr) };
        return Status::NGX_OK.into();
    }
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
    let our = data.cast::<EwmaPeerData>();
    if our.is_null() {
        return;
    }

    if unsafe { (*our).pick_valid } != 0 {
        let failed = state & (NGX_PEER_FAILED as ngx_uint_t) != 0;
        let cfg = unsafe { (*our).config };
        // Skip on stale config-generation: a zone-driven peer-list
        // change between get and free could have shifted slot indices,
        // and we'd otherwise write the RTT into a slot that now
        // belongs to a different peer.
        let stale = if cfg.is_null() {
            true
        } else {
            let primary = unsafe { (*cfg).primary_peers };
            !primary.is_null()
                && !unsafe { (*primary).config }.is_null()
                && unsafe { (*our).config_gen != *(*primary).config }
        };

        if !failed && !stale {
            let now_msec = unsafe { ngx_current_msec };
            let rtt_msec = now_msec.saturating_sub(unsafe { (*our).pick_start_msec });
            let (slots, len) = unsafe {
                if (*our).pick_is_backup != 0 {
                    ((*cfg).backup_slots, (*cfg).backup_len)
                } else {
                    ((*cfg).primary_slots, (*cfg).primary_len)
                }
            };
            unsafe { ewma_update(slots, len, (*our).pick_index, rtt_msec, now_msec) };
        }

        // Clear the pick so a `proxy_next_upstream` retry doesn't
        // double-update if peer.free gets called again before the
        // next peer.get.
        unsafe { (*our).pick_valid = 0 };
    }

    let rrp = unsafe { (*our).rrp };
    if !rrp.is_null() {
        unsafe { ngx_http_upstream_free_round_robin_peer(pc, rrp.cast(), state) };
    }
}

/// Walk `peers`, push every available index into `our.avail_buf`, and
/// return the count. Caller holds the peers wlock.
unsafe fn collect_available(
    rrp: *mut ngx_http_upstream_rr_peer_data_t,
    peers: *mut ngx_http_upstream_rr_peers_t,
    now_sec: time_t,
    our: *mut EwmaPeerData,
) -> ngx_uint_t {
    let cap = unsafe { (*our).avail_cap };
    let buf = unsafe { (*our).avail_buf };
    if buf.is_null() || cap == 0 {
        return 0;
    }

    let mut count: ngx_uint_t = 0;
    let mut peer = unsafe { (*peers).peer };
    let mut index: ngx_uint_t = 0;
    while !peer.is_null() && count < cap {
        if unsafe { peer_available(rrp, peer, index, now_sec) } {
            unsafe { *buf.add(count as usize) = index };
            count += 1;
        }
        peer = unsafe { (*peer).next };
        index += 1;
    }
    count
}

/// Sample two distinct positions from `[0, count)`, look up their
/// peer indices in `our.avail_buf`, and return the index of whichever
/// has the lower decayed EWMA. Ties go to the first sample (`i`).
#[allow(clippy::cast_sign_loss, clippy::cast_possible_truncation)]
unsafe fn p2c_pick(
    our: *mut EwmaPeerData,
    count: ngx_uint_t,
    slots: *mut EwmaSlot,
    slots_len: ngx_uint_t,
    now_msec: ngx_msec_t,
) -> ngx_uint_t {
    // `ngx_random()` is glibc's `random()` — always non-negative and
    // bounded by `RAND_MAX < 2^31`, so the sign-loss / truncation
    // lints are spurious here.
    let buf = unsafe { (*our).avail_buf };
    let i = (ngx_random() as ngx_uint_t) % count;
    let mut j = (ngx_random() as ngx_uint_t) % (count - 1);
    if j >= i {
        j += 1;
    }
    let idx_a = unsafe { *buf.add(i) };
    let idx_b = unsafe { *buf.add(j) };
    let score_a = unsafe { decay_score(slots, slots_len, idx_a, now_msec) };
    let score_b = unsafe { decay_score(slots, slots_len, idx_b, now_msec) };
    if score_a <= score_b { idx_a } else { idx_b }
}

/// Walk the peers list and return the n-th node, or null if `target`
/// is past the end. Used to translate a slot-table index back into
/// the matching `rr_peer_t*` after P2C has picked one.
unsafe fn peer_at(
    peers: *mut ngx_http_upstream_rr_peers_t,
    target: ngx_uint_t,
) -> *mut ngx_http_upstream_rr_peer_t {
    let mut peer = unsafe { (*peers).peer };
    let mut i: ngx_uint_t = 0;
    while !peer.is_null() {
        if i == target {
            return peer;
        }
        peer = unsafe { (*peer).next };
        i += 1;
    }
    ptr::null_mut()
}

/// Read `slots[idx]` and return its EWMA decayed forward to `now`.
/// Out-of-range indices score 0 (never picked, looks "fastest" — but
/// this should be unreachable; index always comes from a valid slot).
#[allow(clippy::cast_precision_loss)]
unsafe fn decay_score(
    slots: *mut EwmaSlot,
    slots_len: ngx_uint_t,
    idx: ngx_uint_t,
    now_msec: ngx_msec_t,
) -> f64 {
    if slots.is_null() || idx >= slots_len {
        return 0.0;
    }
    let slot = unsafe { *slots.add(idx) };
    let td = now_msec.saturating_sub(slot.last_touched_msec);
    let weight = (-(td as f64) / DECAY_MSEC).exp();
    slot.ewma * weight
}

/// Apply the standard EWMA recurrence `ewma = ewma*w + rtt*(1-w)` in
/// place, with `w = exp(-td/DECAY)`. Bumps `last_touched_msec` to now.
#[allow(clippy::cast_precision_loss)]
unsafe fn ewma_update(
    slots: *mut EwmaSlot,
    slots_len: ngx_uint_t,
    idx: ngx_uint_t,
    rtt_msec: ngx_msec_t,
    now_msec: ngx_msec_t,
) {
    if slots.is_null() || idx >= slots_len {
        return;
    }
    let slot_ptr = unsafe { slots.add(idx) };
    let prev = unsafe { *slot_ptr };
    let td = now_msec.saturating_sub(prev.last_touched_msec);
    let weight = (-(td as f64) / DECAY_MSEC).exp();
    let new_ewma = prev.ewma * weight + (rtt_msec as f64) * (1.0 - weight);
    unsafe {
        (*slot_ptr).ewma = new_ewma;
        (*slot_ptr).last_touched_msec = now_msec;
    }
}
