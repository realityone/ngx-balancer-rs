//! `least_conn` policy.
//!
//! Mirrors stock nginx's `ngx_http_upstream_least_conn_module.c`:
//! reuse the `round_robin` upstream init / per-request init machinery,
//! then override only `peer.get` with our own selector.
//!
//! Selection rule: pick the available peer minimizing
//! `conns / effective_weight`. Comparisons are done as cross-products
//! (`a.conns * b.weight < b.conns * a.weight`) to avoid division.
//! Ties are broken by a weighted round-robin pass over the equal-ratio
//! peers, identical to the stock module.

use core::{cmp::Ordering, ffi::c_void, ptr};

use ngx::{
    core::Status,
    ffi::{
        ngx_cached_time, ngx_conf_t, ngx_http_upstream_get_round_robin_peer,
        ngx_http_upstream_init_pt, ngx_http_upstream_init_round_robin,
        ngx_http_upstream_init_round_robin_peer, ngx_http_upstream_rr_peer_data_t,
        ngx_http_upstream_rr_peer_t, ngx_http_upstream_rr_peers_t, ngx_http_upstream_srv_conf_t,
        ngx_int_t, ngx_peer_connection_t, ngx_uint_t, time_t,
    },
    http::Request,
    http_upstream_init_peer_pt, ngx_log_debug_http, ngx_log_debug_mask,
};

use crate::{
    peer::{
        busy_with_primary_name, clear_cached_connection, config_mismatch, peer_available_ref,
        peers_single, select_peer, switch_peers_and_clear_tried, PeersWriteGuard,
    },
    policy::BalancingPolicy,
};

#[derive(Clone, Copy, Debug, Default)]
#[repr(C)]
pub struct LeastConn;

impl BalancingPolicy for LeastConn {
    fn init_upstream() -> ngx_http_upstream_init_pt {
        Some(init_upstream)
    }
}

/// `peer.init_upstream` — populate the `round_robin` peer arrays, then
/// install our per-request initializer.
unsafe extern "C" fn init_upstream(
    cf: *mut ngx_conf_t,
    us: *mut ngx_http_upstream_srv_conf_t,
) -> ngx_int_t {
    let cf_ptr = cf;
    let us_ptr = us;
    let Some(cf) = (unsafe { cf_ptr.as_ref() }) else {
        return Status::NGX_ERROR.into();
    };
    if us_ptr.is_null() {
        return Status::NGX_ERROR.into();
    }

    ngx_log_debug_mask!(DebugMask::Http, cf.log, "balancer_rs: init least conn");

    if unsafe { ngx_http_upstream_init_round_robin(cf_ptr, us_ptr) } != Status::NGX_OK.into() {
        return Status::NGX_ERROR.into();
    }

    let us = unsafe { &mut *us_ptr };
    us.peer.init = Some(init_peer);
    Status::NGX_OK.into()
}

http_upstream_init_peer_pt!(
    init_peer,
    |request: &mut Request, us: *mut ngx_http_upstream_srv_conf_t| {
        ngx_log_debug_http!(request, "balancer_rs: init least conn peer");

        if unsafe { ngx_http_upstream_init_round_robin_peer(request.into(), us) }
            != Status::NGX_OK.into()
        {
            return Status::NGX_ERROR;
        }

        let Some(upstream_ptr) = request.upstream() else {
            return Status::NGX_ERROR;
        };

        // Only `peer.get` is overridden — round_robin's `peer.free`
        // and `peer.data` (the rrp pointer) keep doing their job.
        let upstream = unsafe { &mut *upstream_ptr };
        upstream.peer.get = Some(get_peer);

        Status::NGX_OK
    }
);

/// `peer.get` — pick the available peer with the fewest connections,
/// weighted. Walks the `rr_peers` list under the peers rwlock when the
/// upstream lives in a shared zone; falls back to the `next` (backup)
/// peers list when no primary is selectable.
unsafe extern "C" fn get_peer(pc: *mut ngx_peer_connection_t, data: *mut c_void) -> ngx_int_t {
    let pc_ptr = pc;
    let rrp_ptr = data.cast::<ngx_http_upstream_rr_peer_data_t>();
    let Some(pc) = (unsafe { pc_ptr.as_mut() }) else {
        return Status::NGX_ERROR.into();
    };
    let Some(rrp) = (unsafe { rrp_ptr.as_mut() }) else {
        return Status::NGX_ERROR.into();
    };

    ngx_log_debug_mask!(
        DebugMask::Http,
        pc.log,
        "balancer_rs: get least conn peer, try: {}",
        pc.tries
    );

    // Capture the primary peers list once. Stock nginx's recursion
    // structure means any terminal `NGX_BUSY` ends in the outer call's
    // `busy:` label, which sets `pc->name` from the *primary* peers
    // even when the backup pass also failed. Hold onto it so our
    // iterative loop can match that.
    let primary_peers = rrp.peers;

    // Single-peer fast path: round_robin handles it.
    if peers_single(primary_peers) {
        return unsafe { ngx_http_upstream_get_round_robin_peer(pc_ptr, data) };
    }

    // Clear keepalive scratch fields so a retry to a different peer
    // doesn't inherit a cached connection from the previous attempt.
    // Mirrors `ngx_http_upstream_get_least_conn_peer` in stock nginx.
    clear_cached_connection(pc);

    let now = unsafe { (*ngx_cached_time).sec };

    loop {
        let peers_ptr = rrp.peers;
        if peers_ptr.is_null() {
            return busy_with_primary_name(pc, primary_peers);
        }

        // Hold nginx's upstream peers write lock while we inspect and
        // mutate the round-robin peer list. Least-conn reads availability
        // fields (`down`, fails, `max_conns`, tried bitmap), updates
        // weighted-RR tie-break fields (`current_weight`,
        // `effective_weight`), and finally commits the selected peer by
        // bumping connection/ref counters. In zone mode those fields are
        // shared across workers and may also change during dynamic peer
        // reconfiguration, so the whole config-generation check plus
        // selection/backup switch must be one locked critical section.
        // `PeersWriteGuard` unlocks in `Drop`; all `return` and
        // `continue` paths below intentionally leave the block with the
        // guard in scope so nginx's rwlock is released automatically.
        let mut guard = unsafe { PeersWriteGuard::lock(peers_ptr) };
        let peers = guard.peers();

        // Detect upstream reload mid-request when the peers list lives
        // in a shared zone. `peers->config` tracks the current zone
        // generation; mismatch means our snapshot is stale.
        if config_mismatch(rrp, peers) {
            return busy_with_primary_name(pc, primary_peers);
        }

        let outcome = select_least_conn(pc, rrp, peers, now);

        match outcome {
            Selection::Selected => {
                return Status::NGX_OK.into();
            }
            Selection::TryBackup => {
                ngx_log_debug_mask!(
                    DebugMask::Http,
                    pc.log,
                    "balancer_rs: get least conn peer, no peer found"
                );

                let next = peers.next;
                if next.is_null() {
                    return busy_with_primary_name(pc, primary_peers);
                }

                ngx_log_debug_mask!(
                    DebugMask::Http,
                    pc.log,
                    "balancer_rs: get least conn peer, backup servers"
                );

                // Switch to backup peers and zero the tried bitmap for
                // the new peer count, so backup peers whose index
                // collides with a tried-primary index aren't skipped.
                switch_peers_and_clear_tried(rrp, next);
            }
        }
    }
}

enum Selection {
    Selected,
    TryBackup,
}

/// Run the two-pass `least_conn` selection on a single `rr_peers` list.
/// Caller holds the peers wlock.
fn select_least_conn(
    pc: &mut ngx_peer_connection_t,
    rrp: &mut ngx_http_upstream_rr_peer_data_t,
    peers: &mut ngx_http_upstream_rr_peers_t,
    now: time_t,
) -> Selection {
    let mut best: *mut ngx_http_upstream_rr_peer_t = ptr::null_mut();
    let mut best_index: ngx_uint_t = 0;
    let mut best_load = PeerLoad::default();
    let mut many = false;

    let mut peer_ptr = peers.peer;
    let mut index: ngx_uint_t = 0;
    while let Some(peer) = unsafe { peer_ptr.as_mut() } {
        if peer_available_ref(rrp, peer, index, now) {
            if best.is_null() {
                best = peer_ptr;
                best_index = index;
                best_load = PeerLoad::from_peer(peer);
                many = false;
            } else {
                let load = PeerLoad::from_peer(peer);
                match ratio_cmp(load, best_load) {
                    Ordering::Less => {
                        best = peer_ptr;
                        best_index = index;
                        best_load = load;
                        many = false;
                    }
                    Ordering::Equal => many = true,
                    Ordering::Greater => {}
                }
            }
        }

        peer_ptr = peer.next;
        index += 1;
    }

    if best.is_null() {
        return Selection::TryBackup;
    }

    // Tie-break among equal-ratio peers using weighted round-robin.
    if many {
        ngx_log_debug_mask!(
            DebugMask::Http,
            pc.log,
            "balancer_rs: get least conn peer, many"
        );
        let mut total: ngx_int_t = 0;
        peer_ptr = best;
        index = best_index;
        let mut best_current_weight: ngx_int_t = ngx_int_t::MIN;
        while let Some(peer) = unsafe { peer_ptr.as_mut() } {
            if peer_available_ref(rrp, peer, index, now)
                && ratio_cmp(PeerLoad::from_peer(peer), best_load).is_eq()
            {
                peer.current_weight += peer.effective_weight;
                total += peer.effective_weight;
                if peer.effective_weight < peer.weight {
                    peer.effective_weight += 1;
                }
                if peer_ptr == best {
                    best_current_weight = peer.current_weight;
                } else if peer.current_weight > best_current_weight {
                    best = peer_ptr;
                    best_index = index;
                    best_current_weight = peer.current_weight;
                }
            }
            peer_ptr = peer.next;
            index += 1;
        }
        let best_peer = unsafe { &mut *best };
        best_peer.current_weight -= total;
    }

    unsafe { select_peer(ptr::from_mut(pc), ptr::from_mut(rrp), best, best_index, now) };
    Selection::Selected
}

#[derive(Clone, Copy, Default)]
struct PeerLoad {
    conns: ngx_uint_t,
    weight: ngx_int_t,
}

impl PeerLoad {
    fn from_peer(peer: &ngx_http_upstream_rr_peer_t) -> Self {
        Self {
            conns: peer.conns,
            weight: peer.weight,
        }
    }
}

/// Order `peer.conns / peer.weight` against `other.conns / other.weight`,
/// computed via cross-product to avoid division.
fn ratio_cmp(peer: PeerLoad, other: PeerLoad) -> Ordering {
    let lhs = i128::from(peer.conns as u64) * i128::from(other.weight as i64);
    let rhs = i128::from(other.conns as u64) * i128::from(peer.weight as i64);
    lhs.cmp(&rhs)
}
