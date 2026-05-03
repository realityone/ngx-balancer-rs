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

use core::cmp::Ordering;
use core::ffi::c_void;
use core::ptr;

use ngx::core::Status;
use ngx::ffi::{
    ngx_cached_time, ngx_conf_t, ngx_http_upstream_get_round_robin_peer,
    ngx_http_upstream_init_pt, ngx_http_upstream_init_round_robin,
    ngx_http_upstream_init_round_robin_peer, ngx_http_upstream_rr_peer_data_t,
    ngx_http_upstream_rr_peer_t, ngx_http_upstream_rr_peers_t, ngx_http_upstream_srv_conf_t,
    ngx_int_t, ngx_peer_connection_t, ngx_rwlock_unlock, ngx_rwlock_wlock, ngx_uint_t, time_t,
};
use ngx::http::Request;
use ngx::{http_upstream_init_peer_pt, ngx_log_debug_http, ngx_log_debug_mask};

use crate::{Policy, policy::BalancingPolicy};

const PTR_BITS: ngx_uint_t = ngx_uint_t::BITS as ngx_uint_t;

pub struct LeastConn;

impl BalancingPolicy for LeastConn {
    const KIND: Policy = Policy::LeastConn;

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
    ngx_log_debug_mask!(DebugMask::Http, unsafe { (*cf).log }, "init least conn");

    if unsafe { ngx_http_upstream_init_round_robin(cf, us) } != Status::NGX_OK.into() {
        return Status::NGX_ERROR.into();
    }

    unsafe { (*us).peer.init = Some(init_peer) };
    Status::NGX_OK.into()
}

http_upstream_init_peer_pt!(
    init_peer,
    |request: &mut Request, us: *mut ngx_http_upstream_srv_conf_t| {
        ngx_log_debug_http!(request, "init least conn peer");

        if unsafe { ngx_http_upstream_init_round_robin_peer(request.into(), us) }
            != Status::NGX_OK.into()
        {
            return Status::NGX_ERROR;
        }

        let Some(upstream_ptr) = request.upstream() else {
            return Status::NGX_ERROR;
        };

        // `round_robin`'s `init_peer` set `peer.data = rrp` and
        // `peer.get` / `peer.free` to its own callbacks. We only
        // override `peer.get` so the existing free path keeps working.
        unsafe { (*upstream_ptr).peer.get = Some(get_peer) };

        Status::NGX_OK
    }
);

/// `peer.get` — pick the available peer with the fewest connections,
/// weighted. Walks the `rr_peers` list under the peers rwlock when the
/// upstream lives in a shared zone; falls back to the `next` (backup)
/// peers list when no primary is selectable.
unsafe extern "C" fn get_peer(pc: *mut ngx_peer_connection_t, data: *mut c_void) -> ngx_int_t {
    let rrp = data.cast::<ngx_http_upstream_rr_peer_data_t>();
    if rrp.is_null() {
        return Status::NGX_ERROR.into();
    }

    ngx_log_debug_mask!(
        DebugMask::Http,
        unsafe { (*pc).log },
        "get least conn peer, try: {}",
        unsafe { (*pc).tries }
    );

    // Capture the primary peers list once. Stock nginx's recursion
    // structure means any terminal `NGX_BUSY` ends in the outer call's
    // `busy:` label, which sets `pc->name` from the *primary* peers
    // even when the backup pass also failed. Hold onto it so our
    // iterative loop can match that.
    let primary_peers = unsafe { (*rrp).peers };

    // Single-peer fast path: round_robin handles it.
    if !primary_peers.is_null() && unsafe { (*primary_peers).single() } != 0 {
        return unsafe { ngx_http_upstream_get_round_robin_peer(pc, data) };
    }

    // Clear keepalive scratch fields so a retry to a different peer
    // doesn't inherit a cached connection from the previous attempt.
    // Mirrors `ngx_http_upstream_get_least_conn_peer` in stock nginx.
    unsafe {
        (*pc).set_cached(0);
        (*pc).connection = ptr::null_mut();
    };

    let now = unsafe { (*ngx_cached_time).sec };

    // Walk primary then backup peers. Tail-recursive in the C version;
    // a loop reads more cleanly here.
    loop {
        let peers_ptr = unsafe { (*rrp).peers };
        if peers_ptr.is_null() {
            return busy_with_primary_name(pc, primary_peers);
        }

        unsafe { peers_wlock(peers_ptr) };

        // Detect upstream reload mid-request when the peers list lives
        // in a shared zone. `peers->config` tracks the current zone
        // generation; mismatch means our snapshot is stale.
        if !unsafe { (*peers_ptr).config }.is_null()
            && unsafe { (*rrp).config != *(*peers_ptr).config }
        {
            unsafe { peers_wunlock(peers_ptr) };
            return busy_with_primary_name(pc, primary_peers);
        }

        let outcome = unsafe { select_least_conn(pc, rrp, peers_ptr, now) };

        match outcome {
            Selection::Selected => {
                unsafe { peers_wunlock(peers_ptr) };
                return Status::NGX_OK.into();
            }
            Selection::TryBackup => {
                ngx_log_debug_mask!(
                    DebugMask::Http,
                    unsafe { (*pc).log },
                    "get least conn peer, no peer found"
                );

                let next = unsafe { (*peers_ptr).next };
                if next.is_null() {
                    unsafe { peers_wunlock(peers_ptr) };
                    return busy_with_primary_name(pc, primary_peers);
                }

                ngx_log_debug_mask!(
                    DebugMask::Http,
                    unsafe { (*pc).log },
                    "get least conn peer, backup servers"
                );

                // Switch to backup peers and zero the tried bitmap for
                // the new peer count, so backup peers whose index
                // collides with a tried-primary index aren't skipped.
                unsafe { (*rrp).peers = next };
                let count = unsafe { (*next).number };
                let words = count.div_ceil(PTR_BITS);
                for i in 0..words {
                    unsafe { *(*rrp).tried.add(i) = 0 };
                }

                unsafe { peers_wunlock(peers_ptr) };
            }
        }
    }
}

/// Set `pc.name` from the primary peers list (or leave it alone if the
/// pointer is null) and return `NGX_BUSY`. Centralizes the parity with
/// the stock module's `busy:` label, where the outer-call's primary
/// peers always win the final name assignment.
fn busy_with_primary_name(
    pc: *mut ngx_peer_connection_t,
    primary_peers: *mut ngx_http_upstream_rr_peers_t,
) -> ngx_int_t {
    if !primary_peers.is_null() {
        unsafe { (*pc).name = (*primary_peers).name };
    }
    Status::NGX_BUSY.into()
}

enum Selection {
    Selected,
    TryBackup,
}

/// Run the two-pass `least_conn` selection on a single `rr_peers` list.
/// Caller holds the peers wlock.
unsafe fn select_least_conn(
    pc: *mut ngx_peer_connection_t,
    rrp: *mut ngx_http_upstream_rr_peer_data_t,
    peers: *mut ngx_http_upstream_rr_peers_t,
    now: time_t,
) -> Selection {
    let mut best: *mut ngx_http_upstream_rr_peer_t = ptr::null_mut();
    let mut best_index: ngx_uint_t = 0;
    let mut many = false;

    let mut peer = unsafe { (*peers).peer };
    let mut index: ngx_uint_t = 0;
    while !peer.is_null() {
        if unsafe { peer_available(rrp, peer, index, now) } {
            if best.is_null() {
                best = peer;
                best_index = index;
                many = false;
            } else {
                match unsafe { ratio_cmp(peer, best) } {
                    Ordering::Less => {
                        best = peer;
                        best_index = index;
                        many = false;
                    }
                    Ordering::Equal => many = true,
                    Ordering::Greater => {}
                }
            }
        }

        peer = unsafe { (*peer).next };
        index += 1;
    }

    if best.is_null() {
        return Selection::TryBackup;
    }

    // Tie-break among equal-ratio peers using weighted round-robin.
    if many {
        ngx_log_debug_mask!(
            DebugMask::Http,
            unsafe { (*pc).log },
            "get least conn peer, many"
        );
        let mut total: ngx_int_t = 0;
        peer = best;
        index = best_index;
        while !peer.is_null() {
            if unsafe { peer_available(rrp, peer, index, now) }
                && unsafe { ratio_cmp(peer, best) }.is_eq()
            {
                unsafe {
                    (*peer).current_weight += (*peer).effective_weight;
                    total += (*peer).effective_weight;
                    if (*peer).effective_weight < (*peer).weight {
                        (*peer).effective_weight += 1;
                    }
                    if (*peer).current_weight > (*best).current_weight {
                        best = peer;
                        best_index = index;
                    }
                }
            }
            peer = unsafe { (*peer).next };
            index += 1;
        }
        unsafe { (*best).current_weight -= total };
    }

    unsafe { select_peer(pc, rrp, best, best_index, now) };
    Selection::Selected
}

/// Order `peer.conns / peer.weight` against `other.conns / other.weight`,
/// computed via cross-product to avoid division. Both pointers must be
/// non-null.
unsafe fn ratio_cmp(
    peer: *const ngx_http_upstream_rr_peer_t,
    other: *const ngx_http_upstream_rr_peer_t,
) -> Ordering {
    let lhs = i128::from(unsafe { (*peer).conns } as u64)
        * i128::from(unsafe { (*other).weight } as i64);
    let rhs = i128::from(unsafe { (*other).conns } as u64)
        * i128::from(unsafe { (*peer).weight } as i64);
    lhs.cmp(&rhs)
}

/// Eligibility check matching the stock module: skip already-tried,
/// administratively-down, fail-quarantined, or `max_conns`-saturated peers.
unsafe fn peer_available(
    rrp: *mut ngx_http_upstream_rr_peer_data_t,
    peer: *mut ngx_http_upstream_rr_peer_t,
    index: ngx_uint_t,
    now: time_t,
) -> bool {
    let n = index / PTR_BITS;
    let m = 1 << (index % PTR_BITS);
    if unsafe { *(*rrp).tried.add(n) } & m != 0 {
        return false;
    }
    if unsafe { (*peer).down } != 0 {
        return false;
    }
    if unsafe {
        (*peer).max_fails != 0
            && (*peer).fails >= (*peer).max_fails
            && now - (*peer).checked <= (*peer).fail_timeout
    } {
        return false;
    }
    if unsafe { (*peer).max_conns != 0 && (*peer).conns >= (*peer).max_conns } {
        return false;
    }
    true
}

/// Commit the selected peer: stamp `pc`, bump conns, mark tried.
unsafe fn select_peer(
    pc: *mut ngx_peer_connection_t,
    rrp: *mut ngx_http_upstream_rr_peer_data_t,
    peer: *mut ngx_http_upstream_rr_peer_t,
    index: ngx_uint_t,
    now: time_t,
) {
    unsafe {
        (*pc).sockaddr = (*peer).sockaddr;
        (*pc).socklen = (*peer).socklen;
        (*pc).name = &raw mut (*peer).name;

        if now - (*peer).checked > (*peer).fail_timeout {
            (*peer).checked = now;
        }

        (*peer).conns += 1;
        (*rrp).current = peer;
        // Mirrors stock nginx's `ngx_http_upstream_rr_peer_ref` macro:
        // a no-op outside zone builds, but our vendored nginx is built
        // with NGX_HTTP_UPSTREAM_ZONE so the bump keeps the peer alive
        // across reconfigures while the request still references it.
        (*peer).refs += 1;

        let n = index / PTR_BITS;
        let m = 1 << (index % PTR_BITS);
        *(*rrp).tried.add(n) |= m;
    }
}

unsafe fn peers_wlock(peers: *mut ngx_http_upstream_rr_peers_t) {
    if !unsafe { (*peers).shpool.is_null() } {
        unsafe { ngx_rwlock_wlock(&raw mut (*peers).rwlock) };
    }
}

unsafe fn peers_wunlock(peers: *mut ngx_http_upstream_rr_peers_t) {
    if !unsafe { (*peers).shpool.is_null() } {
        unsafe { ngx_rwlock_unlock(&raw mut (*peers).rwlock) };
    }
}
