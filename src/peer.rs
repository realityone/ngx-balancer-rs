//! Helpers shared by every policy that builds on `round_robin`.
//!
//! These are direct ports of fragments from stock nginx's
//! `ngx_http_upstream_round_robin.c` / `ngx_http_upstream_least_conn_module.c`
//! that have nothing to do with a specific selection rule:
//! eligibility filtering, the bookkeeping done after a peer is chosen,
//! and the rwlock that gates a peers list living in a shared zone.

use core::ptr;

use ngx::{
    core::Status,
    ffi::{
        ngx_http_upstream_rr_peer_data_t, ngx_http_upstream_rr_peer_t,
        ngx_http_upstream_rr_peers_t, ngx_int_t, ngx_peer_connection_t, ngx_rwlock_unlock,
        ngx_rwlock_wlock, ngx_uint_t, time_t,
    },
};

pub(crate) const PTR_BITS: ngx_uint_t = ngx_uint_t::BITS as ngx_uint_t;

/// Eligibility check matching the stock module: skip already-tried,
/// administratively-down, fail-quarantined, or `max_conns`-saturated peers.
pub(crate) fn peer_available_ref(
    rrp: &ngx_http_upstream_rr_peer_data_t,
    peer: &ngx_http_upstream_rr_peer_t,
    index: ngx_uint_t,
    now: time_t,
) -> bool {
    let n = index / PTR_BITS;
    let m = 1 << (index % PTR_BITS);
    if unsafe { *rrp.tried.add(n) } & m != 0 {
        return false;
    }
    if peer.down != 0 {
        return false;
    }
    if peer.max_fails != 0
        && peer.fails >= peer.max_fails
        && now - peer.checked <= peer.fail_timeout
    {
        return false;
    }
    if peer.max_conns != 0 && peer.conns >= peer.max_conns {
        return false;
    }
    true
}

/// Commit the selected peer: stamp `pc`, bump conns, mark tried.
pub(crate) unsafe fn select_peer(
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

pub(crate) unsafe fn peers_wlock(peers: *mut ngx_http_upstream_rr_peers_t) {
    if !unsafe { (*peers).shpool.is_null() } {
        unsafe { ngx_rwlock_wlock(&raw mut (*peers).rwlock) };
    }
}

pub(crate) unsafe fn peers_wunlock(peers: *mut ngx_http_upstream_rr_peers_t) {
    if !unsafe { (*peers).shpool.is_null() } {
        unsafe { ngx_rwlock_unlock(&raw mut (*peers).rwlock) };
    }
}

/// RAII wrapper for nginx's upstream peers write lock.
///
/// NGINX only needs the lock when an upstream lives in a shared zone;
/// `peers_wlock`/`peers_wunlock` internally no-op for static upstreams.
/// Keep this guard scoped to the exact region that reads or mutates
/// `ngx_http_upstream_rr_peers_t`, peer weights/conns, tried/config
/// state, or EWMA slot state tied to that peers list. The lock is
/// released by `Drop`, so every early `return`, `continue`, or normal
/// fall-through unlocks as soon as the guard leaves scope.
pub(crate) struct PeersWriteGuard {
    peers: *mut ngx_http_upstream_rr_peers_t,
}

impl PeersWriteGuard {
    pub(crate) unsafe fn lock(peers: *mut ngx_http_upstream_rr_peers_t) -> Self {
        unsafe { peers_wlock(peers) };
        Self { peers }
    }

    pub(crate) fn peers(&mut self) -> &mut ngx_http_upstream_rr_peers_t {
        unsafe { &mut *self.peers }
    }
}

impl Drop for PeersWriteGuard {
    fn drop(&mut self) {
        unsafe { peers_wunlock(self.peers) };
    }
}

pub(crate) fn config_mismatch(
    rrp: &ngx_http_upstream_rr_peer_data_t,
    peers: &ngx_http_upstream_rr_peers_t,
) -> bool {
    let Some(config) = (unsafe { peers.config.as_ref() }) else {
        return false;
    };
    rrp.config != *config
}

pub(crate) fn sync_config_generation(
    rrp: &mut ngx_http_upstream_rr_peer_data_t,
    peers: &ngx_http_upstream_rr_peers_t,
) {
    if let Some(config) = unsafe { peers.config.as_ref() } {
        rrp.config = *config;
    }
}

pub(crate) fn clear_tried(rrp: &mut ngx_http_upstream_rr_peer_data_t, count: ngx_uint_t) {
    let words = count.div_ceil(PTR_BITS);
    for i in 0..words {
        unsafe { *rrp.tried.add(i) = 0 };
    }
}

pub(crate) fn peers_single(peers: *mut ngx_http_upstream_rr_peers_t) -> bool {
    unsafe { peers.as_ref() }.is_some_and(|peers| peers.single() != 0)
}

pub(crate) fn clear_cached_connection(pc: &mut ngx_peer_connection_t) {
    pc.set_cached(0);
    pc.connection = ptr::null_mut();
}

pub(crate) fn switch_peers_and_clear_tried(
    rrp: &mut ngx_http_upstream_rr_peer_data_t,
    peers: *mut ngx_http_upstream_rr_peers_t,
) {
    rrp.peers = peers;
    if let Some(peers) = unsafe { peers.as_ref() } {
        clear_tried(rrp, peers.number);
    }
}

/// Set `pc.name` from the captured primary peers list (or leave it
/// alone if the pointer is null) and return `NGX_BUSY`. Centralizes
/// the parity with stock nginx's `busy:` label, where the outer-call's
/// primary peers always win the final name assignment regardless of
/// whether the iteration ended on the primary or a backup list.
pub(crate) fn busy_with_primary_name(
    pc: &mut ngx_peer_connection_t,
    primary_peers: *mut ngx_http_upstream_rr_peers_t,
) -> ngx_int_t {
    if let Some(primary) = unsafe { primary_peers.as_ref() } {
        pc.name = primary.name;
    }
    Status::NGX_BUSY.into()
}
