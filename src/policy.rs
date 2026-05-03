use ngx::ffi::ngx_http_upstream_init_pt;

/// A load-balancing policy for an `upstream {}` block.
///
/// Implementations are simple types that may carry per-upstream
/// state (allocated lazily by `init_upstream`); the trait itself only
/// exposes the C entry point that nginx invokes once at upstream
/// config init time. That entry point is responsible for installing
/// the per-request callbacks (`peer.init`, and through it `peer.get`
/// / `peer.free`) on the upstream configuration.
pub trait BalancingPolicy {
    fn init_upstream() -> ngx_http_upstream_init_pt;
}
