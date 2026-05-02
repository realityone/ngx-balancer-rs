use ngx::ffi::ngx_http_upstream_init_pt;

use crate::Policy;

/// A load-balancing policy for an `upstream {}` block.
///
/// `KIND` ties each impl to the runtime [`Policy`] tag stored in
/// `BalancerConfig`; `init_upstream` returns the C entry point that
/// nginx invokes once at upstream config init time. That entry point
/// is responsible for installing the per-request callbacks
/// (`peer.init`, and through it `peer.get` / `peer.free`) on the
/// upstream configuration.
pub trait BalancingPolicy {
    #[allow(dead_code)]
    const KIND: Policy;

    fn init_upstream() -> ngx_http_upstream_init_pt;
}
