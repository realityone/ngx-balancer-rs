#![allow(dead_code)]

use core::ffi::c_void;

use ngx::ffi::{
    ngx_conf_t, ngx_http_request_t, ngx_http_upstream_init_pt, ngx_http_upstream_srv_conf_t,
    ngx_int_t, ngx_peer_connection_t, ngx_uint_t,
};

use crate::{Policy, policy::BalancingPolicy};

pub struct LeastConn;

impl BalancingPolicy for LeastConn {
    const KIND: Policy = Policy::LeastConn;

    fn init_upstream() -> ngx_http_upstream_init_pt {
        Some(init_upstream)
    }
}

unsafe extern "C" fn init_upstream(
    _cf: *mut ngx_conf_t,
    _us: *mut ngx_http_upstream_srv_conf_t,
) -> ngx_int_t {
    todo!()
}

unsafe extern "C" fn init_peer(
    _r: *mut ngx_http_request_t,
    _us: *mut ngx_http_upstream_srv_conf_t,
) -> ngx_int_t {
    todo!()
}

unsafe extern "C" fn get_peer(
    _pc: *mut ngx_peer_connection_t,
    _data: *mut c_void,
) -> ngx_int_t {
    todo!()
}

unsafe extern "C" fn free_peer(
    _pc: *mut ngx_peer_connection_t,
    _data: *mut c_void,
    _state: ngx_uint_t,
) {
    todo!()
}
