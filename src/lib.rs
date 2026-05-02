use core::ffi::{c_char, c_void};
use core::ptr;

use ngx::core::{Pool, Status};
use ngx::ffi::{
    NGX_CONF_TAKE1, NGX_HTTP_MODULE, NGX_HTTP_SRV_CONF_OFFSET, NGX_HTTP_UPS_CONF, NGX_LOG_EMERG,
    ngx_command_t, ngx_conf_t, ngx_http_module_t, ngx_http_upstream_init_pt,
    ngx_http_upstream_init_round_robin, ngx_http_upstream_srv_conf_t, ngx_int_t, ngx_module_t,
    ngx_str_t, ngx_uint_t,
};
use ngx::http::{HttpModule, Merge, MergeConfigError};
use ngx::http::{HttpModuleServerConf, NgxHttpUpstreamModule};
use ngx::{ngx_conf_log_error, ngx_string};

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
#[repr(C)]
enum Policy {
    #[default]
    Unset,
    LeastConn,
}

#[derive(Clone, Copy, Debug, Default)]
#[repr(C)]
struct SrvConfig {
    policy: Policy,
    original_init_upstream: ngx_http_upstream_init_pt,
}

impl Merge for SrvConfig {
    fn merge(&mut self, _prev: &SrvConfig) -> Result<(), MergeConfigError> {
        Ok(())
    }
}

static NGX_HTTP_BALANCER_RS_CTX: ngx_http_module_t = ngx_http_module_t {
    preconfiguration: Some(Module::preconfiguration),
    postconfiguration: Some(Module::postconfiguration),
    create_main_conf: None,
    init_main_conf: None,
    create_srv_conf: Some(Module::create_srv_conf),
    merge_srv_conf: Some(Module::merge_srv_conf),
    create_loc_conf: None,
    merge_loc_conf: None,
};

static mut NGX_HTTP_BALANCER_RS_COMMANDS: [ngx_command_t; 2] = [
    ngx_command_t {
        name: ngx_string!("balancer_rs"),
        type_: (NGX_HTTP_UPS_CONF | NGX_CONF_TAKE1) as ngx_uint_t,
        set: Some(ngx_http_balancer_rs_commands_set),
        conf: NGX_HTTP_SRV_CONF_OFFSET,
        offset: 0,
        post: ptr::null_mut(),
    },
    ngx_command_t::empty(),
];

#[cfg(feature = "export-modules")]
ngx::ngx_modules!(ngx_http_balancer_rs_module);

#[used]
#[allow(non_upper_case_globals)]
#[cfg_attr(not(feature = "export-modules"), unsafe(no_mangle))]
pub static mut ngx_http_balancer_rs_module: ngx_module_t = ngx_module_t {
    ctx: &raw const NGX_HTTP_BALANCER_RS_CTX as _,
    commands: unsafe { &raw mut NGX_HTTP_BALANCER_RS_COMMANDS[0] },
    type_: NGX_HTTP_MODULE as _,
    ..ngx_module_t::default()
};

unsafe extern "C" fn ngx_http_balancer_rs_init_upstream(
    cf: *mut ngx_conf_t,
    us: *mut ngx_http_upstream_srv_conf_t,
) -> ngx_int_t {
    let us = unsafe { &mut *us };
    let Some(hccf) = Module::server_conf_mut(us) else {
        ngx_conf_log_error!(NGX_LOG_EMERG, cf, "balancer_rs: missing upstream srv_conf");
        return isize::from(Status::NGX_ERROR);
    };

    let init_upstream_ptr = hccf
        .original_init_upstream
        .unwrap_or(ngx_http_upstream_init_round_robin);

    if unsafe { init_upstream_ptr(cf, us) } != Status::NGX_OK.into() {
        ngx_conf_log_error!(
            NGX_LOG_EMERG,
            cf,
            "balancer_rs: original init_upstream failed"
        );
        return isize::from(Status::NGX_ERROR);
    }

    isize::from(Status::NGX_OK)
}

unsafe extern "C" fn ngx_http_balancer_rs_commands_set(
    cf: *mut ngx_conf_t,
    cmd: *mut ngx_command_t,
    conf: *mut c_void,
) -> *mut c_char {
    let cf = unsafe { &mut *cf };
    let args: &[ngx_str_t] = unsafe { (*cf.args).as_slice() };

    let ccf = unsafe { &mut *conf.cast::<SrvConfig>() };

    let Some(value) = args.get(1) else {
        ngx_conf_log_error!(NGX_LOG_EMERG, cf, "balancer_rs: missing policy argument");
        return ngx::core::NGX_CONF_ERROR;
    };

    let bytes = unsafe { core::slice::from_raw_parts(value.data, value.len) };
    ccf.policy = if bytes == b"least_conn" {
        Policy::LeastConn
    } else {
        ngx_conf_log_error!(
            NGX_LOG_EMERG,
            cf,
            "balancer_rs: unknown policy \"{}\" in \"{}\" directive",
            value,
            unsafe { &(*cmd).name }
        );
        return ngx::core::NGX_CONF_ERROR;
    };

    let uscf = NgxHttpUpstreamModule::server_conf_mut(cf).expect("http upstream srv conf");

    ccf.original_init_upstream = if uscf.peer.init_upstream.is_some() {
        uscf.peer.init_upstream
    } else {
        Some(ngx_http_upstream_init_round_robin)
    };

    uscf.peer.init_upstream = Some(ngx_http_balancer_rs_init_upstream);

    ngx::core::NGX_CONF_OK
}

struct Module;

impl HttpModule for Module {
    fn module() -> &'static ngx_module_t {
        unsafe { &*::core::ptr::addr_of!(ngx_http_balancer_rs_module) }
    }

    unsafe extern "C" fn create_srv_conf(cf: *mut ngx_conf_t) -> *mut c_void {
        let pool = unsafe { Pool::from_ngx_pool((*cf).pool) };
        let conf = pool.alloc_type::<SrvConfig>();
        if conf.is_null() {
            ngx_conf_log_error!(
                NGX_LOG_EMERG,
                cf,
                "balancer_rs: could not allocate memory for config"
            );
            return ptr::null_mut();
        }

        conf.cast::<c_void>()
    }
}

unsafe impl HttpModuleServerConf for Module {
    type ServerConf = SrvConfig;
}
