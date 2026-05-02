use core::{
    ffi::{c_char, c_void},
    ptr,
};

use ngx::{
    core::Pool,
    ffi::{
        NGX_CONF_TAKE1, NGX_HTTP_MODULE, NGX_HTTP_SRV_CONF_OFFSET, NGX_HTTP_UPS_CONF,
        NGX_LOG_EMERG, ngx_command_t, ngx_conf_t, ngx_http_module_t, ngx_module_t, ngx_str_t,
        ngx_uint_t,
    },
    http::{HttpModule, HttpModuleServerConf, Merge, MergeConfigError},
    ngx_conf_log_error, ngx_string,
};

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
#[repr(C)]
enum Policy {
    #[default]
    Unset,
    LeastConn,
}

#[derive(Clone, Copy, Debug, Default)]
#[repr(C)]
struct BalancerConfig {
    policy: Policy,
}

impl Merge for BalancerConfig {
    fn merge(&mut self, _prev: &BalancerConfig) -> Result<(), MergeConfigError> {
        Ok(())
    }
}

static NGX_HTTP_BALANCER_RS_CTX: ngx_http_module_t = ngx_http_module_t {
    preconfiguration: Some(Balancer::preconfiguration),
    postconfiguration: Some(Balancer::postconfiguration),
    create_main_conf: None,
    init_main_conf: None,
    create_srv_conf: Some(Balancer::create_srv_conf),
    merge_srv_conf: Some(Balancer::merge_srv_conf),
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

unsafe extern "C" fn ngx_http_balancer_rs_commands_set(
    cf: *mut ngx_conf_t,
    cmd: *mut ngx_command_t,
    conf: *mut c_void,
) -> *mut c_char {
    let cf = unsafe { &mut *cf };
    let args: &[ngx_str_t] = unsafe { (*cf.args).as_slice() };

    let ccf = unsafe { &mut *conf.cast::<BalancerConfig>() };
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

    ngx::core::NGX_CONF_OK
}

struct Balancer;

impl HttpModule for Balancer {
    fn module() -> &'static ngx_module_t {
        unsafe { &*::core::ptr::addr_of!(ngx_http_balancer_rs_module) }
    }

    unsafe extern "C" fn create_srv_conf(cf: *mut ngx_conf_t) -> *mut c_void {
        let pool = unsafe { Pool::from_ngx_pool((*cf).pool) };
        let conf = pool.alloc_type::<BalancerConfig>();
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

unsafe impl HttpModuleServerConf for Balancer {
    type ServerConf = BalancerConfig;
}
