use core::{
    ffi::{c_char, c_void},
    ptr,
};

use ngx::{
    core::Pool,
    ffi::{
        NGX_CONF_TAKE1, NGX_HTTP_MODULE, NGX_HTTP_SRV_CONF_OFFSET, NGX_HTTP_UPS_CONF,
        NGX_HTTP_UPSTREAM_BACKUP, NGX_HTTP_UPSTREAM_CREATE, NGX_HTTP_UPSTREAM_DOWN,
        NGX_HTTP_UPSTREAM_FAIL_TIMEOUT, NGX_HTTP_UPSTREAM_MAX_CONNS, NGX_HTTP_UPSTREAM_MAX_FAILS,
        NGX_HTTP_UPSTREAM_MODIFY, NGX_HTTP_UPSTREAM_WEIGHT, NGX_LOG_EMERG, NGX_LOG_WARN,
        ngx_command_t, ngx_conf_t, ngx_http_module_t, ngx_module_t, ngx_str_t, ngx_uint_t,
    },
    http::{HttpModule, HttpModuleServerConf, Merge, MergeConfigError, NgxHttpUpstreamModule},
    ngx_conf_log_error, ngx_string,
};

mod ewma;
mod least_conn;
mod peer;
mod policy;

use crate::{ewma::Ewma, least_conn::LeastConn, policy::BalancingPolicy};

#[derive(Clone, Copy, Debug, Default)]
#[repr(C)]
enum PolicyImpl {
    #[default]
    Unset,
    LeastConn(LeastConn),
    Ewma(Ewma),
}

#[derive(Clone, Copy, Debug, Default)]
#[repr(C)]
struct BalancerConfig {
    policy: PolicyImpl,
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

ngx::ngx_modules!(ngx_http_balancer_rs_module);

#[used]
#[allow(non_upper_case_globals)]
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
    ccf.policy = match bytes {
        b"least_conn" => PolicyImpl::LeastConn(LeastConn),
        b"ewma" => PolicyImpl::Ewma(Ewma::new()),
        _ => {
            ngx_conf_log_error!(
                NGX_LOG_EMERG,
                cf,
                "balancer_rs: unknown policy \"{}\" in \"{}\" directive",
                value,
                unsafe { &(*cmd).name }
            );
            return ngx::core::NGX_CONF_ERROR;
        }
    };

    let uscf = NgxHttpUpstreamModule::server_conf_mut(cf).expect("http upstream srv conf");
    if uscf.peer.init_upstream.is_some() {
        ngx_conf_log_error!(NGX_LOG_WARN, cf, "load balancing method redefined");
    }
    // The parser uses these flags when reading subsequent `server`
    // lines, so they must be set before the policy's `init_upstream`
    // is invoked.
    let policy_flags = (NGX_HTTP_UPSTREAM_CREATE
        | NGX_HTTP_UPSTREAM_MODIFY
        | NGX_HTTP_UPSTREAM_WEIGHT
        | NGX_HTTP_UPSTREAM_MAX_CONNS
        | NGX_HTTP_UPSTREAM_MAX_FAILS
        | NGX_HTTP_UPSTREAM_FAIL_TIMEOUT
        | NGX_HTTP_UPSTREAM_DOWN
        | NGX_HTTP_UPSTREAM_BACKUP) as ngx_uint_t;
    match ccf.policy {
        PolicyImpl::LeastConn(_) => {
            uscf.flags = policy_flags;
            uscf.peer.init_upstream = LeastConn::init_upstream();
        }
        PolicyImpl::Ewma(_) => {
            uscf.flags = policy_flags;
            uscf.peer.init_upstream = Ewma::init_upstream();
        }
        PolicyImpl::Unset => {}
    }

    ngx::core::NGX_CONF_OK
}

struct Balancer;

impl HttpModule for Balancer {
    fn module() -> &'static ngx_module_t {
        unsafe { &*::core::ptr::addr_of!(ngx_http_balancer_rs_module) }
    }

    unsafe extern "C" fn preconfiguration(cf: *mut ngx_conf_t) -> ngx::ffi::ngx_int_t {
        unsafe { ewma::register_variables(cf) }
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

        unsafe { (*conf).policy = PolicyImpl::Unset };

        conf.cast::<c_void>()
    }
}

unsafe impl HttpModuleServerConf for Balancer {
    type ServerConf = BalancerConfig;
}
