use libc;

use base::init_env;

#[no_mangle]
pub extern "C" fn zbox_init_env() -> libc::c_int {
    init_env();
    0
}
