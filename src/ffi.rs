use std::ffi::{CStr, CString};
use std::os::raw::{c_char, c_int, c_long};
use std::time::UNIX_EPOCH;

use base::init_env;
use base::crypto::{Cipher, MemLimit, OpsLimit};
use trans::Eid;
use repo::{Repo, RepoOpener};

#[allow(non_camel_case_types)]
type boolean_t = u8;

#[allow(non_camel_case_types)]
type uint8_t = u8;

#[allow(non_camel_case_types)]
type time_t = c_long;

#[no_mangle]
pub extern "C" fn zbox_init_env() -> c_int {
    init_env();
    0
}

#[no_mangle]
pub extern "C" fn zbox_create_opener() -> *mut RepoOpener {
    Box::into_raw(Box::new(RepoOpener::new()))
}

#[no_mangle]
pub extern "C" fn zbox_opener_ops_limit(opener: *mut RepoOpener, limit: c_int) {
    unsafe {
        match limit {
            0 => {
                (*opener).ops_limit(OpsLimit::Interactive);
            }
            1 => {
                (*opener).ops_limit(OpsLimit::Moderate);
            }
            2 => {
                (*opener).ops_limit(OpsLimit::Sensitive);
            }
            _ => unreachable!(),
        }
    }
}

#[no_mangle]
pub extern "C" fn zbox_opener_mem_limit(opener: *mut RepoOpener, limit: c_int) {
    unsafe {
        match limit {
            0 => {
                (*opener).mem_limit(MemLimit::Interactive);
            }
            1 => {
                (*opener).mem_limit(MemLimit::Moderate);
            }
            2 => {
                (*opener).mem_limit(MemLimit::Sensitive);
            }
            _ => unreachable!(),
        }
    }
}

#[no_mangle]
pub extern "C" fn zbox_opener_cipher(opener: *mut RepoOpener, cipher: c_int) {
    unsafe {
        match cipher {
            0 => {
                (*opener).cipher(Cipher::Xchacha);
            }
            1 => {
                (*opener).cipher(Cipher::Aes);
            }
            _ => unreachable!(),
        }
    }
}

#[no_mangle]
pub extern "C" fn zbox_opener_create(
    opener: *mut RepoOpener,
    create: boolean_t,
) {
    unsafe {
        (*opener).create(create == 1);
    }
}

#[no_mangle]
pub extern "C" fn zbox_opener_create_new(
    opener: *mut RepoOpener,
    create_new: boolean_t,
) {
    unsafe {
        (*opener).create_new(create_new == 1);
    }
}

#[no_mangle]
pub extern "C" fn zbox_opener_version_limit(
    opener: *mut RepoOpener,
    limit: uint8_t,
) {
    unsafe {
        (*opener).version_limit(limit);
    }
}

#[no_mangle]
pub extern "C" fn zbox_opener_read_only(
    opener: *mut RepoOpener,
    read_only: boolean_t,
) {
    unsafe {
        (*opener).read_only(read_only == 1);
    }
}

#[no_mangle]
pub extern "C" fn zbox_free_opener(opener: *mut RepoOpener) {
    let _owned = unsafe { Box::from_raw(opener) };
    // _owned droped here
}

#[no_mangle]
pub extern "C" fn zbox_open_repo(
    out: *mut *mut Repo,
    opts: &RepoOpener,
    uri: *const c_char,
    pwd: *const c_char,
) -> c_int {
    let uri_str = unsafe { CStr::from_ptr(uri).to_str().unwrap() };
    let pwd_str = unsafe { CStr::from_ptr(pwd).to_str().unwrap() };
    match opts.open(uri_str, pwd_str) {
        Ok(repo) => {
            unsafe {
                *out = Box::into_raw(Box::new(repo));
            }
            0
        }
        Err(err) => err.into(),
    }
}

#[no_mangle]
pub extern "C" fn zbox_close_repo(repo: *mut Repo) {
    let _owned = unsafe { Box::from_raw(repo) };
    // _owned droped here
}

#[no_mangle]
pub extern "C" fn zbox_repo_exists(
    out: *mut boolean_t,
    uri: *const c_char,
) -> c_int {
    let uri_str = unsafe { CStr::from_ptr(uri).to_str().unwrap() };
    match Repo::exists(uri_str) {
        Ok(result) => {
            unsafe {
                *out = result as boolean_t;
            }
            0
        }
        Err(err) => err.into(),
    }
}

#[repr(C)]
pub struct CRepoInfo {
    volume_id: Eid,
    version: *mut c_char,
    uri: *mut c_char,
    ops_limit: OpsLimit,
    mem_limit: MemLimit,
    cipher: Cipher,
    version_limit: uint8_t,
    is_read_only: boolean_t,
    created: time_t,
}

#[no_mangle]
pub extern "C" fn zbox_get_repo_info(info: *mut CRepoInfo, repo: *const Repo) {
    let repo_info = unsafe { (*repo).info() };
    let version = CString::new(repo_info.version()).unwrap();
    let uri = CString::new(repo_info.uri()).unwrap();
    let ctime = repo_info.created();

    let info = unsafe { &mut (*info) };
    info.volume_id = repo_info.volume_id().clone();
    info.version = version.into_raw();
    info.uri = uri.into_raw();
    info.ops_limit = repo_info.ops_limit();
    info.mem_limit = repo_info.mem_limit();
    info.cipher = repo_info.cipher();
    info.version_limit = repo_info.version_limit();
    info.is_read_only = repo_info.is_read_only() as boolean_t;
    info.created =
        ctime.duration_since(UNIX_EPOCH).unwrap().as_secs() as time_t;
}

#[no_mangle]
pub extern "C" fn zbox_destroy_repo_info(info: *mut CRepoInfo) {
    unsafe {
        let info = &mut (*info);
        let _version = CString::from_raw(info.version);
        let _uri = CString::from_raw(info.uri);
    }
    // drop _version and _uri here
}
