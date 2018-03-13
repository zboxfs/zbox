use std::ffi::{CStr, CString};
use std::os::raw::{c_char, c_int, c_long};
use std::time::{SystemTime, UNIX_EPOCH};
use std::mem::forget;

use base::init_env;
use base::crypto::{Cipher, MemLimit, OpsLimit};
use trans::Eid;
use repo::{Repo, RepoOpener};
use fs::Metadata;
use file::File;

#[allow(non_camel_case_types)]
type boolean_t = u8;

#[allow(non_camel_case_types)]
type uint8_t = u8;

#[allow(non_camel_case_types)]
type size_t = usize;

#[allow(non_camel_case_types)]
type time_t = c_long;

#[inline]
fn to_time_t(t: SystemTime) -> time_t {
    t.duration_since(UNIX_EPOCH).unwrap().as_secs() as time_t
}

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
    let ops_limit = OpsLimit::from(limit);
    unsafe {
        (*opener).ops_limit(ops_limit);
    }
}

#[no_mangle]
pub extern "C" fn zbox_opener_mem_limit(opener: *mut RepoOpener, limit: c_int) {
    let mem_limit = MemLimit::from(limit);
    unsafe {
        (*opener).mem_limit(mem_limit);
    }
}

#[no_mangle]
pub extern "C" fn zbox_opener_cipher(opener: *mut RepoOpener, cipher: c_int) {
    let cipher = Cipher::from(cipher);
    unsafe {
        (*opener).cipher(cipher);
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
    repo: *mut *mut Repo,
    opts: &RepoOpener,
    uri: *const c_char,
    pwd: *const c_char,
) -> c_int {
    unsafe {
        let uri_str = CStr::from_ptr(uri).to_str().unwrap();
        let pwd_str = CStr::from_ptr(pwd).to_str().unwrap();
        match opts.open(uri_str, pwd_str) {
            Ok(r) => {
                *repo = Box::into_raw(Box::new(r));
                0
            }
            Err(err) => err.into(),
        }
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
    ops_limit: c_int,
    mem_limit: c_int,
    cipher: c_int,
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
    info.ops_limit = repo_info.ops_limit().into();
    info.mem_limit = repo_info.mem_limit().into();
    info.cipher = repo_info.cipher().into();
    info.version_limit = repo_info.version_limit();
    info.is_read_only = repo_info.is_read_only() as boolean_t;
    info.created = to_time_t(ctime);
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

#[no_mangle]
pub extern "C" fn zbox_repo_reset_password(
    repo: *mut Repo,
    old_pwd: *const c_char,
    new_pwd: *const c_char,
    ops_limit: c_int,
    mem_limit: c_int,
) -> c_int {
    unsafe {
        let old_pwd = CStr::from_ptr(old_pwd).to_str().unwrap();
        let new_pwd = CStr::from_ptr(new_pwd).to_str().unwrap();
        let ops_limit = OpsLimit::from(ops_limit);
        let mem_limit = MemLimit::from(mem_limit);
        match (*repo).reset_password(old_pwd, new_pwd, ops_limit, mem_limit) {
            Ok(_) => 0,
            Err(err) => err.into(),
        }
    }
}

#[no_mangle]
pub extern "C" fn zbox_repo_path_exists(
    repo: *mut Repo,
    path: *const c_char,
) -> boolean_t {
    unsafe {
        let path = CStr::from_ptr(path).to_str().unwrap();
        (*repo).path_exists(path) as u8
    }
}

#[no_mangle]
pub extern "C" fn zbox_repo_is_file(
    repo: *mut Repo,
    path: *const c_char,
) -> boolean_t {
    unsafe {
        let path = CStr::from_ptr(path).to_str().unwrap();
        (*repo).is_file(path) as u8
    }
}

#[no_mangle]
pub extern "C" fn zbox_repo_is_dir(
    repo: *mut Repo,
    path: *const c_char,
) -> boolean_t {
    unsafe {
        let path = CStr::from_ptr(path).to_str().unwrap();
        (*repo).is_dir(path) as u8
    }
}

#[no_mangle]
pub extern "C" fn zbox_repo_create_file(
    file: *mut *mut File,
    repo: *mut Repo,
    path: *const c_char,
) -> c_int {
    unsafe {
        let path = CStr::from_ptr(path).to_str().unwrap();
        match (*repo).create_file(path) {
            Ok(f) => {
                *file = Box::into_raw(Box::new(f));
                0
            }
            Err(err) => err.into(),
        }
    }
}

#[no_mangle]
pub extern "C" fn zbox_repo_open_file(
    file: *mut *mut File,
    repo: *mut Repo,
    path: *const c_char,
) -> c_int {
    unsafe {
        let path = CStr::from_ptr(path).to_str().unwrap();
        match (*repo).open_file(path) {
            Ok(f) => {
                *file = Box::into_raw(Box::new(f));
                0
            }
            Err(err) => err.into(),
        }
    }
}

#[no_mangle]
pub extern "C" fn zbox_close_file(file: *mut File) {
    let _owned = unsafe { Box::from_raw(file) };
    // _owned droped here
}

#[no_mangle]
pub extern "C" fn zbox_repo_create_dir(
    repo: *mut Repo,
    path: *const c_char,
) -> c_int {
    unsafe {
        let path = CStr::from_ptr(path).to_str().unwrap();
        match (*repo).create_dir(path) {
            Ok(_) => 0,
            Err(err) => err.into(),
        }
    }
}

#[no_mangle]
pub extern "C" fn zbox_repo_create_dir_all(
    repo: *mut Repo,
    path: *const c_char,
) -> c_int {
    unsafe {
        let path = CStr::from_ptr(path).to_str().unwrap();
        match (*repo).create_dir_all(path) {
            Ok(_) => 0,
            Err(err) => err.into(),
        }
    }
}

#[repr(C)]
pub struct CMetadata {
    ftype: i32,
    len: size_t,
    curr_version: size_t,
    ctime: time_t,
    mtime: time_t,
}

impl From<Metadata> for CMetadata {
    fn from(n: Metadata) -> Self {
        CMetadata {
            ftype: n.file_type().into(),
            len: n.len(),
            curr_version: n.curr_version(),
            ctime: to_time_t(n.created()),
            mtime: to_time_t(n.modified()),
        }
    }
}

#[repr(C)]
pub struct CDirEntry {
    path: *mut c_char,
    file_name: *mut c_char,
    metadata: CMetadata,
}

#[repr(C)]
pub struct DirEntryList {
    entries: *mut CDirEntry,
    len: size_t,
    capacity: size_t,
}

#[no_mangle]
pub extern "C" fn zbox_repo_read_dir(
    entry_list: *mut DirEntryList,
    repo: *mut Repo,
    path: *const c_char,
) -> c_int {
    unsafe {
        let path = CStr::from_ptr(path).to_str().unwrap();
        match (*repo).read_dir(path) {
            Ok(entries) => {
                let mut entries: Vec<CDirEntry> = entries
                    .iter()
                    .map(|ent| -> CDirEntry {
                        let path =
                            CString::new(ent.path().to_str().unwrap()).unwrap();
                        let name = CString::new(ent.file_name()).unwrap();

                        CDirEntry {
                            path: path.into_raw(),
                            file_name: name.into_raw(),
                            metadata: CMetadata::from(ent.metadata()),
                        }
                    })
                    .collect();

                entries.shrink_to_fit();
                (*entry_list).entries = entries.as_mut_ptr();
                (*entry_list).len = entries.len();
                (*entry_list).capacity = entries.capacity();
                forget(entries);
                0
            }
            Err(err) => err.into(),
        }
    }
}

#[no_mangle]
pub extern "C" fn zbox_destroy_dir_entry_list(entry_list: *mut DirEntryList) {
    unsafe {
        let entries = Vec::from_raw_parts(
            (*entry_list).entries,
            (*entry_list).len,
            (*entry_list).capacity,
        );
        for entry in entries {
            let _path = CString::from_raw(entry.path);
            let _file_name = CString::from_raw(entry.file_name);
            // _path and _file_name droped here
        }
        // entries droped here
    }
}

#[no_mangle]
pub extern "C" fn zbox_repo_metadata(
    metadata: *mut CMetadata,
    repo: *mut Repo,
    path: *const c_char,
) -> c_int {
    unsafe {
        let path = CStr::from_ptr(path).to_str().unwrap();
        match (*repo).metadata(path) {
            Ok(meta) => {
                (*metadata) = CMetadata::from(meta);
                0
            }
            Err(err) => err.into(),
        }
    }
}

#[repr(C)]
pub struct CVersion {
    num: size_t,
    len: size_t,
    created: time_t,
}

#[repr(C)]
pub struct VersionList {
    versions: *mut CVersion,
    len: size_t,
    capacity: size_t,
}

#[no_mangle]
pub extern "C" fn zbox_repo_history(
    version_list: *mut VersionList,
    repo: *mut Repo,
    path: *const c_char,
) -> c_int {
    unsafe {
        let path = CStr::from_ptr(path).to_str().unwrap();
        match (*repo).history(path) {
            Ok(versions) => {
                let mut versions: Vec<CVersion> = versions
                    .iter()
                    .map(|ver| -> CVersion {
                        CVersion {
                            num: ver.num(),
                            len: ver.len(),
                            created: to_time_t(ver.created()),
                        }
                    })
                    .collect();

                versions.shrink_to_fit();
                (*version_list).versions = versions.as_mut_ptr();
                (*version_list).len = versions.len();
                (*version_list).capacity = versions.capacity();
                forget(versions);
                0
            }
            Err(err) => err.into(),
        }
    }
}

#[no_mangle]
pub extern "C" fn zbox_destroy_version_list(version_list: *mut VersionList) {
    unsafe {
        let _versions = Vec::from_raw_parts(
            (*version_list).versions,
            (*version_list).len,
            (*version_list).capacity,
        );
        // _versions droped here
    }
}

#[no_mangle]
pub extern "C" fn zbox_repo_copy(
    to: *const c_char,
    from: *const c_char,
    repo: *mut Repo,
) -> c_int {
    unsafe {
        let to = CStr::from_ptr(to).to_str().unwrap();
        let from = CStr::from_ptr(from).to_str().unwrap();
        match (*repo).copy(from, to) {
            Ok(_) => 0,
            Err(err) => err.into(),
        }
    }
}

#[no_mangle]
pub extern "C" fn zbox_repo_remove_file(
    path: *const c_char,
    repo: *mut Repo,
) -> c_int {
    unsafe {
        let path = CStr::from_ptr(path).to_str().unwrap();
        match (*repo).remove_file(path) {
            Ok(_) => 0,
            Err(err) => err.into(),
        }
    }
}

#[no_mangle]
pub extern "C" fn zbox_repo_remove_dir(
    path: *const c_char,
    repo: *mut Repo,
) -> c_int {
    unsafe {
        let path = CStr::from_ptr(path).to_str().unwrap();
        match (*repo).remove_dir(path) {
            Ok(_) => 0,
            Err(err) => err.into(),
        }
    }
}

#[no_mangle]
pub extern "C" fn zbox_repo_remove_dir_all(
    path: *const c_char,
    repo: *mut Repo,
) -> c_int {
    unsafe {
        let path = CStr::from_ptr(path).to_str().unwrap();
        match (*repo).remove_dir_all(path) {
            Ok(_) => 0,
            Err(err) => err.into(),
        }
    }
}

#[no_mangle]
pub extern "C" fn zbox_repo_rename(
    to: *const c_char,
    from: *const c_char,
    repo: *mut Repo,
) -> c_int {
    unsafe {
        let to = CStr::from_ptr(to).to_str().unwrap();
        let from = CStr::from_ptr(from).to_str().unwrap();
        match (*repo).rename(from, to) {
            Ok(_) => 0,
            Err(err) => err.into(),
        }
    }
}
