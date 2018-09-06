use std::ffi::{CStr, CString};
use std::io::{Read, Seek, SeekFrom, Write};
use std::mem::forget;
use std::os::raw::{c_char, c_int};
use std::slice::{from_raw_parts, from_raw_parts_mut};
use std::time::{SystemTime, UNIX_EPOCH};

use base::crypto::{Cipher, MemLimit, OpsLimit};
use base::init_env;
use error::Error;
use file::{File, VersionReader};
use fs::{Metadata, Version};
use repo::{OpenOptions, Repo, RepoOpener};
use trans::Eid;

#[allow(non_camel_case_types)]
type boolean_t = u8;

#[allow(non_camel_case_types)]
type uint8_t = u8;

#[allow(non_camel_case_types)]
type size_t = usize;

#[allow(non_camel_case_types)]
type time_t = u64;

#[inline]
fn to_time_t(t: SystemTime) -> time_t {
    t.duration_since(UNIX_EPOCH).unwrap().as_secs() as time_t
}

#[inline]
fn to_seek_from(offset: i64, whence: c_int) -> SeekFrom {
    match whence {
        0 => SeekFrom::Start(offset as u64),
        1 => SeekFrom::Current(offset),
        2 => SeekFrom::End(offset),
        _ => unimplemented!(),
    }
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
pub extern "C" fn zbox_opener_compress(
    opener: *mut RepoOpener,
    compress: boolean_t,
) {
    unsafe {
        (*opener).compress(compress == 1);
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
pub extern "C" fn zbox_opener_dedup_chunk(
    opener: *mut RepoOpener,
    dedup_chunk: boolean_t,
) {
    unsafe {
        (*opener).dedup_chunk(dedup_chunk == 1);
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
    compress: boolean_t,
    version_limit: uint8_t,
    dedup_chunk: boolean_t,
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
    info.compress = repo_info.compress() as boolean_t;
    info.version_limit = repo_info.version_limit();
    info.dedup_chunk = repo_info.dedup_chunk() as boolean_t;
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

impl From<Vec<Version>> for VersionList {
    fn from(versions: Vec<Version>) -> Self {
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

        let vl = VersionList {
            versions: versions.as_mut_ptr(),
            len: versions.len(),
            capacity: versions.capacity(),
        };
        forget(versions);

        vl
    }
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
                (*version_list) = VersionList::from(versions);
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

#[no_mangle]
pub extern "C" fn zbox_create_open_options() -> *mut OpenOptions {
    Box::into_raw(Box::new(OpenOptions::new()))
}

#[no_mangle]
pub extern "C" fn zbox_open_options_read(
    options: *mut OpenOptions,
    read: boolean_t,
) {
    unsafe {
        (*options).read(read == 1);
    }
}

#[no_mangle]
pub extern "C" fn zbox_open_options_write(
    options: *mut OpenOptions,
    write: boolean_t,
) {
    unsafe {
        (*options).write(write == 1);
    }
}

#[no_mangle]
pub extern "C" fn zbox_open_options_append(
    options: *mut OpenOptions,
    append: boolean_t,
) {
    unsafe {
        (*options).append(append == 1);
    }
}

#[no_mangle]
pub extern "C" fn zbox_open_options_truncate(
    options: *mut OpenOptions,
    truncate: boolean_t,
) {
    unsafe {
        (*options).truncate(truncate == 1);
    }
}

#[no_mangle]
pub extern "C" fn zbox_open_options_create(
    options: *mut OpenOptions,
    create: boolean_t,
) {
    unsafe {
        (*options).create(create == 1);
    }
}

#[no_mangle]
pub extern "C" fn zbox_open_options_create_new(
    options: *mut OpenOptions,
    create_new: boolean_t,
) {
    unsafe {
        (*options).create_new(create_new == 1);
    }
}

#[no_mangle]
pub extern "C" fn zbox_open_options_version_limit(
    options: *mut OpenOptions,
    limit: uint8_t,
) {
    unsafe {
        (*options).version_limit(limit);
    }
}

#[no_mangle]
pub extern "C" fn zbox_open_options_dedup_chunk(
    options: *mut OpenOptions,
    dedup_chunk: boolean_t,
) {
    unsafe {
        (*options).dedup_chunk(dedup_chunk == 1);
    }
}

#[no_mangle]
pub extern "C" fn zbox_free_open_options(options: *mut OpenOptions) {
    let _owned = unsafe { Box::from_raw(options) };
    // _owned droped here
}

#[no_mangle]
pub extern "C" fn zbox_file_metadata(
    metadata: *mut CMetadata,
    file: *mut File,
) -> c_int {
    unsafe {
        match (*file).metadata() {
            Ok(meta) => {
                (*metadata) = CMetadata::from(meta);
                0
            }
            Err(err) => err.into(),
        }
    }
}

#[no_mangle]
pub extern "C" fn zbox_file_history(
    version_list: *mut VersionList,
    file: *mut File,
) -> c_int {
    unsafe {
        match (*file).history() {
            Ok(versions) => {
                (*version_list) = VersionList::from(versions);
                0
            }
            Err(err) => err.into(),
        }
    }
}

#[no_mangle]
pub extern "C" fn zbox_file_curr_version(
    version_num: *mut size_t,
    file: *mut File,
) -> c_int {
    unsafe {
        match (*file).curr_version() {
            Ok(ver) => {
                (*version_num) = ver;
                0
            }
            Err(err) => err.into(),
        }
    }
}

#[no_mangle]
pub extern "C" fn zbox_file_read(
    dst: *mut uint8_t,
    len: size_t,
    file: *mut File,
) -> c_int {
    unsafe {
        let dst = from_raw_parts_mut(dst, len);
        match (*file).read(dst) {
            Ok(read) => read as c_int,
            Err(err) => Error::from(err).into(),
        }
    }
}

#[no_mangle]
pub extern "C" fn zbox_file_version_reader(
    reader: *mut *mut VersionReader,
    ver_num: size_t,
    file: *mut File,
) -> c_int {
    unsafe {
        match (*file).version_reader(ver_num) {
            Ok(rdr) => {
                *reader = Box::into_raw(Box::new(rdr));
                0
            }
            Err(err) => Error::from(err).into(),
        }
    }
}

#[no_mangle]
pub extern "C" fn zbox_file_version_read(
    dst: *mut uint8_t,
    len: size_t,
    reader: *mut VersionReader,
) -> c_int {
    unsafe {
        let dst = from_raw_parts_mut(dst, len);
        match (*reader).read(dst) {
            Ok(read) => read as c_int,
            Err(err) => Error::from(err).into(),
        }
    }
}

#[no_mangle]
pub extern "C" fn zbox_file_version_reader_seek(
    reader: *mut VersionReader,
    offset: i64,
    whence: c_int,
) -> c_int {
    unsafe {
        let seek = to_seek_from(offset, whence);
        match (*reader).seek(seek) {
            Ok(_) => 0,
            Err(err) => Error::from(err).into(),
        }
    }
}

#[no_mangle]
pub extern "C" fn zbox_close_version_reader(reader: *mut VersionReader) {
    let _owned = unsafe { Box::from_raw(reader) };
    // _owned droped here
}

#[no_mangle]
pub extern "C" fn zbox_file_write(
    file: *mut File,
    buf: *const uint8_t,
    len: size_t,
) -> c_int {
    unsafe {
        let buf = from_raw_parts(buf, len);
        match (*file).write(buf) {
            Ok(written) => written as c_int,
            Err(err) => Error::from(err).into(),
        }
    }
}

#[no_mangle]
pub extern "C" fn zbox_file_finish(file: *mut File) -> c_int {
    unsafe {
        match (*file).finish() {
            Ok(_) => 0,
            Err(err) => Error::from(err).into(),
        }
    }
}

#[no_mangle]
pub extern "C" fn zbox_file_write_once(
    file: *mut File,
    buf: *const uint8_t,
    len: size_t,
) -> c_int {
    unsafe {
        let buf = from_raw_parts(buf, len);
        match (*file).write_once(buf) {
            Ok(_) => 0,
            Err(err) => Error::from(err).into(),
        }
    }
}

#[no_mangle]
pub extern "C" fn zbox_file_seek(
    file: *mut File,
    offset: i64,
    whence: c_int,
) -> c_int {
    unsafe {
        let seek = to_seek_from(offset, whence);
        match (*file).seek(seek) {
            Ok(_) => 0,
            Err(err) => Error::from(err).into(),
        }
    }
}

#[no_mangle]
pub extern "C" fn zbox_file_set_len(file: *mut File, len: size_t) -> c_int {
    unsafe {
        match (*file).set_len(len) {
            Ok(_) => 0,
            Err(err) => Error::from(err).into(),
        }
    }
}
