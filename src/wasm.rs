use std::error::Error as StdError;
use std::io::{Read, Seek, SeekFrom, Write};
use std::mem;
use std::ptr;
use std::result;
use std::time::SystemTime;

use js_sys;
use wasm_bindgen::prelude::*;
use wasm_bindgen::JsCast;
use web_sys;
//use http::header::{ HeaderMap, HeaderName, HeaderValue };

use base;
use base::crypto::{Cipher, MemLimit, OpsLimit};
use error::Error;
use file::{File as ZboxFile, VersionReader as ZboxVersionReader};
use fs::{
    DirEntry as ZboxDirEntry, Metadata as ZboxMetadata, Version as ZboxVersion,
};
use repo::{
    OpenOptions as ZboxOpenOptions, Repo as ZboxRepo, RepoInfo as ZboxRepoInfo,
    RepoOpener as ZboxRepoOpener,
};

#[wasm_bindgen]
extern "C" {
    #[wasm_bindgen(js_namespace = console)]
    fn log(a: &str);
}

macro_rules! console_log {
    ($($t:tt)*) => (log(&format!($($t)*)))
}

#[wasm_bindgen]
pub fn malloc(size: u32) -> u32 {
    let capacity = (8 + size) as usize;
    let mut buf: Vec<u8> = Vec::with_capacity(capacity);
    let len = buf.len();
    let ptr = buf.as_mut_ptr();
    let p = ptr as *mut u32;
    unsafe {
        mem::forget(buf);
        ptr::write(p, len as u32);
        ptr::write(p.add(1), capacity as u32);
        //console_log!("malloc: size: {:?}, ptr: {:?}", size, ptr);
        ptr.add(8) as u32
    }
}

#[wasm_bindgen]
pub fn calloc(nmemb: u32, size: u32) -> u32 {
    //console_log!("calloc: nmemb: {:?}, size: {:?}", nmemb, size);
    malloc(nmemb * size)
}

#[wasm_bindgen]
pub fn free(ptr: u32) {
    if ptr == 0 {
        return;
    }
    //console_log!("free: {:?}", ptr);
    let p = ptr as *mut u8;
    unsafe {
        let base = p.sub(8) as *mut u32;
        let len = ptr::read(base) as usize;
        let capacity = ptr::read(base.add(1)) as usize;
        let _buf = Vec::from_raw_parts(base, len, capacity);
        // buffer drops here
    }
}

#[wasm_bindgen]
pub fn __errno_location() -> i32 {
    0
}

#[wasm_bindgen]
pub fn strlen(s: u32) -> u32 {
    console_log!("[rust] call strlen()");
    let p = s as *const u8;
    let mut i: usize = 0;
    unsafe {
        while i < std::u32::MAX as usize {
            if *p.add(i) == 0 {
                break;
            }
            i += 1;
        }
    }
    i as u32
}

#[wasm_bindgen]
pub fn strchr(s: u32, c: u32) -> u32 {
    console_log!("[rust] call strchr()");
    let mut p = s as *const u8;
    let c = c as u8;
    unsafe {
        while *p != 0 {
            if *p == c {
                return p as u32;
            }
            p = p.offset(1);
        }
    }
    0
}

#[wasm_bindgen]
pub fn strncmp(s1: u32, s2: u32, n: u32) -> i32 {
    console_log!("[rust] call strncmp()");
    let s1 =
        unsafe { core::slice::from_raw_parts(s1 as *const u8, n as usize) };
    let s2 =
        unsafe { core::slice::from_raw_parts(s2 as *const u8, n as usize) };

    for (&a, &b) in s1.iter().zip(s2.iter()) {
        let val = (a as i32) - (b as i32);
        if a != b || a == 0 {
            return val;
        }
    }

    0
}

#[wasm_bindgen]
pub fn js_random_uint32() -> u32 {
    // since the wasm is running in a web worker, the global scope should be
    // WorkerGlobalScope, not Window
    let global = js_sys::global();
    let worker = global.dyn_into::<web_sys::WorkerGlobalScope>().unwrap();

    // get crypto from worker
    let crypto = worker.crypto().unwrap();

    // generate a random u32 number using the global scope crypto
    let mut buf = vec![0u8; 4];
    crypto.get_random_values_with_u8_array(&mut buf).unwrap();
    let ret: u32 = (buf[3] as u32) << 24
        | (buf[2] as u32) << 16
        | (buf[1] as u32) << 8
        | (buf[0] as u32);

    ret
}

#[wasm_bindgen]
pub fn emscripten_asm_const_int(_a: i32, _b: i32, _c: i32) -> i32 {
    0
}

#[wasm_bindgen]
pub fn __assert_fail(_assertion: i32, _file: i32, _line: i32, _function: i32) {
    wasm_bindgen::throw_str("assert in C trapped");
}

#[wasm_bindgen]
pub fn abort() {
    wasm_bindgen::throw_str("abort");
}

type Result<T> = result::Result<T, JsValue>;

macro_rules! map_js_err {
    ($x:expr) => {
        $x.map_err(|e| JsValue::from(e.description()));
    };
}

/*#[derive(Debug, Serialize, Deserialize)]
pub struct Resp {
    pub origin: String,
}*/

#[inline]
fn time_to_u64(t: SystemTime) -> u64 {
    t.duration_since(SystemTime::UNIX_EPOCH).unwrap().as_secs()
}

#[wasm_bindgen]
pub fn init_env() {
    base::init_env();
}

#[wasm_bindgen]
pub struct RepoOpener {
    inner: ZboxRepoOpener,
}

#[wasm_bindgen(js_class = RepoOpener)]
impl RepoOpener {
    #[wasm_bindgen(constructor)]
    pub fn new() -> Self {
        let mut inner = ZboxRepoOpener::new();
        inner
            .cipher(Cipher::Xchacha)
            .ops_limit(OpsLimit::Interactive)
            .mem_limit(MemLimit::Interactive);
        RepoOpener { inner }
    }

    pub fn create(&mut self, create: bool) {
        self.inner.create(create);
    }

    #[wasm_bindgen(js_name = createNew)]
    pub fn create_new(mut self, create_new: bool) {
        self.inner.create_new(create_new);
    }

    pub fn compress(mut self, compress: bool) {
        self.inner.compress(compress);
    }

    #[wasm_bindgen(js_name = versionLimit)]
    pub fn version_limit(mut self, version_limit: u8) {
        self.inner.version_limit(version_limit);
    }

    #[wasm_bindgen(js_name = dedupChunk)]
    pub fn dedup_chunk(mut self, dedup_chunk: bool) {
        self.inner.dedup_chunk(dedup_chunk);
    }

    #[wasm_bindgen(js_name = readOnly)]
    pub fn read_only(mut self, read_only: bool) {
        self.inner.read_only(read_only);
    }

    pub fn open(self, uri: &str, pwd: &str) -> Result<Repo> {
        let repo = map_js_err!(self.inner.open(uri, pwd))?;
        Ok(Repo { inner: Some(repo) })
    }
}

#[wasm_bindgen]
pub struct OpenOptions {
    inner: ZboxOpenOptions,
}

#[wasm_bindgen(js_class = OpenOptions)]
impl OpenOptions {
    #[wasm_bindgen(constructor)]
    pub fn new() -> Self {
        OpenOptions {
            inner: ZboxOpenOptions::new(),
        }
    }

    pub fn read(&mut self, read: bool) {
        self.inner.read(read);
    }

    pub fn write(&mut self, write: bool) {
        self.inner.write(write);
    }

    pub fn append(&mut self, append: bool) {
        self.inner.append(append);
    }

    pub fn truncate(&mut self, truncate: bool) {
        self.inner.truncate(truncate);
    }

    pub fn create(&mut self, create: bool) {
        self.inner.create(create);
    }

    #[wasm_bindgen(js_name = createNew)]
    pub fn create_new(mut self, create_new: bool) {
        self.inner.create_new(create_new);
    }

    #[wasm_bindgen(js_name = versionLimit)]
    pub fn version_limit(mut self, version_limit: u8) {
        self.inner.version_limit(version_limit);
    }

    #[wasm_bindgen(js_name = dedupChunk)]
    pub fn dedup_chunk(mut self, dedup_chunk: bool) {
        self.inner.dedup_chunk(dedup_chunk);
    }

    pub fn open(self, repo: &mut Repo, path: &str) -> Result<File> {
        let file = map_js_err!(match repo.inner {
            Some(ref mut repo) => self.inner.open(repo, path),
            None => Err(Error::RepoClosed),
        })?;
        Ok(File { inner: Some(file) })
    }
}

#[allow(non_snake_case)]
#[derive(Serialize)]
pub struct RepoInfo {
    pub volumeId: String,
    pub ver: String,
    pub uri: String,
    pub compress: bool,
    pub versionLimit: u8,
    pub dedupChunk: bool,
    pub readOnly: bool,
    pub ctime: u64,
}

impl From<ZboxRepoInfo> for RepoInfo {
    fn from(info: ZboxRepoInfo) -> Self {
        RepoInfo {
            volumeId: info.volume_id().to_string(),
            ver: info.version(),
            uri: info.uri().to_owned(),
            compress: info.compress(),
            versionLimit: info.version_limit(),
            dedupChunk: info.dedup_chunk(),
            readOnly: info.is_read_only(),
            ctime: time_to_u64(info.created_at()),
        }
    }
}

#[allow(non_snake_case)]
#[derive(Serialize)]
pub struct Metadata {
    pub fileType: i32,
    pub isFile: bool,
    pub isDir: bool,
    pub len: usize,
    pub currVersion: usize,
    pub createdAt: u64,
    pub modifiedAt: u64,
}

impl From<ZboxMetadata> for Metadata {
    fn from(md: ZboxMetadata) -> Self {
        Metadata {
            fileType: md.file_type().into(),
            isFile: md.is_file(),
            isDir: md.is_dir(),
            len: md.len(),
            currVersion: md.curr_version(),
            createdAt: time_to_u64(md.created_at()),
            modifiedAt: time_to_u64(md.modified_at()),
        }
    }
}

#[allow(non_snake_case)]
#[derive(Serialize)]
pub struct DirEntry {
    pub path: String,
    pub fileName: String,
    pub metadata: Metadata,
}

impl From<&ZboxDirEntry> for DirEntry {
    fn from(ent: &ZboxDirEntry) -> Self {
        DirEntry {
            path: ent.path().to_str().unwrap().to_owned(),
            fileName: ent.file_name().to_owned(),
            metadata: Metadata::from(ent.metadata()),
        }
    }
}

#[allow(non_snake_case)]
#[derive(Serialize)]
pub struct Version {
    pub num: usize,
    pub len: usize,
    pub createdAt: u64,
}

impl From<&ZboxVersion> for Version {
    fn from(ver: &ZboxVersion) -> Self {
        Version {
            num: ver.num(),
            len: ver.len(),
            createdAt: time_to_u64(ver.created_at()),
        }
    }
}

#[wasm_bindgen(js_name = VersionReader)]
pub struct VersionReader {
    inner: Option<ZboxVersionReader>,
}

#[wasm_bindgen(js_class = VersionReader)]
impl VersionReader {
    pub fn close(&mut self) {
        self.inner.take();
    }

    pub fn read(&mut self, dst: &mut [u8]) -> Result<usize> {
        let read = map_js_err!(match self.inner {
            Some(ref mut rdr) => rdr.read(dst).map_err(Error::from),
            None => Err(Error::Closed),
        })?;
        Ok(read)
    }

    #[wasm_bindgen(js_name = readAll)]
    pub fn read_all(&mut self) -> Result<js_sys::Uint8ClampedArray> {
        let mut buf = Vec::new();
        map_js_err!(match self.inner {
            Some(ref mut rdr) => rdr.read_to_end(&mut buf).map_err(Error::from),
            None => Err(Error::Closed),
        })?;
        let array = unsafe { js_sys::Uint8Array::view(&buf) };
        Ok(array.slice(0, array.length()))
    }

    pub fn seek(&mut self, from: u32, offset: i32) -> Result<u32> {
        let new_pos = map_js_err!(match self.inner {
            Some(ref mut rdr) => {
                let pos = match from {
                    0 => SeekFrom::Start(offset as u64),
                    1 => SeekFrom::End(offset as i64),
                    2 => SeekFrom::Current(offset as i64),
                    _ => return map_js_err!(Err(Error::InvalidArgument)),
                };
                rdr.seek(pos).map_err(Error::from)
            }
            None => Err(Error::Closed),
        })?;
        Ok(new_pos as u32)
    }
}

#[wasm_bindgen(js_name = File)]
pub struct File {
    inner: Option<ZboxFile>,
}

#[wasm_bindgen(js_class = File)]
impl File {
    pub fn close(&mut self) {
        self.inner.take();
    }

    pub fn read(&mut self, dst: &mut [u8]) -> Result<usize> {
        let read = map_js_err!(match self.inner {
            Some(ref mut file) => file.read(dst).map_err(Error::from),
            None => Err(Error::Closed),
        })?;
        Ok(read)
    }

    #[wasm_bindgen(js_name = readAll)]
    pub fn read_all(&mut self) -> Result<js_sys::Uint8ClampedArray> {
        let mut buf = Vec::new();
        map_js_err!(match self.inner {
            Some(ref mut file) => {
                file.read_to_end(&mut buf).map_err(Error::from)
            }
            None => Err(Error::Closed),
        })?;
        let array = unsafe { js_sys::Uint8Array::view(&buf) };
        Ok(array.slice(0, array.length()))
    }

    pub fn write(&mut self, buf: &[u8]) -> Result<usize> {
        let written = map_js_err!(match self.inner {
            Some(ref mut file) => file.write(buf).map_err(Error::from),
            None => Err(Error::Closed),
        })?;
        Ok(written)
    }

    pub fn finish(&mut self) -> Result<()> {
        map_js_err!(match self.inner {
            Some(ref mut file) => file.finish(),
            None => Err(Error::Closed),
        })
    }

    #[wasm_bindgen(js_name = writeOnce)]
    pub fn write_once(&mut self, buf: &[u8]) -> Result<()> {
        map_js_err!(match self.inner {
            Some(ref mut file) => file.write_once(buf),
            None => Err(Error::Closed),
        })?;
        Ok(())
    }

    pub fn seek(&mut self, from: u32, offset: i32) -> Result<u32> {
        let new_pos = map_js_err!(match self.inner {
            Some(ref mut file) => {
                let pos = match from {
                    0 => SeekFrom::Start(offset as u64),
                    1 => SeekFrom::End(offset as i64),
                    2 => SeekFrom::Current(offset as i64),
                    _ => return map_js_err!(Err(Error::InvalidArgument)),
                };
                file.seek(pos).map_err(Error::from)
            }
            None => Err(Error::Closed),
        })?;
        Ok(new_pos as u32)
    }

    #[wasm_bindgen(js_name = setLen)]
    pub fn set_len(&mut self, len: usize) -> Result<()> {
        map_js_err!(match self.inner {
            Some(ref mut file) => file.set_len(len),
            None => Err(Error::Closed),
        })
    }

    #[wasm_bindgen(js_name = currVersion)]
    pub fn curr_version(&self) -> Result<usize> {
        map_js_err!(match self.inner {
            Some(ref file) => file.curr_version(),
            None => Err(Error::Closed),
        })
    }

    #[wasm_bindgen(js_name = versionReader)]
    pub fn version_reader(&self, ver_num: usize) -> Result<VersionReader> {
        let rdr = map_js_err!(match self.inner {
            Some(ref file) => file.version_reader(ver_num),
            None => Err(Error::Closed),
        })?;
        Ok(VersionReader { inner: Some(rdr) })
    }

    pub fn metadata(&self) -> Result<JsValue> {
        let md = map_js_err!(match self.inner {
            Some(ref file) => file.metadata(),
            None => Err(Error::Closed),
        })?;
        let ret = Metadata::from(md);
        Ok(JsValue::from_serde(&ret).unwrap())
    }

    pub fn history(&self) -> Result<JsValue> {
        let hist = map_js_err!(match self.inner {
            Some(ref file) => file.history(),
            None => Err(Error::Closed),
        })?;
        let ret: Vec<Version> = hist.iter().map(Version::from).collect();
        Ok(JsValue::from_serde(&ret).unwrap())
    }
}

#[wasm_bindgen]
pub struct Repo {
    inner: Option<ZboxRepo>,
}

#[wasm_bindgen(js_class = Repo)]
impl Repo {
    pub fn close(&mut self) {
        self.inner.take();
    }

    pub fn exists(uri: &str) -> Result<bool> {
        map_js_err!(ZboxRepo::exists(uri))
    }

    pub fn info(&self) -> Result<JsValue> {
        let info = map_js_err!(match self.inner {
            Some(ref repo) => repo.info(),
            None => Err(Error::RepoClosed),
        })?;
        let ret = RepoInfo::from(info);
        Ok(JsValue::from_serde(&ret).unwrap())
    }

    #[wasm_bindgen(js_name = resetPassword)]
    pub fn reset_password(
        &mut self,
        old_pwd: &str,
        new_pwd: &str,
    ) -> Result<()> {
        map_js_err!(match self.inner {
            Some(ref mut repo) => repo.reset_password(
                old_pwd,
                new_pwd,
                OpsLimit::Interactive,
                MemLimit::Interactive
            ),
            None => Err(Error::RepoClosed),
        })
    }

    #[wasm_bindgen(js_name = pathExists)]
    pub fn path_exists(&self, path: &str) -> Result<bool> {
        map_js_err!(match self.inner {
            Some(ref repo) => repo.path_exists(path),
            None => Err(Error::RepoClosed),
        })
    }

    #[wasm_bindgen(js_name = isFile)]
    pub fn is_file(&self, path: &str) -> Result<bool> {
        map_js_err!(match self.inner {
            Some(ref repo) => repo.is_file(path),
            None => Err(Error::RepoClosed),
        })
    }

    #[wasm_bindgen(js_name = isDir)]
    pub fn is_dir(&self, path: &str) -> Result<bool> {
        map_js_err!(match self.inner {
            Some(ref repo) => repo.is_dir(path),
            None => Err(Error::RepoClosed),
        })
    }

    #[wasm_bindgen(js_name = createFile)]
    pub fn create_file(&mut self, path: &str) -> Result<File> {
        let file = map_js_err!(match self.inner {
            Some(ref mut repo) => repo.create_file(path),
            None => Err(Error::RepoClosed),
        })?;
        Ok(File { inner: Some(file) })
    }

    #[wasm_bindgen(js_name = openFile)]
    pub fn open_file(&mut self, path: &str) -> Result<File> {
        let file = map_js_err!(match self.inner {
            Some(ref mut repo) => repo.open_file(path),
            None => Err(Error::RepoClosed),
        })?;
        Ok(File { inner: Some(file) })
    }

    #[wasm_bindgen(js_name = createDir)]
    pub fn create_dir(&mut self, path: &str) -> Result<()> {
        map_js_err!(match self.inner {
            Some(ref mut repo) => repo.create_dir(path),
            None => Err(Error::RepoClosed),
        })
    }

    #[wasm_bindgen(js_name = createDirAll)]
    pub fn create_dir_all(&mut self, path: &str) -> Result<()> {
        map_js_err!(match self.inner {
            Some(ref mut repo) => repo.create_dir_all(path),
            None => Err(Error::RepoClosed),
        })
    }

    #[wasm_bindgen(js_name = readDir)]
    pub fn read_dir(&self, path: &str) -> Result<JsValue> {
        let dirs = map_js_err!(match self.inner {
            Some(ref repo) => repo.read_dir(path),
            None => Err(Error::RepoClosed),
        });
        trace!("read_dir: {:?}", dirs);
        let dirs = dirs?;
        let ret: Vec<DirEntry> = dirs.iter().map(DirEntry::from).collect();
        Ok(JsValue::from_serde(&ret).unwrap())
    }

    pub fn metadata(&self, path: &str) -> Result<JsValue> {
        let md = map_js_err!(match self.inner {
            Some(ref repo) => repo.metadata(path),
            None => Err(Error::RepoClosed),
        })?;
        let ret = Metadata::from(md);
        Ok(JsValue::from_serde(&ret).unwrap())
    }

    pub fn history(&self, path: &str) -> Result<JsValue> {
        let hist = map_js_err!(match self.inner {
            Some(ref repo) => repo.history(path),
            None => Err(Error::RepoClosed),
        })?;
        let ret: Vec<Version> = hist.iter().map(Version::from).collect();
        Ok(JsValue::from_serde(&ret).unwrap())
    }

    pub fn copy(&mut self, from: &str, to: &str) -> Result<()> {
        map_js_err!(match self.inner {
            Some(ref mut repo) => repo.copy(from, to),
            None => Err(Error::RepoClosed),
        })
    }

    #[wasm_bindgen(js_name = removeFile)]
    pub fn remove_file(&mut self, path: &str) -> Result<()> {
        map_js_err!(match self.inner {
            Some(ref mut repo) => repo.remove_file(path),
            None => Err(Error::RepoClosed),
        })
    }

    #[wasm_bindgen(js_name = removeDir)]
    pub fn remove_dir(&mut self, path: &str) -> Result<()> {
        map_js_err!(match self.inner {
            Some(ref mut repo) => repo.remove_dir(path),
            None => Err(Error::RepoClosed),
        })
    }

    #[wasm_bindgen(js_name = removeDirAll)]
    pub fn remove_dir_all(&mut self, path: &str) -> Result<()> {
        map_js_err!(match self.inner {
            Some(ref mut repo) => repo.remove_dir_all(path),
            None => Err(Error::RepoClosed),
        })
    }

    pub fn rename(&mut self, from: &str, to: &str) -> Result<()> {
        map_js_err!(match self.inner {
            Some(ref mut repo) => repo.rename(from, to),
            None => Err(Error::RepoClosed),
        })
    }

    /*pub fn open(&mut self, uri: &str, pwd: &str) -> Result<()> {
        if self.repo.is_some() {
            return map_js_err!(Err(Error::Opened))?;
        }

        let mut opener = RepoOpener::new();
        let repo = map_js_err!(opener.create(true).open(&uri, pwd))?;
        self.repo = Some(repo);
        Ok(())

        let buf = [1u8, 2u8, 3u8];

        // create and write a file
        let mut f = OpenOptions::new()
            .create(true)
            .open(&mut repo, "/file")
            .unwrap();
        f.write_once(&buf[..]).unwrap();

        // read file
        let mut dst = Vec::new();
        let ver_num = f.history().unwrap().last().unwrap().num();
        let mut rdr = f.version_reader(ver_num).unwrap();
        let result = rdr.read_to_end(&mut dst).unwrap();
        assert_eq!(result, buf.len());
        assert_eq!(&dst[..], &buf[..]);
    }*/

    /*
    pub fn put() -> JsValue {
        debug!("call put()");
        let xhr = web_sys::XmlHttpRequest::new().unwrap();
        xhr.open_with_async("PUT", "https://httpbin.org/put", false).unwrap();
        xhr.set_timeout(3000);
        xhr.set_response_type(web_sys::XmlHttpRequestResponseType::Arraybuffer);

        // put binary body
        let buf = vec![1, 2, 3];
        let buf = unsafe { js_sys::Uint8Array::view(&buf)  };
        xhr.send_with_opt_buffer_source(Some(&buf)).unwrap();

        return JsValue::NULL;
    }

    pub fn request() -> JsValue {
        console_log!("[rust] call request3()");
        let xhr = web_sys::XmlHttpRequest::new().unwrap();
        xhr.open_with_async("GET", "https://httpbin.org/ip", false).unwrap();
        //xhr.open_with_async("GET", "http://misbehaving.site/timeout", false).unwrap();
        //xhr.open_with_async("GET", "http://notexistswebsite.site", false).unwrap();
        xhr.set_timeout(3000);
        xhr.set_response_type(web_sys::XmlHttpRequestResponseType::Arraybuffer);
        xhr.send().unwrap();
        console_log!("[rust] request sent, ready state: {}, status: {}",
                     xhr.ready_state(), xhr.status().unwrap());

        // get headers
        let mut headers = HeaderMap::new();
        let headers_str = xhr.get_all_response_headers().unwrap();
        debug!("{}", headers_str);
        headers_str.trim_end()
            .split("\r\n")
            .for_each(|ent| {
                let ent: Vec<&str> = ent.split(": ").collect();
                let name = HeaderName::from_lowercase(ent[0].as_bytes()).unwrap();
                let value = HeaderValue::from_str(ent[1]).unwrap();
                headers.insert(name, value);
            });
        debug!("{:?}", headers);

        // get binary body
        let resp = xhr.response().unwrap();
        let bin = js_sys::Uint8Array::new_with_byte_offset(&resp, 0);
        debug!("bin: {:?}, len: {:?}", bin, bin.byte_length());
        let mut buf = vec![0u8; bin.byte_length() as usize];
        bin.copy_to(&mut buf);
        debug!("body len: {:?}", buf.len());

        return JsValue::NULL;

        /*let body = xhr.response_text().unwrap().unwrap();
        console_log!("[rust] body: {:?}", body);
        if body == "" {
            return JsValue::NULL;
        }
        let resp: Resp = serde_json::from_str(&body).unwrap();
        console_log!("[rust] call request3, {:?}", resp);
        let ret = JsValue::from_serde(&resp).unwrap();

        ret*/
    }*/
}
