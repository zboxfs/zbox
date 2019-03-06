use std::path::Path;

use js_sys::Uint8Array;
use wasm_bindgen::prelude::*;
use wasm_bindgen::JsValue;

use super::CacheBackend;
use error::{Error, Result};

#[wasm_bindgen(module = "../cache_backend")]
extern "C" {
    fn contains(rel_path: &str) -> bool;
    fn get(rel_path: &str) -> JsValue;
    fn insert(rel_path: &str, data: Uint8Array);
    fn remove(rel_path: &str);
    fn clear();
}

pub struct WasmBackend {}

impl WasmBackend {
    #[inline]
    pub fn new() -> Self {
        WasmBackend {}
    }
}

impl CacheBackend for WasmBackend {
    #[inline]
    fn contains(&mut self, rel_path: &Path) -> bool {
        contains(rel_path.to_str().unwrap())
    }

    fn get_exact(
        &mut self,
        rel_path: &Path,
        offset: usize,
        dst: &mut [u8],
    ) -> Result<()> {
        let js_val = get(rel_path.to_str().unwrap());
        if js_val.is_undefined() {
            return Err(Error::NotFound);
        }
        let js_buf = Uint8Array::from(js_val);
        let sub_buf =
            js_buf.subarray(offset as u32, (offset + dst.len()) as u32);
        sub_buf.copy_to(dst);
        Ok(())
    }

    #[inline]
    fn get(&mut self, rel_path: &Path) -> Result<Vec<u8>> {
        let js_val = get(rel_path.to_str().unwrap());
        if js_val.is_undefined() {
            return Err(Error::NotFound);
        }
        let js_buf = Uint8Array::from(js_val);
        let mut ret = vec![0u8; js_buf.length() as usize];
        js_buf.copy_to(&mut ret);
        Ok(ret)
    }

    #[inline]
    fn insert(&mut self, rel_path: &Path, obj: &[u8]) -> Result<()> {
        unsafe {
            insert(rel_path.to_str().unwrap(), Uint8Array::view(obj));
        }
        Ok(())
    }

    #[inline]
    fn remove(&mut self, rel_path: &Path) -> Result<()> {
        remove(rel_path.to_str().unwrap());
        Ok(())
    }

    #[inline]
    fn clear(&mut self) -> Result<()> {
        clear();
        Ok(())
    }
}
