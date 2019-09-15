#[cfg(feature = "storage-zbox-faulty")]
pub(super) mod faulty;

#[cfg(feature = "storage-zbox-native")]
pub(super) mod native;

#[cfg(feature = "storage-zbox-android")]
pub(super) mod jni;

#[cfg(target_arch = "wasm32")]
pub(super) mod wasm;

use std::io::{copy, Read, Write};

use http::{HeaderMap, Response as HttpResponse, StatusCode, Uri};
use serde::de::DeserializeOwned;
use serde_json::from_slice;

use error::{Error, Result};

/// Http response wrapper
pub struct Response {
    pub inner: HttpResponse<Box<dyn Read>>,
}

impl Response {
    #[inline]
    pub fn new(inner: HttpResponse<Box<dyn Read>>) -> Self {
        Response { inner }
    }

    pub fn error_for_status(self) -> Result<Self> {
        let status = self.inner.status();

        // 409 conflict error means remote session is already opened
        if status == StatusCode::CONFLICT {
            return Err(Error::Opened);
        }

        if !status.is_success() {
            return Err(Error::HttpStatus(status));
        }

        Ok(self)
    }

    pub fn as_json<T: DeserializeOwned>(&mut self) -> Result<T> {
        let body = self.inner.body_mut();
        let mut buf = Vec::new();
        body.read_to_end(&mut buf)?;
        from_slice(&buf).map_err(Error::from)
    }

    #[inline]
    pub fn copy_to<W: Write + ?Sized>(&mut self, w: &mut W) -> Result<u64> {
        copy(self.inner.body_mut(), w).map_err(Error::from)
    }
}

/// Transport trait
pub trait Transport: Send + Sync {
    // HTTP GET request
    fn get(&self, uri: &Uri, headers: &HeaderMap) -> Result<Response>;

    // HTTP PUT request
    fn put(
        &mut self,
        uri: &Uri,
        headers: &HeaderMap,
        body: &[u8],
    ) -> Result<Response>;

    // HTTP DELETE request
    fn delete(&mut self, uri: &Uri, headers: &HeaderMap) -> Result<Response>;

    // HTTP bulk DELETE request
    fn delete_bulk(
        &mut self,
        uri: &Uri,
        headers: &HeaderMap,
        body: &[u8],
    ) -> Result<Response>;
}

/// Dummy transport
pub struct DummyTransport;

impl Transport for DummyTransport {
    #[inline]
    fn get(&self, _uri: &Uri, _headers: &HeaderMap) -> Result<Response> {
        unimplemented!()
    }

    #[inline]
    fn put(
        &mut self,
        _uri: &Uri,
        _headers: &HeaderMap,
        _body: &[u8],
    ) -> Result<Response> {
        unimplemented!()
    }

    #[inline]
    fn delete(&mut self, _uri: &Uri, _headers: &HeaderMap) -> Result<Response> {
        unimplemented!()
    }

    #[inline]
    fn delete_bulk(
        &mut self,
        _uri: &Uri,
        _headers: &HeaderMap,
        _body: &[u8],
    ) -> Result<Response> {
        unimplemented!()
    }
}
