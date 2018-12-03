#[cfg(feature = "storage-zbox-native")]
pub(super) mod native;

#[cfg(feature = "storage-zbox-jni")]
pub(super) mod jni;

use std::io::{copy, Read, Write};

use http::{HeaderMap, Response as HttpResponse, Uri};
use serde::de::DeserializeOwned;

use error::{Error, Result};

/// Http response wrapper
pub struct Response {
    pub inner: HttpResponse<Box<Read>>,
}

impl Response {
    #[inline]
    pub fn new(inner: HttpResponse<Box<Read>>) -> Self {
        Response { inner }
    }

    #[inline]
    pub fn error_for_status(self) -> Result<Self> {
        let status = self.inner.status();
        if !status.is_success() {
            return Err(Error::HttpStatus(status));
        }
        Ok(self)
    }

    pub fn to_json<T: DeserializeOwned>(&mut self) -> Result<T> {
        let body = self.inner.body_mut();
        let mut buf = Vec::new();
        body.read_to_end(&mut buf)?;
        serde_json::from_slice(&buf).map_err(Error::from)
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
        &self,
        uri: &Uri,
        headers: &HeaderMap,
        body: &[u8],
    ) -> Result<Response>;

    // HTTP DELETE request
    fn delete(&self, url: &Uri, headers: &HeaderMap) -> Result<Response>;
}

/// Dummy transport
pub struct DummyTransport;

impl Transport for DummyTransport {
    fn get(&self, _uri: &Uri, _headers: &HeaderMap) -> Result<Response> {
        unimplemented!()
    }

    fn put(
        &self,
        _uri: &Uri,
        _headers: &HeaderMap,
        _body: &[u8],
    ) -> Result<Response> {
        unimplemented!()
    }

    fn delete(&self, _url: &Uri, _headers: &HeaderMap) -> Result<Response> {
        unimplemented!()
    }
}
