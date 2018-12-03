use http::{HeaderMap, Response as HttpResponse, Uri};
use std::io::{Read, Result as IoResult};
use std::time::Duration;

use super::{Response, Transport};
use error::Result;

struct Reader {}

impl Reader {
    fn new() -> Self {
        Reader {}
    }
}

impl Read for Reader {
    fn read(&mut self, buf: &mut [u8]) -> IoResult<usize> {
        Ok(0)
    }
}

// transport using jni http layer
pub struct JniTransport {}

impl JniTransport {
    pub fn new(timeout: u64) -> Result<Self> {
        Ok(JniTransport {})
    }
}

impl Transport for JniTransport {
    fn get(&self, uri: &Uri, headers: &HeaderMap) -> Result<Response> {
        let mut builder = HttpResponse::builder();
        let rdr = Reader::new();
        let ret = Response::new(builder.body(Box::new(rdr) as Box<Read>)?);
        Ok(ret)
    }

    fn put(
        &self,
        uri: &Uri,
        headers: &HeaderMap,
        body: &[u8],
    ) -> Result<Response> {
        let mut builder = HttpResponse::builder();
        let rdr = Reader::new();
        let ret = Response::new(builder.body(Box::new(rdr) as Box<Read>)?);
        Ok(ret)
    }

    fn delete(&self, uri: &Uri, headers: &HeaderMap) -> Result<Response> {
        let mut builder = HttpResponse::builder();
        let rdr = Reader::new();
        let ret = Response::new(builder.body(Box::new(rdr) as Box<Read>)?);
        Ok(ret)
    }
}
