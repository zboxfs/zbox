use http::{HeaderMap, Response as HttpResponse, Uri};
use std::io::Read;
use std::time::Duration;

use reqwest::{Client, Response as NativeResponse};

use super::{Response, Transport};
use error::Result;

// convert reqwest response to response
fn into_response(resp: NativeResponse) -> Result<Response> {
    let mut builder = HttpResponse::builder();
    builder.status(resp.status()).version(resp.version());
    for (name, value) in resp.headers() {
        builder.header(name, value);
    }
    let ret = Response::new(builder.body(Box::new(resp) as Box<Read>)?);
    Ok(ret)
}

// transport using native http layer
pub struct NativeTransport {
    client: Client,
}

impl NativeTransport {
    pub fn new(timeout: u64) -> Result<Self> {
        let client = Client::builder()
            .timeout(Duration::from_secs(timeout))
            .build()?;

        Ok(NativeTransport { client })
    }
}

impl Transport for NativeTransport {
    fn get(&self, uri: &Uri, headers: &HeaderMap) -> Result<Response> {
        let resp = self
            .client
            .get(&uri.to_string())
            .headers(headers.clone())
            .send()?;
        into_response(resp)
    }

    fn put(
        &self,
        uri: &Uri,
        headers: &HeaderMap,
        body: &[u8],
    ) -> Result<Response> {
        let resp = self
            .client
            .put(&uri.to_string())
            .headers(headers.clone())
            .body(body.to_owned())
            .send()?;
        into_response(resp)
    }

    fn delete(&self, uri: &Uri, headers: &HeaderMap) -> Result<Response> {
        let resp = self
            .client
            .delete(&uri.to_string())
            .headers(headers.clone())
            .send()?;
        into_response(resp)
    }
}
