use http::{Response as HttpResponse, Uri};
use http::header::{
    HeaderMap, HeaderName, HeaderValue
};
use http::status::StatusCode;
use std::io::{Cursor, Read};

use web_sys::{XmlHttpRequest, XmlHttpRequestResponseType};
use js_sys::Uint8Array;

use super::{Response, Transport};
use error::{Error, Result};

// XMLHttpRequest ready state: DONE
const READY_STATE_DONE: u16 = 4;

// get response from XHR
fn create_response(xhr: XmlHttpRequest) -> Result<Response> {
    // check response status
    let ready_state = xhr.ready_state();
    let status = xhr.status().unwrap();
    if ready_state != READY_STATE_DONE || (status != 200 && status != 204 && status != 404) {
        return Err(Error::RequestError);
    }

    let mut builder = HttpResponse::builder();

    // extract response status
    builder.status(StatusCode::from_u16(status).unwrap());

    // extract response headers
    let headers_str = xhr.get_all_response_headers().unwrap();
    if !headers_str.is_empty() {
        headers_str.trim_end()
            .split("\r\n")
            .for_each(|ent| {
                let ent: Vec<&str> = ent.split(": ").collect();
                let name = HeaderName::from_lowercase(ent[0].as_bytes()).unwrap();
                let value = HeaderValue::from_str(ent[1]).unwrap();
                builder.header(name, value);
            });
    }

    // extract response body as binary data
    let resp = xhr.response().unwrap();
    let bin = Uint8Array::new_with_byte_offset(&resp, 0);
    let mut buf = vec![0u8; bin.byte_length() as usize];
    bin.copy_to(&mut buf);
    let body = Box::new(Cursor::new(buf)) as Box<Read>;
    let ret = Response::new(builder.body(body)?);
    Ok(ret)
}

// transport using wasm http layer
pub struct WasmTransport {
    timeout: u32,
}

impl WasmTransport {
    pub fn new(timeout: u32) -> Result<Self> {
        Ok(WasmTransport { timeout: timeout * 1000 })
    }

    fn create_xhr(&self, method: &str, uri: &Uri, headers: &HeaderMap) -> XmlHttpRequest {
        let xhr = XmlHttpRequest::new().unwrap();
        xhr.open_with_async(method, &uri.to_string(), false).unwrap();
        xhr.set_timeout(self.timeout);
        xhr.set_with_credentials(true);
        xhr.set_response_type(XmlHttpRequestResponseType::Arraybuffer);
        for (name, value) in headers.iter() {
            xhr.set_request_header(name.as_str(), value.to_str().unwrap()).unwrap();
        }
        xhr
    }
}

impl Transport for WasmTransport {
    fn get(&self, uri: &Uri, headers: &HeaderMap) -> Result<Response> {
        let xhr = self.create_xhr("GET", uri, headers);
        xhr.send().unwrap();
        create_response(xhr)
    }

    fn put(
        &mut self,
        uri: &Uri,
        headers: &HeaderMap,
        body: &[u8],
    ) -> Result<Response> {
        let xhr = self.create_xhr("PUT", uri, headers);
        let buf = unsafe { Uint8Array::view(body) };
        xhr.send_with_opt_buffer_source(Some(&buf)).unwrap();
        create_response(xhr)
    }

    fn delete(&mut self, uri: &Uri, headers: &HeaderMap) -> Result<Response> {
        let xhr = self.create_xhr("DELETE", uri, headers);
        xhr.send().unwrap();
        create_response(xhr)
    }
}

