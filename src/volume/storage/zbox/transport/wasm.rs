use http::header::{HeaderMap, HeaderName, HeaderValue};
use http::status::StatusCode;
use http::{Response as HttpResponse, Uri};
use std::io::{Cursor, Read};

use js_sys::Uint8Array;
use web_sys::{XmlHttpRequest, XmlHttpRequestResponseType};

use super::{Response, Transport};
use error::{Error, Result};

// XMLHttpRequest ready state: DONE
const READY_STATE_DONE: u16 = 4;

// map error to request error
macro_rules! map_req_err {
    ($x:expr) => {
        $x.map_err(|_| Error::RequestError);
    };
}

// get response from XHR
fn create_response(xhr: XmlHttpRequest) -> Result<Response> {
    // check response status
    let ready_state = xhr.ready_state();
    let status = map_req_err!(xhr.status())?;
    if ready_state != READY_STATE_DONE {
        return Err(Error::RequestError);
    }

    let mut builder = HttpResponse::builder();

    // extract response status
    let status_code = map_req_err!(StatusCode::from_u16(status))?;
    builder.status(status_code);

    // extract response headers
    let headers_str = map_req_err!(xhr.get_all_response_headers())?;
    if !headers_str.is_empty() {
        headers_str.trim_end().split("\r\n").for_each(|ent| {
            let ent: Vec<&str> = ent.split(": ").collect();
            let name = HeaderName::from_lowercase(ent[0].as_bytes()).unwrap();
            let value = HeaderValue::from_str(ent[1]).unwrap();
            builder.header(name, value);
        });
    }

    // extract response body as binary data
    let resp = map_req_err!(xhr.response())?;
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
        Ok(WasmTransport {
            timeout: timeout * 1000,
        })
    }

    fn create_xhr(
        &self,
        method: &str,
        uri: &Uri,
        headers: &HeaderMap,
    ) -> XmlHttpRequest {
        let xhr = XmlHttpRequest::new().unwrap();
        xhr.open_with_async(method, &uri.to_string(), false)
            .unwrap();
        xhr.set_timeout(self.timeout);
        xhr.set_with_credentials(true);
        xhr.set_response_type(XmlHttpRequestResponseType::Arraybuffer);
        for (name, value) in headers.iter() {
            xhr.set_request_header(name.as_str(), value.to_str().unwrap())
                .unwrap();
        }
        xhr
    }
}

impl Transport for WasmTransport {
    fn get(&self, uri: &Uri, headers: &HeaderMap) -> Result<Response> {
        let xhr = self.create_xhr("GET", uri, headers);
        map_req_err!(xhr.send())?;
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
        map_req_err!(xhr.send_with_opt_buffer_source(Some(&buf)))?;
        create_response(xhr)
    }

    fn delete(&mut self, uri: &Uri, headers: &HeaderMap) -> Result<Response> {
        let xhr = self.create_xhr("DELETE", uri, headers);
        map_req_err!(xhr.send())?;
        create_response(xhr)
    }

    fn delete_bulk(
        &mut self,
        uri: &Uri,
        headers: &HeaderMap,
        body: &[u8],
    ) -> Result<Response> {
        let xhr = self.create_xhr("DELETE", uri, headers);
        let buf = unsafe { Uint8Array::view(body) };
        map_req_err!(xhr.send_with_opt_buffer_source(Some(&buf)))?;
        create_response(xhr)
    }
}
