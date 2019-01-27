use std::collections::HashMap;
use std::io::{Cursor, Read};
use std::sync::Mutex;

use http::header::HeaderName;
use http::status::StatusCode;
use http::{HeaderMap, Response as HttpResponse, Uri};

use super::{Response, Transport};
use error::Result;

lazy_static! {
    // static store
    static ref STORE: Mutex<StaticStore> = {
        Mutex::new(StaticStore::default())
    };
}

// convert reqwest response to response
fn create_response(status: StatusCode, body: Vec<u8>) -> Result<Response> {
    let mut builder = HttpResponse::builder();
    builder.status(status);
    let body = Cursor::new(body);
    let ret = Response::new(builder.body(Box::new(body) as Box<Read>)?);
    Ok(ret)
}

#[inline]
fn create_ok_response() -> Result<Response> {
    create_response(StatusCode::OK, Vec::new())
}

#[derive(Default)]
struct StaticStore {
    map: HashMap<Uri, Vec<u8>>,
    update_seq: usize,
    is_opened: bool,
    is_updated: bool,
}

impl StaticStore {
    fn update(&mut self) {
        if !self.is_updated {
            self.update_seq += 1;
        }
    }
}

pub struct FaultyTransport;

impl FaultyTransport {
    pub fn new(_timeout: u32) -> Result<Self> {
        Ok(FaultyTransport {})
    }
}

impl Transport for FaultyTransport {
    fn get(&self, uri: &Uri, _headers: &HeaderMap) -> Result<Response> {
        let mut store = STORE.lock().unwrap();

        if uri.path().ends_with("/open") {
            if store.is_opened {
                return create_response(StatusCode::CONFLICT, Vec::new());
            }
            let body = format!(
                r#"{{
                "status":"OK",
                "sessionToken":"de4d75fc786669c61c51e5d1d01998215a4157",
                "updateSeq":{},
                "ttl":1544269210
            }}"#,
                store.update_seq
            );
            store.is_opened = true;
            return create_response(StatusCode::OK, body.into_bytes());
        }

        if uri.path().ends_with("/close") {
            store.is_opened = false;
            return create_ok_response();
        }

        if uri.path().ends_with("/exists") {
            let body = if store.update_seq == 0 {
                String::from(r#"{"result":false}"#)
            } else {
                String::from(r#"{"result":true}"#)
            };
            return create_response(StatusCode::OK, body.into_bytes());
        }

        match store.map.get(uri) {
            //Some(body) => create_response(StatusCode::OK, body.clone()),
            Some(body) => create_response(StatusCode::OK, body.clone()),
            None => create_response(StatusCode::NOT_FOUND, Vec::new()),
        }
    }

    fn put(
        &mut self,
        uri: &Uri,
        headers: &HeaderMap,
        body: &[u8],
    ) -> Result<Response> {
        let mut store = STORE.lock().unwrap();

        let header = HeaderName::from_static("zbox-offset");
        let offset = headers.get(&header).unwrap();
        let offset: usize = offset.to_str().unwrap().parse().unwrap();
        store
            .map
            .entry(uri.to_owned())
            .and_modify(|val| {
                val.truncate(offset);
                val.extend_from_slice(body);
            }).or_insert(body.to_owned());

        store.update();
        create_ok_response()
    }

    fn delete(&mut self, uri: &Uri, _headers: &HeaderMap) -> Result<Response> {
        let mut store = STORE.lock().unwrap();
        store.map.remove(uri);
        store.update();
        create_ok_response()
    }
}
