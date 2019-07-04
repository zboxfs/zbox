use std::collections::HashMap;
use std::io::{Cursor, Read};
use std::path::PathBuf;
use std::sync::Mutex;

use http::header::HeaderName;
use http::status::StatusCode;
use http::{HeaderMap, Response as HttpResponse, Uri};

use super::{Response, Transport};
use error::Result;
use volume::storage::faulty_ctl::Controller;

lazy_static! {
    // static store
    static ref STORE: Mutex<StaticStore> = {
        Mutex::new(StaticStore::default())
    };
}

fn create_response(status: StatusCode, body: Vec<u8>) -> Result<Response> {
    let mut builder = HttpResponse::builder();
    builder.status(status);
    let body = Cursor::new(body);
    let ret = Response::new(builder.body(Box::new(body) as Box<dyn Read>)?);
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
    #[inline]
    fn update(&mut self) {
        if !self.is_updated {
            self.update_seq += 1;
            self.is_updated = true;
        }
    }
}

pub struct FaultyTransport {
    ctlr: Controller,
}

impl FaultyTransport {
    pub fn new(_timeout: u32) -> Result<Self> {
        Ok(FaultyTransport {
            ctlr: Controller::new(),
        })
    }
}

impl Transport for FaultyTransport {
    fn get(&self, uri: &Uri, _headers: &HeaderMap) -> Result<Response> {
        self.ctlr.make_random_error()?;

        let mut store = STORE.lock().unwrap();

        if uri.path().ends_with("/open") {
            if store.is_opened {
                //return create_response(StatusCode::CONFLICT, Vec::new());
            }

            // fixed response body
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
            store.is_updated = false;
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
        self.ctlr.make_random_error()?;

        let mut store = STORE.lock().unwrap();
        let header = HeaderName::from_static("zbox-range");
        let range = headers.get(&header).unwrap();
        let range = range.to_str().unwrap();
        let idx = range.find('-').unwrap();
        let begin: usize = range[..idx].parse().unwrap();
        let end: usize = range[idx + 1..].parse().unwrap();
        assert_eq!(end - begin + 1, body.len());

        store
            .map
            .entry(uri.to_owned())
            .and_modify(|val| {
                // set new length for the value, fill gap with constant 42
                val.resize(begin, 42);
                val.extend_from_slice(body);
            })
            .or_insert_with(|| {
                if begin == 0 {
                    body.to_owned()
                } else {
                    let mut buf = vec![42u8; begin]; // gap buffer
                    buf.extend_from_slice(body);
                    buf
                }
            });

        store.update();
        create_ok_response()
    }

    fn delete(&mut self, uri: &Uri, _headers: &HeaderMap) -> Result<Response> {
        self.ctlr.make_random_error()?;

        let mut store = STORE.lock().unwrap();
        store.map.remove(uri);
        store.update();
        create_ok_response()
    }

    fn delete_bulk(
        &mut self,
        uri: &Uri,
        _headers: &HeaderMap,
        body: &[u8],
    ) -> Result<Response> {
        self.ctlr.make_random_error()?;

        let base = uri.to_string();
        let idx = base.find("bulk").unwrap();
        let base = &base[..idx].to_string();

        let mut store = STORE.lock().unwrap();
        let map: HashMap<String, Vec<PathBuf>> =
            serde_json::from_slice(body).unwrap();
        for list in map.values() {
            for uri in list {
                let url = base.to_owned() + uri.to_str().unwrap();
                let url = url.parse::<Uri>().unwrap();
                store.map.remove(&url);
            }
        }
        store.update();
        create_ok_response()
    }
}
