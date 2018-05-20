use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use std::io::Result as IoResult;

use base::IntoRef;
use base::lru::Lru;
use super::http_client::{HttpClient, HttpClientRef};

#[derive(Debug)]
pub struct Estore {
    map: HashMap<u8, Vec<u8>>,
    client: HttpClientRef,
}

impl Estore {
    pub fn new(client: &HttpClientRef) -> Self {
        Estore {
            map: HashMap::new(),
            client: client.clone(),
        }
    }

    pub fn get(
        &mut self,
        buf: &mut [u8],
        key: &str,
        offset: u64,
    ) -> IoResult<usize> {
        Ok(0)
    }

    pub fn put(&mut self, key: &str, buf: &[u8]) -> IoResult<()> {
        Ok(())
    }
}

impl IntoRef for Estore {}

pub type EstoreRef = Arc<RwLock<Estore>>;
