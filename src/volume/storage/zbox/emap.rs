use std::collections::HashMap;
use std::collections::hash_map::Entry;

use base::crypto::{Crypto, Key};
use trans::{Eid, Txid};
use volume::storage::space::Space;

const BUCKET_CNT: usize = 4;
const BUCKET_MASK: usize = BUCKET_CNT - 1;

#[inline]
fn bucket_id(id: &Eid) -> usize {
    id[0] as usize & BUCKET_MASK
}

type Bucket = HashMap<Eid, Space>;

#[derive(Debug)]
pub struct Emap {
    buckets: Vec<Bucket>,
    skey: Key,
    crypto: Crypto,
}

impl Emap {
    pub fn new(_txid: Txid) -> Self {
        Emap {
            buckets: Vec::new(),
            skey: Key::new_empty(),
            crypto: Crypto::default(),
        }
    }

    pub fn set_crypto_key(&mut self, crypto: &Crypto, skey: &Key) {
        self.crypto = crypto.clone();
        self.skey = skey.clone();
    }

    #[inline]
    pub fn get(&self, id: &Eid) -> Option<&Space> {
        self.buckets[bucket_id(id)].get(id)
    }

    #[inline]
    pub fn entry(&mut self, id: Eid) -> Entry<Eid, Space> {
        self.buckets[bucket_id(&id)].entry(id)
    }

    #[inline]
    pub fn remove(&mut self, id: &Eid) -> Option<Space> {
        self.buckets[bucket_id(id)].remove(id)
    }

    #[inline]
    pub fn clear(&mut self) {
        // TODO
    }
}
