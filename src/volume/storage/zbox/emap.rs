use std::collections::HashMap;

use trans::{Eid, Txid};

#[derive(Debug)]
struct Space {
    txid: Txid,
}

#[derive(Debug)]
struct Bucket {
    map: HashMap<Eid, Space>,
}

#[derive(Debug)]
pub struct Emap {
    buckets: HashMap<u8, Bucket>,
}

impl Emap {
    pub fn new() -> Self {
        Emap {
            buckets: HashMap::new(),
        }
    }
}
