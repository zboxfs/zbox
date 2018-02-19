use std::collections::HashMap;
use std::collections::hash_map::Entry;

use error::Result;
use base::RefCnt;
use base::crypto::Hash;
use trans::Eid;

/// Content map entry
#[derive(Debug, Clone, Deserialize, Serialize)]
pub(super) struct ContentMapEntry {
    content_id: Eid,
    refcnt: RefCnt,
}

impl ContentMapEntry {
    pub fn new(content_id: &Eid) -> Self {
        ContentMapEntry {
            content_id: content_id.clone(),
            refcnt: RefCnt::new(),
        }
    }

    #[inline]
    pub fn content_id(&self) -> &Eid {
        &self.content_id
    }

    #[inline]
    pub fn inc_ref(&mut self) -> Result<u32> {
        self.refcnt.inc_ref()
    }

    #[inline]
    pub fn dec_ref(&mut self) -> Result<u32> {
        self.refcnt.dec_ref()
    }
}

/// Content map
#[derive(Debug, Default, Clone, Deserialize, Serialize)]
pub(super) struct ContentMap {
    map: HashMap<Hash, ContentMapEntry>,
}

impl ContentMap {
    pub fn new() -> Self {
        ContentMap {
            map: HashMap::new(),
        }
    }

    #[inline]
    pub fn get_mut(&mut self, k: &Hash) -> Option<&mut ContentMapEntry> {
        self.map.get_mut(k)
    }

    #[inline]
    pub fn entry(&mut self, key: Hash) -> Entry<Hash, ContentMapEntry> {
        self.map.entry(key)
    }

    #[inline]
    pub fn remove(&mut self, k: &Hash) -> Option<ContentMapEntry> {
        self.map.remove(k)
    }
}
