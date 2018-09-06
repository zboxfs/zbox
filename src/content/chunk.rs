use std::fmt::{self, Debug};

use linked_hash_map::LinkedHashMap;

use base::crypto::Hash;
use base::RefCnt;
use error::Result;
use trans::Eid;

/// Data chunk
#[derive(Clone, Deserialize, Serialize)]
pub struct Chunk {
    pub(super) pos: usize, // chunk start position in segment data
    pub(super) len: usize, // chunk length, in bytes
    refcnt: RefCnt,
}

impl Chunk {
    pub fn new(pos: usize, len: usize) -> Self {
        Chunk {
            pos,
            len,
            refcnt: RefCnt::new(),
        }
    }

    #[inline]
    pub fn inc_ref(&mut self) -> Result<u32> {
        self.refcnt.inc_ref()
    }

    #[inline]
    pub fn dec_ref(&mut self) -> Result<u32> {
        self.refcnt.dec_ref()
    }

    #[inline]
    pub fn end_pos(&self) -> usize {
        self.pos + self.len
    }

    #[inline]
    pub fn is_orphan(&self) -> bool {
        self.refcnt.val() == 0
    }
}

impl Debug for Chunk {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "Chunk(pos: {}, len: {}, refcnt: {})",
            self.pos,
            self.len,
            self.refcnt.val()
        )
    }
}

/// Chunk location
#[derive(Debug, Hash, Eq, PartialEq, Deserialize, Serialize)]
pub struct ChunkLoc {
    pub(super) seg_id: Eid,
    pub(super) idx: usize, // index in segment chunk list
}

#[derive(Default, Clone, Deserialize, Serialize)]
struct ChunkIdx {
    seg_idx: usize, // index in segment id list
    chk_idx: usize, // index in segment chunk list
}

/// Chunk map, used for chunk dedup in a file
#[derive(Default, Clone, Deserialize, Serialize)]
pub struct ChunkMap {
    seg_ids: Vec<Eid>, // segment id array

    // key: chunk hash
    // val: (index in segment id array, index in segment chunk list)
    map: LinkedHashMap<Hash, ChunkIdx>,

    is_enabled: bool,
}

impl ChunkMap {
    // max number of index in the map
    const INDEX_MAP_CAPACITY: usize = 256;

    pub fn new(is_enabled: bool) -> Self {
        ChunkMap {
            seg_ids: Vec::new(),
            map: LinkedHashMap::with_capacity(Self::INDEX_MAP_CAPACITY),
            is_enabled,
        }
    }

    pub fn get_refresh(&mut self, hash: &Hash) -> Option<ChunkLoc> {
        if !self.is_enabled {
            return None;
        }
        let seg_ids = &self.seg_ids;
        self.map.get_refresh(hash).map(|ci| ChunkLoc {
            seg_id: seg_ids[ci.seg_idx as usize].clone(),
            idx: ci.chk_idx,
        })
    }

    pub fn insert(&mut self, chk_hash: &Hash, seg_id: &Eid, chk_idx: usize) {
        if !self.is_enabled {
            return;
        }
        let idx = self
            .seg_ids
            .iter()
            .position(|s| s == seg_id)
            .unwrap_or_else(|| {
                self.seg_ids.push(seg_id.clone());
                self.seg_ids.len() - 1
            });
        self.map.insert(
            chk_hash.clone(),
            ChunkIdx {
                seg_idx: idx,
                chk_idx,
            },
        );
        if self.map.len() >= Self::INDEX_MAP_CAPACITY {
            self.map.pop_front();
        }
    }

    fn retain<F>(&mut self, mut f: F)
    where
        F: FnMut(&Hash, &ChunkIdx) -> bool,
    {
        for ent in self.map.entries() {
            if !f(ent.key(), ent.get()) {
                ent.remove();
            }
        }
    }

    pub fn remove_chunks(&mut self, seg_id: &Eid, chk_indices: &[usize]) {
        if !self.is_enabled {
            return;
        }
        self.seg_ids.iter().position(|s| s == seg_id).and_then(
            |seg_idx| -> Option<()> {
                self.retain(|_, val| {
                    val.seg_idx != seg_idx
                        || !chk_indices.contains(&val.chk_idx)
                });
                None
            },
        );
    }

    pub fn remove_segment(&mut self, seg_id: &Eid) {
        if !self.is_enabled {
            return;
        }
        self.seg_ids.iter().position(|s| s == seg_id).and_then(
            |seg_idx| -> Option<()> {
                self.retain(|_, val| val.seg_idx != seg_idx);
                None
            },
        );
    }
}

impl Debug for ChunkMap {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("ChunkMap")
            .field("seg_ids", &self.seg_ids)
            .field("map_len", &self.map.len())
            .field("is_enabled", &self.is_enabled)
            .finish()
    }
}
