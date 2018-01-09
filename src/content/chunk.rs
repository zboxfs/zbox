use std::sync::{Arc, RwLock};
use std::fmt::{self, Debug};
use std::collections::HashMap;
use std::result::Result as StdResult;

use serde::{Deserialize, Serialize};
use serde::ser::Serializer;
use serde::de::Deserializer;

use error::Result;
use base::RefCnt;
use base::crypto::Hash;
use trans::Eid;

/// Data chunk
#[derive(Clone, Deserialize, Serialize)]
pub struct Chunk {
    pub(super) pos: usize, // chunk start position in segment
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

#[derive(Debug, Default, Clone, Deserialize, Serialize)]
struct ChunkMapInner {
    seg_ids: Vec<Eid>, // segment id array

    // key: chunk hash
    // val: (index in segment id array, index in segment chunk list)
    map: HashMap<Hash, (usize, usize)>,
}

impl ChunkMapInner {
    fn new() -> Self {
        ChunkMapInner {
            seg_ids: Vec::new(),
            map: HashMap::new(),
        }
    }

    fn get(&self, hash: &Hash) -> Option<ChunkLoc> {
        self.map.get(hash).map(|&(seg_idx, chk_idx)| {
            ChunkLoc {
                seg_id: self.seg_ids[seg_idx as usize].clone(),
                idx: chk_idx,
            }
        })
    }

    fn insert(&mut self, chk_hash: &Hash, seg_id: &Eid, chk_idx: usize) {
        let idx = self.seg_ids
            .iter()
            .position(|s| s == seg_id)
            .unwrap_or_else(|| {
                self.seg_ids.push(seg_id.clone());
                self.seg_ids.len() - 1
            });
        self.map.insert(chk_hash.clone(), (idx, chk_idx));
    }

    fn remove_chunks(&mut self, seg_id: &Eid, chk_indices: &[usize]) {
        self.seg_ids.iter().position(|s| s == seg_id).and_then(
            |idx| -> Option<()> {
                self.map.retain(
                    |_, val| val.0 != idx || !chk_indices.contains(&val.1),
                );
                None
            },
        );
    }

    fn remove_segment(&mut self, seg_id: &Eid) {
        self.seg_ids.iter().position(|s| s == seg_id).and_then(
            |idx| -> Option<()> {
                self.map.retain(|_, val| val.0 != idx);
                None
            },
        );
    }
}

/// Chunk map, used for chunk dedup
#[derive(Debug, Default, Clone)]
pub struct ChunkMap(Arc<RwLock<ChunkMapInner>>);

impl ChunkMap {
    pub fn new() -> Self {
        ChunkMap(Arc::new(RwLock::new(ChunkMapInner::new())))
    }

    pub fn get(&self, hash: &Hash) -> Option<ChunkLoc> {
        let inner = self.0.read().unwrap();
        inner.get(hash)
    }

    pub fn insert(&mut self, chk_hash: &Hash, seg_id: &Eid, chk_idx: usize) {
        let mut inner = self.0.write().unwrap();
        inner.insert(chk_hash, seg_id, chk_idx);
    }

    pub fn remove_chunks(&mut self, seg_id: &Eid, chk_indices: &[usize]) {
        let mut inner = self.0.write().unwrap();
        inner.remove_chunks(seg_id, chk_indices);
    }

    pub fn remove_segment(&mut self, seg_id: &Eid) {
        let mut inner = self.0.write().unwrap();
        inner.remove_segment(seg_id);
    }
}

impl Serialize for ChunkMap {
    fn serialize<S>(&self, serializer: S) -> StdResult<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let inner = self.0.read().unwrap();
        inner.serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for ChunkMap {
    fn deserialize<D>(deserializer: D) -> StdResult<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        ChunkMapInner::deserialize(deserializer).map(|inner| {
            ChunkMap(Arc::new(RwLock::new(inner)))
        })
    }
}
