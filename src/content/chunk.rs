use std::collections::HashMap;
use std::fmt::{self, Debug};
use std::ops::Deref;
use std::result::Result as StdResult;
use std::sync::{Arc, RwLock};

use serde::de::{self, Deserializer, SeqAccess};
use serde::ser::{SerializeSeq, Serializer};
use serde::{Deserialize, Serialize};

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

#[derive(Debug, Default, Clone, Deserialize, Serialize)]
struct ChunkMapInner {
    seg_ids: Vec<Eid>, // segment id array

    // key: chunk hash
    // val: (index in segment id array, index in segment chunk list)
    map: HashMap<Hash, (usize, usize)>,
}

impl ChunkMapInner {
    fn get(&self, hash: &Hash) -> Option<ChunkLoc> {
        self.map.get(hash).map(|&(seg_idx, chk_idx)| ChunkLoc {
            seg_id: self.seg_ids[seg_idx as usize].clone(),
            idx: chk_idx,
        })
    }

    fn insert(&mut self, chk_hash: &Hash, seg_id: &Eid, chk_idx: usize) {
        let idx = self
            .seg_ids
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
                self.map.retain(|_, val| {
                    val.0 != idx || !chk_indices.contains(&val.1)
                });
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
pub struct ChunkMap {
    inner: Arc<RwLock<ChunkMapInner>>,
    is_enabled: bool,
}

impl ChunkMap {
    #[inline]
    pub fn new(is_enabled: bool) -> Self {
        ChunkMap {
            inner: Arc::default(),
            is_enabled,
        }
    }

    pub fn get(&self, hash: &Hash) -> Option<ChunkLoc> {
        if !self.is_enabled {
            return None;
        }
        let inner = self.inner.read().unwrap();
        inner.get(hash)
    }

    pub fn insert(&mut self, chk_hash: &Hash, seg_id: &Eid, chk_idx: usize) {
        if !self.is_enabled {
            return;
        }
        let mut inner = self.inner.write().unwrap();
        inner.insert(chk_hash, seg_id, chk_idx);
    }

    pub fn remove_chunks(&mut self, seg_id: &Eid, chk_indices: &[usize]) {
        if !self.is_enabled {
            return;
        }
        let mut inner = self.inner.write().unwrap();
        inner.remove_chunks(seg_id, chk_indices);
    }

    pub fn remove_segment(&mut self, seg_id: &Eid) {
        if !self.is_enabled {
            return;
        }
        let mut inner = self.inner.write().unwrap();
        inner.remove_segment(seg_id);
    }
}

impl Serialize for ChunkMap {
    fn serialize<S>(&self, serializer: S) -> StdResult<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let inner = self.inner.read().unwrap();

        // 2 is the numnber of fields
        let mut seq = serializer.serialize_seq(Some(2))?;
        seq.serialize_element(inner.deref())?;
        seq.serialize_element(&self.is_enabled)?;
        seq.end()
    }
}

struct ChunkMapVisitor;

impl<'de> de::Visitor<'de> for ChunkMapVisitor {
    type Value = ChunkMap;

    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        write!(formatter, "struct ChunkMap")
    }

    fn visit_seq<V>(self, mut seq: V) -> StdResult<Self::Value, V::Error>
    where
        V: SeqAccess<'de>,
    {
        let inner = seq
            .next_element()?
            .ok_or_else(|| de::Error::invalid_length(0, &self))?;
        let is_enabled = seq
            .next_element()?
            .ok_or_else(|| de::Error::invalid_length(1, &self))?;
        Ok(ChunkMap {
            inner: Arc::new(RwLock::new(inner)),
            is_enabled,
        })
    }
}

impl<'de> Deserialize<'de> for ChunkMap {
    fn deserialize<D>(deserializer: D) -> StdResult<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_seq(ChunkMapVisitor)
    }
}
