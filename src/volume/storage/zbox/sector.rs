use std::collections::HashMap;
use std::fmt::{self, Debug};
use std::path::{Path, PathBuf};
use std::u16;

use bytes::BufMut;
use rmp_serde::{Deserializer, Serializer};
use serde::{Deserialize, Serialize};

use super::local_cache::LocalCache;
use base::crypto::{Crypto, HashKey, Key};
use error::{Error, Result};
use volume::address::Span;
use volume::BLK_SIZE;

// number of blocks in a sector
const BLKS_PER_SECTOR: usize = 16;

// sector size in bytes, 128KB
const SECTOR_SIZE: usize = BLKS_PER_SECTOR * BLK_SIZE;

const BASE_DIR: &'static str = "data";
const RECYCLE_FILE: &'static str = "recycle";

// make sector relative path from its index
fn sector_rel_path(sec_idx: usize, hash_key: &HashKey) -> PathBuf {
    let mut buf = Vec::with_capacity(8);
    buf.put_u64_le(sec_idx as u64);
    Path::new(BASE_DIR)
        .join(Crypto::hash_with_key(&buf, hash_key).to_rel_path())
}

/// Sector recycle map
#[derive(Debug, Default, Deserialize, Serialize)]
struct RecycleMap {
    // block deletion map, key: sector index, value: deletion bitmap
    map: HashMap<usize, u16>,

    #[serde(skip_serializing, skip_deserializing, default)]
    hash_key: HashKey,

    #[serde(skip_serializing, skip_deserializing, default)]
    is_saved: bool,
}

impl RecycleMap {
    // delete blocks
    fn del_blocks(
        &mut self,
        span: Span,
        local_cache: &mut LocalCache,
    ) -> Result<()> {
        for mut sec_span in span.divide_by(BLKS_PER_SECTOR) {
            let sec_idx = sec_span.begin / BLKS_PER_SECTOR;
            let mut dmap = self.map.get(&sec_idx).map(|v| *v).unwrap_or(0);

            // mark blocks as deleted in sector
            sec_span.begin %= BLKS_PER_SECTOR;
            for i in sec_span {
                dmap |= 1 << i;
            }

            // if all blocks in sector are deleted
            if dmap == u16::MAX {
                let rel_path = sector_rel_path(sec_idx, &self.hash_key);
                local_cache.del(&rel_path)?;
                self.map.remove(&sec_idx);
            } else {
                self.map.insert(sec_idx, dmap);
            }
        }
        self.is_saved = false;
        Ok(())
    }

    // check if any blocks in span are deleted
    fn has_deleted(&self, sec_idx: usize, mut span: Span) -> bool {
        span.begin %= BLKS_PER_SECTOR;
        self.map
            .get(&sec_idx)
            .map(|dmap| {
                for i in span {
                    let mask = 1 << i;
                    if (dmap & mask) != 0 {
                        return true;
                    }
                }
                false
            }).unwrap_or(false)
    }

    fn remove_deleted(&mut self, sec_idx: usize, mut span: Span) {
        span.begin %= BLKS_PER_SECTOR;
        let mut dmap = self.map.get(&sec_idx).map(|v| *v).unwrap_or(0);
        if dmap == 0 {
            return;
        }
        for i in span {
            dmap &= !(1 << i);
        }
        if dmap == 0 {
            self.map.remove(&sec_idx);
        } else {
            self.map.insert(sec_idx, dmap);
        }
        self.is_saved = false;
    }

    fn load(
        crypto: &Crypto,
        key: &Key,
        local_cache: &mut LocalCache,
    ) -> Result<Self> {
        let rel_path = Path::new(RECYCLE_FILE);
        let buf = local_cache.get_pinned(&rel_path)?;
        let buf = crypto.decrypt(&buf, key)?;
        let mut de = Deserializer::new(&buf[..]);
        let ret: Self = Deserialize::deserialize(&mut de)?;
        Ok(ret)
    }

    fn save(
        &mut self,
        crypto: &Crypto,
        key: &Key,
        local_cache: &mut LocalCache,
    ) -> Result<()> {
        if self.is_saved {
            return Ok(());
        }
        let mut buf = Vec::new();
        self.serialize(&mut Serializer::new(&mut buf)).unwrap();
        let buf = crypto.encrypt(&buf, key)?;
        local_cache.put_pinned(Path::new(RECYCLE_FILE), &buf)?;
        self.is_saved = true;
        Ok(())
    }
}

/// Sector manager
pub struct SectorMgr {
    // head sector
    head: Vec<u8>,
    head_offset: usize,
    head_len: usize,
    head_sec_idx: usize,

    // sector recycle map
    rmap: RecycleMap,

    crypto: Crypto,
    key: Key,
    hash_key: HashKey,
}

impl SectorMgr {
    pub fn new() -> Self {
        SectorMgr {
            head: vec![0u8; SECTOR_SIZE],
            head_offset: 0,
            head_len: 0,
            head_sec_idx: 0,
            rmap: RecycleMap::default(),
            crypto: Crypto::default(),
            key: Key::new_empty(),
            hash_key: HashKey::new_empty(),
        }
    }

    pub fn set_crypto_ctx(&mut self, crypto: Crypto, key: Key) {
        self.crypto = crypto;
        self.key = key.derive(0);
        self.rmap.hash_key = self.hash_key.clone()
    }

    #[inline]
    pub fn open(&mut self, local_cache: &mut LocalCache) -> Result<()> {
        self.rmap = RecycleMap::load(&self.crypto, &self.key, local_cache)?;
        Ok(())
    }

    pub fn get_blocks(
        &mut self,
        dst: &mut [u8],
        span: Span,
        local_cache: &mut LocalCache,
    ) -> Result<()> {
        let mut read = 0;

        for sec_span in span.divide_by(BLKS_PER_SECTOR) {
            let sec_idx = sec_span.begin / BLKS_PER_SECTOR;

            // if any bloks are deleted
            if self.rmap.has_deleted(sec_idx, sec_span) {
                return Err(Error::NotFound);
            }

            // if any blocks are still in head sector buffer
            if self.head_len > 0 && sec_idx == self.head_sec_idx {
                let head_span = Span::new(
                    self.head_sec_idx * BLKS_PER_SECTOR,
                    BLKS_PER_SECTOR,
                );
                if let Some(intersect) = head_span.intersect(sec_span) {
                    assert!(span.end() <= head_span.end());
                    let offset = (intersect.begin % BLKS_PER_SECTOR) * BLK_SIZE;
                    let copy_len = intersect.bytes_len();
                    dst[read..read + copy_len]
                        .copy_from_slice(&self.head[offset..offset + copy_len]);
                    read += copy_len;
                    continue;
                }
            }

            // otherwise get it from local cache
            let rel_path = sector_rel_path(sec_idx, &self.hash_key);
            let offset = (sec_span.begin % BLKS_PER_SECTOR) * BLK_SIZE;
            let len = sec_span.bytes_len();

            local_cache.get(&rel_path, offset, &mut dst[read..read + len])?;
            read += len;
        }

        Ok(())
    }

    pub fn put_blocks(
        &mut self,
        span: Span,
        mut blks: &[u8],
        local_cache: &mut LocalCache,
    ) -> Result<()> {
        for sec_span in span.divide_by(BLKS_PER_SECTOR) {
            let sec_idx = sec_span.begin / BLKS_PER_SECTOR;
            let offset = (sec_span.begin % BLKS_PER_SECTOR) * BLK_SIZE;
            let len = sec_span.bytes_len();

            self.head[offset..offset + len].copy_from_slice(&blks[..len]);
            blks = &blks[len..];
            self.head_len += len;
            self.head_sec_idx = sec_idx;

            // write to local cache if sector buffer is full
            if self.head_len >= SECTOR_SIZE {
                self.flush(local_cache)?;
            }

            // ensure blocks are not in deleted map
            self.rmap.remove_deleted(sec_idx, sec_span);
        }

        Ok(())
    }

    #[inline]
    pub fn del_blocks(
        &mut self,
        span: Span,
        local_cache: &mut LocalCache,
    ) -> Result<()> {
        self.rmap.del_blocks(span, local_cache)
    }

    pub fn flush(&mut self, local_cache: &mut LocalCache) -> Result<()> {
        // save recyle map
        self.rmap.save(&self.crypto, &self.key, local_cache)?;

        if self.head_offset == self.head_len {
            return Ok(());
        }

        // write sector to local cache
        let rel_path = sector_rel_path(self.head_sec_idx, &self.hash_key);
        local_cache.put(
            &rel_path,
            self.head_offset,
            &self.head[self.head_offset..self.head_len],
        )?;

        // if head sector is full, reset head sector
        if self.head_len >= SECTOR_SIZE {
            self.head_len = 0;
        }

        // advance head sector
        self.head_offset = self.head_len;

        Ok(())
    }
}

impl Debug for SectorMgr {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("SectorMgr")
            .field("head_offset", &self.head_offset)
            .field("head_len", &self.head_len)
            .field("head_sec_idx", &self.head_sec_idx)
            .field("rmap", &self.rmap)
            .finish()
    }
}

#[cfg(test)]
mod tests {

    use super::*;
    use base::init_env;
    use volume::storage::zbox::local_cache::CacheType;

    #[test]
    fn sector_oper() {
        init_env();
        let repo_id = "repo456";
        let access_key = "accessKey456";
        let mut cache = LocalCache::new(
            CacheType::Mem,
            1,
            Path::new(""),
            &repo_id,
            &access_key,
        ).unwrap();
        let mut sec_mgr = SectorMgr::new();
        let blks = vec![1u8; 2 * BLK_SIZE];
        let blks2 = vec![2u8; 14 * BLK_SIZE];
        let blks3 = vec![3u8; 18 * BLK_SIZE];
        let span = Span::new(0, 2);
        let span2 = Span::new(2, 14);
        let span3 = Span::new(16, 18);

        cache.init().unwrap();

        sec_mgr.put_blocks(span, &blks, &mut cache).unwrap();

        let mut dst = vec![0u8; blks.len()];
        sec_mgr.get_blocks(&mut dst, span, &mut cache).unwrap();
        assert_eq!(&dst, &blks);

        sec_mgr.put_blocks(span2, &blks2, &mut cache).unwrap();
        sec_mgr.flush(&mut cache).unwrap();

        let mut dst = vec![0u8; blks.len()];
        sec_mgr.get_blocks(&mut dst, span, &mut cache).unwrap();
        assert_eq!(&dst, &blks);

        let mut dst = vec![0u8; blks2.len()];
        sec_mgr.get_blocks(&mut dst, span2, &mut cache).unwrap();
        assert_eq!(&dst, &blks2);

        sec_mgr.del_blocks(span, &mut cache).unwrap();
        assert_eq!(
            sec_mgr.get_blocks(&mut dst, span, &mut cache).unwrap_err(),
            Error::NotFound
        );
        sec_mgr.del_blocks(span2, &mut cache).unwrap();

        sec_mgr.put_blocks(span3, &blks3, &mut cache).unwrap();
        sec_mgr.flush(&mut cache).unwrap();
    }
}
