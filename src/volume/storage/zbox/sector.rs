use std::collections::HashMap;
use std::fmt::{self, Debug};
use std::path::{Path, PathBuf};
use std::u16;

use bytes::BufMut;
use rmp_serde::{Deserializer, Serializer};
use serde::{Deserialize, Serialize};

use super::local_cache::{LocalCache, LocalCacheRef};
use base::crypto::{Crypto, HashKey, Key};
use error::{Error, Result};
use volume::address::Span;
use volume::BLK_SIZE;

// number of blocks in a sector, sector size is 128KB
const BLKS_PER_SECTOR: usize = 16;

// sector size
const SECTOR_SIZE: usize = BLKS_PER_SECTOR * BLK_SIZE;

const BASE_DIR: &str = "data";
const RECYCLE_FILE: &str = "recycle";

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
            let mut dmap = self.map.get(&sec_idx).cloned().unwrap_or(0);

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
            })
            .unwrap_or(false)
    }

    fn remove_deleted(&mut self, sec_idx: usize, mut span: Span) {
        span.begin %= BLKS_PER_SECTOR;
        let mut dmap = self.map.get(&sec_idx).cloned().unwrap_or(0);
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
        let buf = local_cache.get(&rel_path)?;
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
    // sector write buffer
    sec: Vec<u8>,
    sec_base: usize,
    sec_top: usize,
    sec_idx: usize,

    // sector recycle map
    rmap: RecycleMap,

    local_cache: LocalCacheRef,

    crypto: Crypto,
    key: Key,
    hash_key: HashKey,
}

impl SectorMgr {
    pub fn new(local_cache: &LocalCacheRef) -> Self {
        SectorMgr {
            sec: vec![0u8; SECTOR_SIZE],
            sec_base: 0,
            sec_top: 0,
            sec_idx: 0,
            rmap: RecycleMap::default(),
            local_cache: local_cache.clone(),
            crypto: Crypto::default(),
            key: Key::new_empty(),
            hash_key: HashKey::new_empty(),
        }
    }

    #[inline]
    pub fn set_crypto_ctx(&mut self, crypto: Crypto, key: Key) {
        self.crypto = crypto;
        self.hash_key = key.derive(0);
        self.rmap.hash_key = key.derive(1);
        self.key = key;
    }

    #[inline]
    pub fn init(&mut self) -> Result<()> {
        let mut local_cache = self.local_cache.write().unwrap();
        self.rmap.save(&self.crypto, &self.key, &mut local_cache)
    }

    #[inline]
    pub fn open(&mut self) -> Result<()> {
        let mut local_cache = self.local_cache.write().unwrap();
        self.rmap =
            RecycleMap::load(&self.crypto, &self.key, &mut local_cache)?;
        Ok(())
    }

    pub fn get_blocks(&mut self, dst: &mut [u8], span: Span) -> Result<()> {
        let mut local_cache = self.local_cache.write().unwrap();
        let mut read = 0;

        for sec_span in span.divide_by(BLKS_PER_SECTOR) {
            let sec_idx = sec_span.begin / BLKS_PER_SECTOR;

            // if any blocks are deleted
            if self.rmap.has_deleted(sec_idx, sec_span) {
                return Err(Error::NotFound);
            }

            let offset = (sec_span.begin % BLKS_PER_SECTOR) * BLK_SIZE;
            let len = sec_span.bytes_len();

            // if the blocks to be read are still in the staging sector
            if sec_idx == self.sec_idx && offset < self.sec_top {
                let end = offset + len;
                assert!(end <= self.sec_top);
                dst[read..read + len].copy_from_slice(&self.sec[offset..end]);
            } else {
                // otherwise read it from local cache
                let rel_path = sector_rel_path(sec_idx, &self.hash_key);
                local_cache.get_to(
                    &rel_path,
                    offset,
                    &mut dst[read..read + len],
                )?;
            }

            read += len;
        }

        Ok(())
    }

    pub fn put_blocks(&mut self, span: Span, mut blks: &[u8]) -> Result<()> {
        for sec_span in span.divide_by(BLKS_PER_SECTOR) {
            let sec_idx = sec_span.begin / BLKS_PER_SECTOR;
            let offset = (sec_span.begin % BLKS_PER_SECTOR) * BLK_SIZE;
            let len = sec_span.bytes_len();

            // if this write is not continuous, we need to 'jump' in the
            // staging sector accordingly
            if sec_idx != self.sec_idx || offset != self.sec_top {
                // if this is very first time write, fill the gap by reading
                // from local cache
                if self.sec_idx == 0 && self.sec_top == 0 && offset > 0 {
                    let mut local_cache = self.local_cache.write().unwrap();
                    let rel_path = sector_rel_path(sec_idx, &self.hash_key);
                    match local_cache.get(&rel_path) {
                        Ok(data) => {
                            self.sec[..data.len()].copy_from_slice(&data);
                        }
                        Err(ref err) if *err == Error::NotFound => {
                            // if the block watermark jumped too far, we would
                            // not be able to read the gap because the gap is
                            // in a hole, so we ignore this NotFound error
                        }
                        Err(err) => return Err(err),
                    }
                }

                // reset base pointer with new offset
                self.sec_base = offset;
            }

            // copy data to sector buffer
            self.sec_top = offset + len;
            self.sec_idx = sec_idx;
            self.sec[offset..self.sec_top].copy_from_slice(&blks[..len]);
            blks = &blks[len..];

            // ensure blocks are not in deleted map
            self.rmap.remove_deleted(sec_idx, sec_span);

            // if sector buffer is full, flush it to local cache
            if self.sec_top >= SECTOR_SIZE {
                self.flush()?;
            }
        }

        Ok(())
    }

    #[inline]
    pub fn del_blocks(&mut self, span: Span) -> Result<()> {
        let mut local_cache = self.local_cache.write().unwrap();
        self.rmap.del_blocks(span, &mut local_cache)
    }

    pub fn flush(&mut self) -> Result<()> {
        let mut local_cache = self.local_cache.write().unwrap();

        // save recycle map
        self.rmap.save(&self.crypto, &self.key, &mut local_cache)?;

        if self.sec_base == self.sec_top {
            return Ok(());
        }

        // save sector buffer to local cache
        let rel_path = sector_rel_path(self.sec_idx, &self.hash_key);
        local_cache.put(
            &rel_path,
            self.sec_base,
            &self.sec[self.sec_base..self.sec_top],
        )?;
        if self.sec_top >= SECTOR_SIZE {
            self.sec_top = 0;
            self.sec_idx += 1;
        }
        self.sec_base = self.sec_top;

        Ok(())
    }
}

impl Debug for SectorMgr {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("SectorMgr")
            .field("sec_base", &self.sec_base)
            .field("sec_top", &self.sec_top)
            .field("sec_idx", &self.sec_idx)
            .field("rmap", &self.rmap)
            .finish()
    }
}

#[cfg(test)]
mod tests {

    use super::*;
    use base::{init_env, IntoRef};
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
        )
        .unwrap();
        cache.connect().unwrap();
        cache.init().unwrap();

        let mut sec_mgr = SectorMgr::new(&cache.into_ref());
        let blks = vec![1u8; 2 * BLK_SIZE];
        let blks2 = vec![2u8; 14 * BLK_SIZE];
        let blks3 = vec![3u8; 18 * BLK_SIZE];
        let span = Span::new(0, 2);
        let span2 = Span::new(2, 14);
        let span3 = Span::new(16, 18);

        sec_mgr.put_blocks(span, &blks).unwrap();

        let mut dst = vec![0u8; blks.len()];
        sec_mgr.get_blocks(&mut dst, span).unwrap();
        assert_eq!(&dst, &blks);

        sec_mgr.put_blocks(span2, &blks2).unwrap();
        sec_mgr.flush().unwrap();

        let mut dst = vec![0u8; blks.len()];
        sec_mgr.get_blocks(&mut dst, span).unwrap();
        assert_eq!(&dst, &blks);

        let mut dst = vec![0u8; blks2.len()];
        sec_mgr.get_blocks(&mut dst, span2).unwrap();
        assert_eq!(&dst, &blks2);

        sec_mgr.del_blocks(span).unwrap();
        assert_eq!(
            sec_mgr.get_blocks(&mut dst, span).unwrap_err(),
            Error::NotFound
        );
        sec_mgr.del_blocks(span2).unwrap();

        sec_mgr.put_blocks(span3, &blks3).unwrap();
        sec_mgr.flush().unwrap();
    }
}
