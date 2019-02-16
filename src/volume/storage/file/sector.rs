use std::fmt::{self, Debug};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::u16;

use bytes::BufMut;
use linked_hash_map::LinkedHashMap;

use super::file_armor::FileArmor;
use super::vio;
use base::crypto::{Crypto, HashKey, Key};
use base::lru::{CountMeter, Lru, PinChecker};
use base::utils::{ensure_parents_dir, remove_empty_parent_dir};
use error::{Error, Result};
use trans::{Eid, Id};
use volume::address::Span;
use volume::storage::index_mgr::Accessor;
use volume::{Arm, ArmAccess, Armor, Seq, BLK_SIZE};

// how many blocks in a sector, must be 2^n and less than u16::MAX
pub const BLKS_PER_SECTOR: usize = 4 * 1024;

// sector size, in bytes
pub const SECTOR_SIZE: usize = BLK_SIZE * BLKS_PER_SECTOR;

// block deletion mark
const BLK_DELETE_MARK: u16 = u16::MAX;

// sector cache size
const SECTOR_CACHE_SIZE: usize = 16;

// sector data file cache size
const SECTOR_DATA_CACHE_SIZE: usize = 4;

// sector
#[derive(Default, Clone, Deserialize, Serialize)]
struct Sector {
    id: Eid,
    seq: u64,
    arm: Arm,
    idx: usize,

    // sector current size in bytes, excluding deleted blocks
    // curr_size > 0 means sector is finished writing
    curr_size: usize,

    // sector actual size in bytes, including deleted blocks
    actual_size: usize,

    // block offset map, length is BLKS_PER_SECTOR, u16::MAX means deleted
    blk_map: Vec<u16>,
}

impl Sector {
    #[inline]
    fn new(id: &Eid, idx: usize) -> Self {
        Sector {
            id: id.clone(),
            seq: 0,
            arm: Arm::default(),
            idx,
            curr_size: 0,
            actual_size: 0,
            blk_map: (0..BLKS_PER_SECTOR as u16).collect(),
        }
    }

    #[inline]
    fn is_finished(&self) -> bool {
        self.curr_size > 0
    }

    // if the actual size is smaller than 1/4 of current size, then it is shrinkable
    #[inline]
    fn is_shrinkable(&self) -> bool {
        self.actual_size <= self.curr_size >> 2
    }

    // mark blocks as deleted
    fn mark_blocks_deletion(&mut self, span: Span) {
        let insec_idx = span.begin % BLKS_PER_SECTOR;
        let mut deleted_size = 0;

        // mark blocks as deleted
        for idx in insec_idx..insec_idx + span.cnt {
            if self.blk_map[idx] != BLK_DELETE_MARK {
                self.blk_map[idx] = BLK_DELETE_MARK;
                deleted_size += BLK_SIZE;
            }
        }

        // if sector is finished, update its actual size
        if self.is_finished() {
            self.actual_size -= deleted_size;
        }
    }
}

impl Id for Sector {
    #[inline]
    fn id(&self) -> &Eid {
        &self.id
    }

    #[inline]
    fn id_mut(&mut self) -> &mut Eid {
        &mut self.id
    }
}

impl Seq for Sector {
    #[inline]
    fn seq(&self) -> u64 {
        self.seq
    }

    #[inline]
    fn inc_seq(&mut self) {
        self.seq += 1
    }
}

impl<'de> ArmAccess<'de> for Sector {
    #[inline]
    fn arm(&self) -> Arm {
        self.arm
    }

    #[inline]
    fn arm_mut(&mut self) -> &mut Arm {
        &mut self.arm
    }
}

impl Debug for Sector {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("Sector")
            .field("id", &self.id)
            .field("seq", &self.seq)
            .field("arm", &self.arm)
            .field("idx", &self.idx)
            .field("curr_size", &self.curr_size)
            .field("actual_size", &self.actual_size)
            .finish()
    }
}

// sector manager
pub struct SectorMgr {
    base: PathBuf,

    sec_armor: FileArmor<Sector>,

    // sector cache
    sec_cache: Lru<usize, Sector, CountMeter<Sector>, PinChecker<Sector>>,

    // sector data file cache
    sec_data_cache: LinkedHashMap<usize, vio::File>,

    hash_key: HashKey,
}

impl SectorMgr {
    // sector data and shrink file file extensions
    const SECTOR_DATA_EXT: &'static str = "data";
    const SECTOR_SHRINK_EXT: &'static str = "shrink";

    pub fn new(base: &Path) -> Self {
        SectorMgr {
            base: base.to_path_buf(),
            sec_armor: FileArmor::new(base),
            sec_cache: Lru::new(SECTOR_CACHE_SIZE),
            sec_data_cache: LinkedHashMap::new(),
            hash_key: HashKey::new_empty(),
        }
    }

    #[inline]
    pub fn set_crypto_ctx(
        &mut self,
        crypto: Crypto,
        key: Key,
        hash_key: HashKey,
    ) {
        self.sec_armor.set_crypto_ctx(crypto, key);
        self.hash_key = hash_key;
    }

    // convert sector index to Eid
    fn sector_idx_to_id(&self, sec_idx: usize) -> Eid {
        let mut buf = Vec::with_capacity(8);
        buf.put_u64_le(sec_idx as u64);
        let hash = Crypto::hash_with_key(&buf, &self.hash_key);
        Eid::from_slice(&hash)
    }

    // sector data file path
    fn sector_data_path(&self, sec_idx: usize) -> PathBuf {
        let id = self.sector_idx_to_id(sec_idx);
        let mut path = id.to_path_buf(&self.base);
        path.set_extension(Self::SECTOR_DATA_EXT);
        path
    }

    // open a sector where the block index sits in
    fn open_sector(
        &mut self,
        sec_idx: usize,
        create: bool,
    ) -> Result<&mut Sector> {
        if !self.sec_cache.contains_key(&sec_idx) {
            let sec_id = self.sector_idx_to_id(sec_idx);

            // load sector and insert into cache
            match self.sec_armor.load_item(&sec_id) {
                Ok(sec) => {
                    self.sec_cache.insert(sec_idx, sec);
                }
                Err(ref err) if *err == Error::NotFound => {
                    if create {
                        // if sector doesn't exist, create a new sector
                        // and save it to cache
                        let mut sec = Sector::new(&sec_id, sec_idx);
                        self.sec_armor.save_item(&mut sec)?;
                        self.sec_cache.insert(sec_idx, sec);
                    } else {
                        return Err(Error::NotFound);
                    }
                }
                Err(err) => return Err(err),
            }
        }

        let sec = self.sec_cache.get_refresh(&sec_idx).unwrap();

        Ok(sec)
    }

    // save sector to file
    fn save_sector(&mut self, sec_idx: usize) -> Result<()> {
        let mut sec = self.sec_cache.get_refresh(&sec_idx).unwrap();
        self.sec_armor.save_item(&mut sec)
    }

    // open sector data file
    fn open_sector_data(
        &mut self,
        sec_idx: usize,
        create: bool,
    ) -> Result<vio::File> {
        if !self.sec_data_cache.contains_key(&sec_idx) {
            // open sector data file and save it to cache
            let path = self.sector_data_path(sec_idx);
            if !create && !path.exists() {
                return Err(Error::NotFound);
            }
            ensure_parents_dir(&path)?;
            let data_file = vio::OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .open(&path)?;
            self.sec_data_cache.insert(sec_idx, data_file);
            if self.sec_data_cache.len() >= SECTOR_DATA_CACHE_SIZE {
                self.sec_data_cache.pop_front();
            }
        }

        let data_file = self.sec_data_cache.get_refresh(&sec_idx).unwrap();
        let data_file = data_file.try_clone()?;
        Ok(data_file)
    }

    // read data blocks
    pub fn read_blocks(&mut self, dst: &mut [u8], span: Span) -> Result<()> {
        assert_eq!(dst.len(), span.bytes_len());

        let mut read = 0;
        for sec_span in span.divide_by(BLKS_PER_SECTOR) {
            let sec_idx = sec_span.begin / BLKS_PER_SECTOR;
            let mut sec_data = self.open_sector_data(sec_idx, false)?;
            let blk_offset = {
                let sec = self.open_sector(sec_idx, false)?;
                let map_idx = sec_span.begin % BLKS_PER_SECTOR;
                let insec_idx = sec.blk_map[map_idx];
                if sec.blk_map[map_idx..map_idx + sec_span.cnt]
                    .iter()
                    .any(|b| *b == BLK_DELETE_MARK)
                {
                    return Err(Error::NotFound);
                }
                u64::from(insec_idx) * BLK_SIZE as u64
            };

            // read blocks bytes
            let read_len = sec_span.bytes_len();
            sec_data.seek(SeekFrom::Start(blk_offset))?;
            sec_data.read_exact(&mut dst[read..read + read_len])?;
            read += read_len;
        }

        Ok(())
    }

    // write data blocks to sector
    pub fn write_blocks(&mut self, span: Span, mut blks: &[u8]) -> Result<()> {
        assert_eq!(blks.len(), span.bytes_len());

        for sec_span in span.divide_by(BLKS_PER_SECTOR) {
            let sec_idx = sec_span.begin / BLKS_PER_SECTOR;
            let mut sec_data = self.open_sector_data(sec_idx, true)?;
            let blk_offset = (sec_span.begin % BLKS_PER_SECTOR) * BLK_SIZE;

            // write blocks bytes to sector data file
            let write_len = sec_span.bytes_len();
            sec_data.seek(SeekFrom::Start(blk_offset as u64))?;
            sec_data.write_all(&blks[..write_len])?;
            blks = &blks[write_len..];
            drop(sec_data);

            // In case of a tx contains deletion operation
            // and that deletes blocks in the same sector, the blocks will
            // remain as deleted if the tx aborted. This is to deal with
            // that situation by overwriting the deletion mark.
            let corrected_blk_cnt = {
                // this will ensure sector is created as well
                let sec = self.open_sector(sec_idx, true)?;

                assert!(!sec.is_finished());
                let map_idx = sec_span.begin % BLKS_PER_SECTOR;
                let mut corrected = 0;
                for i in map_idx..map_idx + sec_span.cnt {
                    if sec.blk_map[i] == BLK_DELETE_MARK {
                        sec.blk_map[i] = i as u16;
                        corrected += 1;
                    }
                }
                corrected
            };
            if corrected_blk_cnt > 0 {
                warn!(
                    "corrected {} deleted block when write blocks",
                    corrected_blk_cnt
                );
                self.save_sector(sec_idx)?;
            }

            // if we reached the end of sector, mark it as finished
            if sec_span.end() % BLKS_PER_SECTOR == 0 {
                let is_shrinkable = {
                    let sec = self.open_sector(sec_idx, false)?;
                    sec.curr_size = SECTOR_SIZE;
                    sec.actual_size = BLK_SIZE
                        * sec
                            .blk_map
                            .iter()
                            .filter(|b| **b != BLK_DELETE_MARK)
                            .count() as usize;
                    sec.is_shrinkable()
                };

                if is_shrinkable {
                    // shrink sector
                    self.shrink_sector(sec_idx)?;
                } else {
                    // save sector
                    self.save_sector(sec_idx)?;
                }
            }
        }

        Ok(())
    }

    // shrink a sector
    fn shrink_sector(&mut self, sec_idx: usize) -> Result<()> {
        let mut sec = self.open_sector(sec_idx, false)?.clone();
        let mut sec_data = self.open_sector_data(sec_idx, false)?;

        // open shrink destination file
        let data_file_path = self.sector_data_path(sec.idx);
        let mut dst_path = data_file_path.clone();
        dst_path.set_extension(Self::SECTOR_SHRINK_EXT);
        let mut dst_file = vio::OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(&dst_path)?;

        // copy all not deleted blocks to destination file
        let mut buf = vec![0u8; BLK_SIZE];
        let mut written_blk_cnt = 0;
        for insec_idx in sec.blk_map.iter_mut() {
            // skip deleted block
            if *insec_idx == BLK_DELETE_MARK {
                continue;
            }

            let data_offset = *insec_idx as usize * BLK_SIZE;
            if data_offset >= sec.curr_size {
                break;
            }

            // read from sector and write to destination
            sec_data.seek(SeekFrom::Start(data_offset as u64))?;
            sec_data.read_exact(&mut buf)?;
            dst_file.write_all(&buf)?;

            *insec_idx = written_blk_cnt;
            written_blk_cnt += 1;
        }

        // set sector new size, save sector and update sector in cache
        sec.actual_size = written_blk_cnt as usize * BLK_SIZE;
        sec.curr_size = sec.actual_size;
        self.sec_armor.save_item(&mut sec)?;
        self.sec_cache.insert(sec.idx, sec);

        // close all opened sector data files and switch it
        drop(sec_data);
        self.sec_data_cache.remove(&sec_idx);
        vio::rename(&dst_path, &data_file_path)?;

        Ok(())
    }

    // delete data blocks
    pub fn del_blocks(&mut self, span: Span) -> Result<()> {
        for sec_span in span.divide_by(BLKS_PER_SECTOR) {
            let sec_idx = sec_span.begin / BLKS_PER_SECTOR;
            let sec_id;
            let actual_size;
            let is_finished;
            let is_shrinkable;

            {
                match self.open_sector(sec_idx, false) {
                    Ok(sec) => {
                        // mark blocks as deleted
                        sec.mark_blocks_deletion(sec_span);

                        sec_id = sec.id.clone();
                        actual_size = sec.actual_size;
                        is_finished = sec.is_finished();
                        is_shrinkable = sec.is_shrinkable();
                    }
                    Err(ref err) if *err == Error::NotFound => continue,
                    Err(err) => return Err(err),
                }
            }

            // if this sector is not finished yet, save the sector
            if !is_finished {
                self.save_sector(sec_idx)?;
                continue;
            }

            // if all blocks are deleted, remove the whole sector
            // including sector and sector data file
            if actual_size == 0 {
                self.sec_armor.remove_all_arms(&sec_id)?;
                let sec_data_path = self.sector_data_path(sec_idx);
                vio::remove_file(&sec_data_path)?;
                remove_empty_parent_dir(&sec_data_path)?;
                self.sec_cache.remove(&sec_idx);
            } else if is_shrinkable {
                // shrink sector if possible
                self.shrink_sector(sec_idx)?;
            } else {
                // otherwise, save the sector
                self.save_sector(sec_idx)?;
            }
        }

        Ok(())
    }
}

impl Debug for SectorMgr {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("SectorMgr")
            .field("hash_key", &self.hash_key)
            .finish()
    }
}
