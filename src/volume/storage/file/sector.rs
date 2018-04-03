use std::error::Error as StdError;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::fmt::{self, Debug};
use std::io::{Error as IoError, ErrorKind, Read, Result as IoResult, Seek,
              SeekFrom, Write};
use std::slice;
use std::cmp::min;
use std::u16;

use bytes::{Buf, BufMut, Bytes, IntoBuf, LittleEndian};

use error::Result;
use base::crypto::{Crypto, HashKey, Key};
use base::lru::{CountMeter, Lru, PinChecker};
use base::utils::{align_offset, align_offset_u64};
use trans::Txid;
use super::remove_file;
use super::span::SpanList;
use super::vio::imp as vio_imp;

// block size, in bytes
pub const BLK_SIZE: usize = 4 * 1024;

// how many blocks in a sector, must be 2^n and less than u16::MAX
pub const SECTOR_BLK_CNT: usize = 4096;

// sector size, in bytes
pub const SECTOR_SIZE: usize = BLK_SIZE * SECTOR_BLK_CNT;

// max number of blocks in block cache
const BLK_CACHE_CAPACITY: usize = 2048;

// block deletion mark
const BLK_DELETE_MARK: u16 = u16::MAX;

// subkey constant for hash key derivation
const SUBKEY_ID: u64 = 42;

// get sector size level
// [SECTOR_SIZE..SECTOR_SIZE / 4) => 0
// ...
// [SECTOR_SIZE / 4096..0) => 6
fn size_level(size: usize) -> u8 {
    assert!(size <= SECTOR_SIZE);
    let mut high = SECTOR_SIZE;
    let mut low = SECTOR_SIZE >> 2;
    let mut lvl = 0;
    while lvl < 6 {
        if low < size && size <= high {
            break;
        }
        high = low;
        low >>= 2;
        lvl += 1;
    }
    lvl
}

/// Location Id
#[derive(Debug, Clone, Copy, Hash, Default, PartialEq, Eq)]
pub struct LocId {
    pub(super) txid: Txid,
    pub(super) idx: u64,
}

impl LocId {
    const BYTES_LEN: usize = 16;

    pub fn new(txid: Txid, idx: u64) -> Self {
        LocId { txid, idx }
    }

    fn lower_blk_bound(&self) -> u64 {
        self.idx * SECTOR_BLK_CNT as u64
    }

    fn upper_blk_bound(&self) -> u64 {
        (self.idx + 1) * SECTOR_BLK_CNT as u64
    }
}

/// Space
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct Space {
    pub(super) txid: Txid,
    pub(super) spans: SpanList,
}

impl Space {
    pub fn new(txid: Txid, spans: SpanList) -> Self {
        Space { txid, spans }
    }

    pub fn len(&self) -> usize {
        self.spans.len
    }

    pub fn set_len(&mut self, len: usize) {
        self.spans.len = len;
    }

    pub fn append(&mut self, other: &Space) {
        assert_eq!(self.txid, other.txid);
        let offset = self.spans.len as u64;
        for span in other.spans.iter() {
            let mut span = span.clone();
            span.offset += offset;
            self.spans.append(span, 0);
        }
        self.spans.len += other.len();
    }

    fn divide_into_sectors(&self) -> Vec<(LocId, SpanList)> {
        let mut ret: Vec<(LocId, SpanList)> = Vec::new();
        for span in self.spans.iter() {
            let mut span = span.clone();
            let begin = span.begin / SECTOR_BLK_CNT as u64;
            let end = span.end / SECTOR_BLK_CNT as u64 + 1;
            for sec_idx in begin..end {
                let sec_id = LocId::new(self.txid, sec_idx);
                let ubound = min(span.end, sec_id.upper_blk_bound());
                let split = span.split_to(ubound);
                if split.is_empty() {
                    continue;
                }
                if let Some(&mut (loc, ref mut spans)) = ret.last_mut() {
                    if loc.idx == sec_idx {
                        spans.append(split, split.blk_len());
                        continue;
                    }
                }
                ret.push((sec_id, split.into_span_list(split.blk_len())));
            }
        }
        ret
    }
}

/// Sector
#[derive(Debug)]
struct Sector {
    id: LocId,
    blk_map: Vec<u16>, // block offset map
    path: PathBuf,
}

impl Sector {
    const BYTES_LEN: usize = LocId::BYTES_LEN + 2 * SECTOR_BLK_CNT;
    const BACKUP_EXT: &'static str = "bk";
    const DATA_EXT: &'static str = "data";
    const DATA_BACKUP_EXT: &'static str = "data_bk";

    fn new(id: LocId, path: PathBuf) -> Self {
        Sector {
            id,
            blk_map: (0..SECTOR_BLK_CNT as u16).collect(),
            path,
        }
    }

    // sector backup file path
    fn backup_path(&self) -> PathBuf {
        self.path.with_extension(Sector::BACKUP_EXT)
    }

    // data file path
    fn data_path(&self) -> PathBuf {
        self.path.with_extension(Sector::DATA_EXT)
    }

    // data file backup path
    fn data_backup_path(&self) -> PathBuf {
        self.path.with_extension(Sector::DATA_BACKUP_EXT)
    }
}

/// Sector manager
pub struct SectorMgr {
    base: PathBuf,
    lru: Lru<LocId, Bytes, CountMeter<Bytes>, PinChecker<Bytes>>,
    skey: Key,
    hash_key: HashKey,
    crypto: Crypto,
}

impl SectorMgr {
    const DIR_NAME: &'static str = "data";

    pub fn new(base: &Path) -> Self {
        SectorMgr {
            base: base.join(SectorMgr::DIR_NAME),
            lru: Lru::new(BLK_CACHE_CAPACITY),
            skey: Key::new_empty(),
            hash_key: HashKey::new_empty(),
            crypto: Crypto::default(),
        }
    }

    pub fn init(&self) -> Result<()> {
        vio_imp::create_dir(&self.base)?;
        Ok(())
    }

    pub fn set_crypto_key(
        &mut self,
        crypto: &Crypto,
        skey: &Key,
    ) -> Result<()> {
        self.crypto = crypto.clone();
        self.skey = skey.clone();
        self.hash_key = Crypto::derive_from_key(skey, SUBKEY_ID)?;
        Ok(())
    }

    // generate sector file path from sector id
    fn sec_path(&self, sec_id: LocId) -> PathBuf {
        let mut buf = Vec::with_capacity(16);
        buf.put_u64::<LittleEndian>(sec_id.txid.val());
        buf.put_u64::<LittleEndian>(sec_id.idx);
        let hash = Crypto::hash_with_key(&buf, &self.hash_key);
        let s = hash.to_string();
        self.base.join(&s[0..2]).join(&s[2..4]).join(&s)
    }

    fn load_sec(&self, path: &Path) -> IoResult<Sector> {
        // read from file
        let mut file = vio_imp::OpenOptions::new()
            .read(true)
            .write(true)
            .open(&path)?;
        let mut buf = vec![0u8; self.crypto.encrypted_len(Sector::BYTES_LEN)];
        file.read_exact(&mut buf)?;

        // make ad buffer
        let mut ad = Vec::with_capacity(4);
        ad.put_u32::<LittleEndian>(Sector::BYTES_LEN as u32);

        // decrypt
        let dec =
            map_io_err!(self.crypto.decrypt_with_ad(&buf, &self.skey, &ad))?;

        // deserialize
        let mut buf = dec.into_buf();
        let mut id = LocId::default();
        id.txid = Txid::from(buf.get_u64::<LittleEndian>());
        id.idx = buf.get_u64::<LittleEndian>();
        let mut blk_map = vec![0u16; SECTOR_BLK_CNT];
        for i in 0..SECTOR_BLK_CNT {
            blk_map[i] = buf.get_u16::<LittleEndian>();
        }

        Ok(Sector {
            id,
            blk_map,
            path: path.to_path_buf(),
        })
    }

    fn save_sec(&self, sec: &Sector) -> IoResult<()> {
        // serialize sector
        let mut buf = Vec::with_capacity(Sector::BYTES_LEN);
        buf.put_u64::<LittleEndian>(sec.id.txid.val());
        buf.put_u64::<LittleEndian>(sec.id.idx);
        let slice = unsafe {
            slice::from_raw_parts(
                sec.blk_map.as_ptr() as *const u8,
                SECTOR_BLK_CNT * 2,
            )
        };
        buf.put_slice(slice);

        // make ad buffer
        let mut ad = Vec::with_capacity(4);
        ad.put_u32::<LittleEndian>(Sector::BYTES_LEN as u32);

        // encrypt
        let enc =
            map_io_err!(self.crypto.encrypt_with_ad(&buf, &self.skey, &ad))?;

        // write to file
        let mut file = vio_imp::OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(&sec.path)?;
        file.write_all(&enc).and(file.sync_all())
    }

    // open sector file, create if it doesn't exist
    fn open_sec(&self, sec_id: LocId) -> IoResult<Sector> {
        let path = self.sec_path(sec_id);
        if path.exists() {
            self.load_sec(&path)
        } else {
            vio_imp::create_dir_all(path.parent().unwrap())?;
            let sec = Sector::new(sec_id, path);
            self.save_sec(&sec)?;
            Ok(sec)
        }
    }

    // open sector data file, create if it doesn't exist
    fn open_sec_data(&self, sec_path: &Path) -> IoResult<vio_imp::File> {
        let path = sec_path.with_extension(Sector::DATA_EXT);
        vio_imp::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(path)
    }

    // read data
    pub fn read(
        &mut self,
        buf: &mut [u8],
        space: &Space,
        offset: u64,
    ) -> IoResult<usize> {
        let buf_len = buf.len();
        let space_len = space.len();
        let mut start = offset;
        let mut read: usize = 0;

        if offset == space_len as u64 {
            return Ok(0);
        } else if offset > space_len as u64 {
            return Err(IoError::new(
                ErrorKind::UnexpectedEof,
                "Read beyond EOF",
            ));
        }

        for &(sec_id, ref spans) in space
            .divide_into_sectors()
            .iter()
            .skip_while(|&&(_, ref spans)| offset < spans.offset())
        {
            let path = self.sec_path(sec_id);

            // open sector and sector data file
            let sec = self.open_sec(sec_id)?;
            let mut data_file = self.open_sec_data(&path)?;

            for span in spans.iter().skip_while(|s| offset >= s.end_offset()) {
                let start_blk_idx =
                    span.begin + (start - span.offset) / BLK_SIZE as u64;
                for blk_idx in start_blk_idx..span.end {
                    let blk_id = LocId::new(space.txid, blk_idx);

                    if !self.lru.contains_key(&blk_id) {
                        // block is not in cache, read it from sector data file
                        // and add it to cache
                        let idx =
                            align_offset_u64(blk_idx, SECTOR_BLK_CNT as u64);
                        let idx = sec.blk_map[idx as usize];
                        let data_offset = idx as u64 * BLK_SIZE as u64;
                        data_file.seek(SeekFrom::Start(data_offset as u64))?;
                        let mut blk = vec![0u8; BLK_SIZE];
                        data_file.read_exact(&mut blk)?;
                        self.lru.insert(blk_id, Bytes::from(blk));
                    }

                    let blk_offset = align_offset(start as usize, BLK_SIZE);
                    let copy_len = min(
                        space_len - start as usize,
                        min(buf_len - read, BLK_SIZE - blk_offset),
                    );
                    let blk = self.lru.get_refresh(&blk_id).unwrap();
                    let blk = &blk[blk_offset..blk_offset + copy_len];

                    // copy data to destination buffer
                    buf[read..read + copy_len].copy_from_slice(blk);
                    read += copy_len;
                    start += copy_len as u64;
                    if read >= buf_len || read >= space_len {
                        return Ok(read);
                    }
                }
            }
        }

        Ok(read)
    }

    // write data
    pub fn write(
        &self,
        mut buf: &[u8],
        space: &Space,
        offset: u64,
    ) -> IoResult<()> {
        let mut start = offset;

        for &(sec_id, ref spans) in space
            .divide_into_sectors()
            .iter()
            .skip_while(|&&(_, ref spans)| offset < spans.offset())
        {
            let path = self.sec_path(sec_id);

            // create sector file if it doesn't exist
            if !path.exists() {
                self.open_sec(sec_id)?;
            }

            // then open sector data file
            let mut data_file = self.open_sec_data(&path)?;

            for span in spans.iter().skip_while(|s| offset >= s.end_offset()) {
                let sec_offset = span.offset_in_sec(start);
                let ubound = {
                    let mut blk_align =
                        align_offset_u64(span.end, SECTOR_BLK_CNT as u64);
                    if blk_align == 0 {
                        blk_align = SECTOR_BLK_CNT as u64;
                    }
                    blk_align * BLK_SIZE as u64
                };
                if sec_offset == ubound {
                    continue;
                }
                let write_len = min(buf.len(), (ubound - sec_offset) as usize);

                // write sector data
                data_file.seek(SeekFrom::Start(sec_offset))?;
                data_file.write_all(&buf[..write_len])?;
                buf = buf.split_at(write_len).1;
                start += write_len as u64;

                // write padding if necessary
                if buf.is_empty() {
                    let padding_len =
                        BLK_SIZE - align_offset(start as usize, BLK_SIZE);
                    if padding_len != BLK_SIZE {
                        let mut padding = vec![0u8; padding_len];
                        Crypto::random_buf(&mut padding);
                        data_file.write_all(&padding)?;
                    }
                    return Ok(());
                }
            }
        }

        Ok(())
    }

    // remove block from block cache
    pub fn remove_cache(&mut self, blk_id: LocId) {
        self.lru.remove(&blk_id);
    }

    // shrink sector
    fn shrink(&self, sec: &mut Sector, curr_size: usize) -> Result<()> {
        let bk_path = sec.backup_path();
        let data_path = sec.data_path();
        let data_bk_path = sec.data_backup_path();
        vio_imp::rename(&sec.path, &bk_path)?;
        vio_imp::rename(&data_path, &data_bk_path)?;

        // open sector data and shrink file
        let mut orig_data = vio_imp::File::open(&data_bk_path)?;
        let mut dst_data = vio_imp::OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(&data_path)?;

        // copy over not deleted blocks
        let mut buf = vec![0u8; BLK_SIZE];
        let mut idx = 0;
        for blk_idx in sec.blk_map.iter_mut() {
            // skip deleted block
            if *blk_idx == BLK_DELETE_MARK {
                continue;
            }

            let data_offset = *blk_idx as u64 * BLK_SIZE as u64;
            if data_offset >= curr_size as u64 {
                break;
            }

            orig_data.seek(SeekFrom::Start(data_offset))?;
            orig_data
                .read_exact(&mut buf)
                .and(dst_data.write_all(&buf))?;

            *blk_idx = idx;
            idx += 1;
        }

        // save sector
        self.save_sec(&sec)?;

        // remove backup files
        remove_file(&bk_path)?;
        remove_file(&data_bk_path).unwrap_or(true);

        Ok(())
    }

    // restore sector from backup files if its status is incompleted
    // return true if restore is successfull and ready to recycle
    // return false if sector is fully removed
    fn restore_sec(&self, sec_id: LocId) -> Result<bool> {
        let sec = Sector::new(sec_id, self.sec_path(sec_id));
        let bk_path = sec.backup_path();
        let data_path = sec.data_path();
        let data_bk_path = sec.data_backup_path();

        if bk_path.exists() {
            if data_bk_path.exists() {
                vio_imp::rename(&data_bk_path, &data_path)?;
            }
            vio_imp::rename(&bk_path, &sec.path)?;
            Ok(true)
        } else {
            if sec.path.exists() {
                if data_bk_path.exists() {
                    remove_file(&data_bk_path)?;
                }
                Ok(true)
            } else {
                // sector is completely removed
                remove_file(&data_path)?;
                remove_file(&data_bk_path)?;
                Ok(false)
            }
        }
    }

    // recycle retired space
    pub fn recycle(&self, retired: &Vec<Space>) -> Result<()> {
        // collect each sector's retired spans
        let mut tracks: HashMap<LocId, SpanList> = HashMap::new();
        for space in retired {
            for &(sec_id, ref val) in space.divide_into_sectors().iter() {
                let spans = tracks.entry(sec_id).or_insert(SpanList::new());
                spans.join(val);
            }
        }

        // recyle spans in each sector
        for (sec_id, spans) in tracks.iter() {
            // restore sector
            if !self.restore_sec(*sec_id)? {
                continue;
            }

            // open sector files
            let mut sec = self.open_sec(*sec_id)?;
            let base_bid = sec_id.lower_blk_bound();
            let mut freed_size = 0;

            // mark blocks as deleted and sum up total size to be freed
            for span in spans.list.iter() {
                let (begin, end) = (span.begin - base_bid, span.end - base_bid);
                if sec.blk_map[begin as usize] == BLK_DELETE_MARK {
                    continue;
                }
                for i in begin..end {
                    sec.blk_map[i as usize] = BLK_DELETE_MARK;
                }
                freed_size += span.blk_len();
            }

            if freed_size == 0 {
                continue;
            }

            let curr_size = {
                let data_file = self.open_sec_data(&sec.path)?;
                data_file.metadata()?.len() as usize
            };
            let next_size = curr_size - freed_size;
            let curr_size_lvl = size_level(curr_size);
            let next_size_lvl = size_level(next_size);

            debug!(
                "recycle sector#{}.{} {}. curr: (size: {}, lv: {}), \
                 next: (size: {}, lv: {})",
                sec_id.txid,
                sec_id.idx,
                sec.path.display(),
                curr_size,
                curr_size_lvl,
                next_size,
                next_size_lvl,
            );

            // if all blocks are deleted, remove the sector
            if next_size == 0 {
                remove_file(&sec.path)?;
                remove_file(&sec.data_path())?;
            } else if next_size_lvl == curr_size_lvl {
                // if next size is still in the same size level,
                // no need to shrink, just update the sector
                let backup = sec.backup_path();
                vio_imp::rename(&sec.path, &backup)?;
                self.save_sec(&sec)?;
                remove_file(&backup)?;
            } else {
                // shrink sector
                self.shrink(&mut sec, curr_size)?;
            }
        }

        Ok(())
    }

    pub fn cleanup(&self, txid: Txid) -> Result<()> {
        let mut sec_idx = 0;
        loop {
            let sec_id = LocId::new(txid, sec_idx);
            let sec = Sector::new(sec_id, self.sec_path(sec_id));

            // if any one file is deleted successfully, then continue
            if remove_file(sec.backup_path())?
                | remove_file(sec.data_backup_path())?
                | remove_file(sec.data_path())?
                | remove_file(&sec.path)?
            {
                sec_idx += 1;
                continue;
            }

            break;
        }
        Ok(())
    }
}

impl Debug for SectorMgr {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("SectorMgr")
            .field("base", &self.base)
            .field("skey", &self.skey)
            .field("hash_key", &self.hash_key)
            .field("crypto", &self.crypto)
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use volume::storage::file::span::Span;
    use super::*;

    #[test]
    fn split_space() {
        let txid = Txid::from(42);

        // case #1, [0, 1)
        let span = Span::new(0, 1, 0);
        let spans = span.clone().into_span_list(123);
        let s = Space::new(txid, spans);
        let t = s.divide_into_sectors();
        assert_eq!(t.len(), 1);
        let &(loc_id, ref spans) = t.first().unwrap();
        assert_eq!(loc_id, LocId::new(txid, 0));
        assert_eq!(spans.len, BLK_SIZE * span.blk_cnt());
        assert_eq!(spans.offset(), 0);
        assert_eq!(*spans.list.first().unwrap(), span);

        // case #2, [0, 4096)
        let span = Span::new(0, SECTOR_BLK_CNT as u64, 0);
        let spans = span.clone().into_span_list(123);
        let s = Space::new(txid, spans);
        let t = s.divide_into_sectors();
        assert_eq!(t.len(), 1);
        let &(loc_id, ref spans) = t.first().unwrap();
        assert_eq!(loc_id, LocId::new(txid, 0));
        assert_eq!(spans.len, BLK_SIZE * span.blk_cnt());
        assert_eq!(spans.offset(), 0);
        assert_eq!(*spans.list.first().unwrap(), span);

        // case #2, [0, 4097)
        let span = Span::new(0, SECTOR_BLK_CNT as u64 + 1, 0);
        let spans = span.clone().into_span_list(123);
        let s = Space::new(txid, spans);
        let t = s.divide_into_sectors();
        assert_eq!(t.len(), 2);
        let &(loc_id, ref spans) = t.first().unwrap();
        assert_eq!(loc_id, LocId::new(txid, 0));
        assert_eq!(spans.len, BLK_SIZE * SECTOR_BLK_CNT);
        assert_eq!(spans.offset(), 0);
        assert_eq!(
            *spans.list.first().unwrap(),
            Span::new(0, SECTOR_BLK_CNT as u64, 0)
        );
        let &(loc_id, ref spans) = t.last().unwrap();
        assert_eq!(loc_id, LocId::new(txid, 1));
        assert_eq!(spans.len, BLK_SIZE);
        assert_eq!(spans.offset(), SECTOR_SIZE as u64);
        assert_eq!(
            *spans.list.first().unwrap(),
            Span::new(
                SECTOR_BLK_CNT as u64,
                SECTOR_BLK_CNT as u64 + 1,
                SECTOR_SIZE as u64,
            )
        );

        // case #3, [0, 1), [2, 3)
        let mut spans = SpanList::new();
        let span = Span::new(0, 1, 0);
        let span2 = Span::new(2, 3, span.end_offset());
        spans.append(span, span.blk_len());
        spans.append(span2, span2.blk_len());
        let s = Space::new(txid, spans);
        let t = s.divide_into_sectors();
        assert_eq!(t.len(), 1);
        let &(loc_id, ref spans) = t.first().unwrap();
        assert_eq!(loc_id, LocId::new(txid, 0));
        assert_eq!(spans.len, BLK_SIZE * 2);
        assert_eq!(spans.offset(), 0);
        assert_eq!(*spans.list.first().unwrap(), span);
        assert_eq!(*spans.list.last().unwrap(), span2);

        // case #4, [1, 2), [3, 4096)
        let mut spans = SpanList::new();
        let span = Span::new(1, 2, 42);
        let span2 = Span::new(3, SECTOR_BLK_CNT as u64, span.end_offset());
        spans.append(span, span.blk_len());
        spans.append(span2, span2.blk_len());
        let s = Space::new(txid, spans);
        let t = s.divide_into_sectors();
        assert_eq!(t.len(), 1);
        let &(loc_id, ref spans) = t.first().unwrap();
        assert_eq!(loc_id, LocId::new(txid, 0));
        assert_eq!(spans.len, span.blk_len() + span2.blk_len());
        assert_eq!(spans.offset(), 42);
        assert_eq!(*spans.list.first().unwrap(), span);
        assert_eq!(*spans.list.last().unwrap(), span2);

        // case #5, [1, 2), [3, 4098), [4100, 4101)
        let mut spans = SpanList::new();
        let span = Span::new(1, 2, 0);
        let span2 = Span::new(3, 4098, span.end_offset());
        let span3 = Span::new(4100, 4101, span2.end_offset());
        spans.append(span, span.blk_len());
        spans.append(span2, span2.blk_len());
        spans.append(span3, span3.blk_len());
        let s = Space::new(txid, spans);
        let t = s.divide_into_sectors();
        assert_eq!(t.len(), 2);
        let &(loc_id, ref spans) = t.first().unwrap();
        assert_eq!(loc_id, LocId::new(txid, 0));
        assert_eq!(spans.len, SECTOR_SIZE - BLK_SIZE * 2);
        assert_eq!(spans.offset(), 0);
        assert_eq!(*spans.list.first().unwrap(), span);
        assert_eq!(
            *spans.list.last().unwrap(),
            Span::new(3, 4096, span.end_offset())
        );
        let &(loc_id, ref spans) = t.last().unwrap();
        assert_eq!(loc_id, LocId::new(txid, 1));
        assert_eq!(spans.len, BLK_SIZE * 3);
        assert_eq!(spans.offset(), (SECTOR_SIZE - BLK_SIZE * 2) as u64);
        assert_eq!(
            *spans.list.first().unwrap(),
            Span::new(4096, 4098, (SECTOR_SIZE - BLK_SIZE * 2) as u64)
        );
        assert_eq!(
            *spans.list.last().unwrap(),
            Span::new(4100, 4101, SECTOR_SIZE as u64)
        );
    }
}
