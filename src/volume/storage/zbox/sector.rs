use std::collections::HashMap;
use std::io::{Error as IoError, ErrorKind, Read, Result as IoResult, Seek,
              SeekFrom, Write};
use std::cmp::min;

use base::crypto::{Crypto, HashKey, Key};
use trans::Txid;
use volume::storage::span::{SpanList, BLK_SIZE};
use volume::storage::space::{LocId, Space};
use super::estore::EstoreRef;

// how many blocks in a sector, must be 2^n and less than u16::MAX
pub const SECTOR_BLK_CNT: usize = 512;

// sector size, in bytes
pub const SECTOR_SIZE: usize = BLK_SIZE * SECTOR_BLK_CNT;

/// Sector
#[derive(Debug)]
struct Sector {}

impl Sector {
    pub fn new() -> Self {
        Sector {}
    }
}

/// Sector manager
#[derive(Debug)]
pub struct SectorMgr {
    // stage buffers, one per session, each buffer max size is SECTOR_SIZE
    stages: HashMap<Txid, Vec<u8>>,

    estore: EstoreRef,

    hash_key: HashKey,
    skey: Key,
    crypto: Crypto,
}

impl SectorMgr {
    pub fn new(estore: &EstoreRef) -> Self {
        SectorMgr {
            stages: HashMap::new(),
            estore: estore.clone(),
            hash_key: HashKey::new_empty(),
            skey: Key::new_empty(),
            crypto: Crypto::default(),
        }
    }

    pub fn init_stage(&mut self, txid: Txid) {
        assert!(!self.stages.contains_key(&txid));
        self.stages.insert(txid, Vec::new());
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
            .divide_into_sectors(SECTOR_BLK_CNT)
            .iter()
            .skip_while(|&&(_, ref spans)| offset < spans.offset())
        {
            for span in spans.iter().skip_while(|s| offset >= s.end_offset()) {
                let sec_str = sec_id.unique_str(&self.hash_key);
                self.estore.write().unwrap().get(buf, &sec_str, offset);
            }
        }

        Ok(read)
    }

    // write data
    pub fn write(
        &mut self,
        mut buf: &[u8],
        space: &Space,
        offset: u64,
    ) -> IoResult<()> {
        let mut stage = self.stages.get_mut(&space.txid).unwrap();
        let mut start = offset;

        for &(sec_id, ref spans) in space
            .divide_into_sectors(SECTOR_BLK_CNT)
            .iter()
            .skip_while(|&&(_, ref spans)| offset < spans.offset())
        {
            let write_len = min(buf.len(), spans.blk_len());

            // copy data to stage
            stage.copy_from_slice(&buf[..write_len]);

            // if stage is full, flush it to underling storage
            if stage.len() >= BLK_SIZE {
                let sec_str = sec_id.unique_str(&self.hash_key);
                self.estore.write().unwrap().put(&sec_str, &stage)?;
                stage.clear();
            }
        }

        Ok(())
    }

    pub fn finish_write(&mut self, txid: Txid) {}
}
