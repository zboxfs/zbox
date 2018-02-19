use std::error::Error as StdError;
use std::io::{Error as IoError, ErrorKind, Result as IoResult};
use std::collections::{HashMap, HashSet};
use std::cmp::min;

use error::{Error, Result};
use base::crypto::{Crypto, Key};
use trans::{Eid, Txid};
use volume::storage::Storage;

type Emap = HashMap<Eid, Vec<u8>>;

#[derive(Debug)]
struct Session {
    emap: Emap,
    deleted: HashSet<Eid>, // deleted entities
}

impl Session {
    fn new() -> Self {
        Session {
            emap: HashMap::new(),
            deleted: HashSet::new(),
        }
    }
}

/// Mem Storage
#[derive(Debug)]
pub struct MemStorage {
    // super block
    super_blk: Vec<u8>,

    // base entity map
    emap: Emap,

    // session map
    sessions: HashMap<Txid, Session>,

    skey: Key, // storage encryption key
    crypto: Crypto,
}

impl MemStorage {
    pub fn new() -> Self {
        MemStorage {
            super_blk: Vec::new(),
            emap: HashMap::new(),
            sessions: HashMap::new(),
            skey: Key::new_empty(),
            crypto: Crypto::default(),
        }
    }
}

impl Storage for MemStorage {
    fn exists(&self, location: &str) -> bool {
        let _ = location;
        false
    }

    fn init(
        &mut self,
        volume_id: &Eid,
        crypto: &Crypto,
        skey: &Key,
    ) -> Result<()> {
        let _ = volume_id;
        self.crypto = crypto.clone();
        self.skey = skey.clone();
        Ok(())
    }

    fn get_super_blk(&self) -> Result<Vec<u8>> {
        Ok(self.super_blk.clone())
    }

    fn put_super_blk(&mut self, super_blk: &[u8]) -> Result<()> {
        self.super_blk.clear();
        self.super_blk.extend_from_slice(super_blk);
        Ok(())
    }

    fn open(
        &mut self,
        volume_id: &Eid,
        crypto: &Crypto,
        skey: &Key,
    ) -> Result<Txid> {
        let _ = volume_id;
        self.crypto = crypto.clone();
        self.skey = skey.clone();
        Ok(Txid::new_empty())
    }

    fn read(
        &mut self,
        id: &Eid,
        offset: u64,
        buf: &mut [u8],
        txid: Txid,
    ) -> IoResult<usize> {
        fn copy_buf(buf: &mut [u8], src: &[u8], offset: u64) -> usize {
            let offset = offset as usize;
            let read_len = min(buf.len(), src.len() - offset);
            buf[..read_len].copy_from_slice(&src[offset..offset + read_len]);
            read_len
        }

        if !txid.is_empty() {
            let session =
                map_io_err!(self.sessions.get(&txid).ok_or(Error::NoTrans))?;
            if let Some(data) = session.emap.get(id) {
                return Ok(copy_buf(buf, &data, offset));
            }
        }
        match self.emap.get(id) {
            Some(data) => Ok(copy_buf(buf, &data, offset)),
            None => Err(IoError::new(
                ErrorKind::NotFound,
                Error::NoEntity.description(),
            )),
        }
    }

    fn write(
        &mut self,
        id: &Eid,
        offset: u64,
        buf: &[u8],
        txid: Txid,
    ) -> IoResult<usize> {
        let session =
            map_io_err!(self.sessions.get_mut(&txid).ok_or(Error::NoTrans))?;
        let data = session.emap.entry(id.clone()).or_insert(Vec::new());
        if offset == 0 {
            data.clear();
        }
        assert!(offset == data.len() as u64);
        data.extend_from_slice(buf);
        Ok(buf.len())
    }

    fn del(&mut self, id: &Eid, txid: Txid) -> Result<Option<Eid>> {
        let session = self.sessions.get_mut(&txid).ok_or(Error::NoTrans)?;

        if session.deleted.contains(id) {
            return Ok(None);
        }

        match session.emap.remove(id) {
            Some(_) => {
                session.deleted.insert(id.clone());
                Ok(Some(id.clone()))
            }
            None => {
                if self.emap.contains_key(id) {
                    session.deleted.insert(id.clone());
                    return Ok(Some(id.clone()));
                }
                Ok(None)
            }
        }
    }

    fn begin_trans(&mut self, txid: Txid) -> Result<()> {
        if self.sessions.contains_key(&txid) {
            return Err(Error::InTrans);
        }
        let session = Session::new();
        self.sessions.insert(txid, session);
        debug!("begin tx#{}", txid);
        Ok(())
    }

    fn abort_trans(&mut self, txid: Txid) -> Result<()> {
        debug!("abort tx#{}", txid);
        self.sessions.remove(&txid).ok_or(Error::NoTrans)?;
        debug!("tx#{} is aborted", txid);
        Ok(())
    }

    fn commit_trans(&mut self, txid: Txid) -> Result<()> {
        debug!("start commit tx#{}", txid);

        let session = self.sessions.remove(&txid).ok_or(Error::NoTrans)?;

        // merge new and updated
        for (k, v) in session.emap.iter() {
            self.emap.insert(k.clone(), v.clone());
        }

        // merge deleted
        for k in session.deleted.iter() {
            self.emap.remove(k);
        }

        debug!("tx#{} is comitted", txid);

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::time::{Duration, Instant};

    use base::init_env;
    use base::crypto::{Cipher, Cost, Crypto, RandomSeed, RANDOM_SEED_SIZE};
    use super::*;

    fn speed_str(duration: &Duration, data_len: usize) -> String {
        let secs = duration.as_secs() as f32
            + duration.subsec_nanos() as f32 / 1_000_000_000.0;
        let speed = data_len as f32 / (1024.0 * 1024.0) / secs;
        format!("{} MB/s", speed)
    }

    #[test]
    fn mem_storage_perf() {
        init_env();

        let crypto = Crypto::new(Cost::default(), Cipher::Xchacha).unwrap();
        let mut storage = MemStorage::new();
        storage.init(&Eid::new(), &crypto, &Key::new()).unwrap();

        let id = Eid::new();
        const DATA_LEN: usize = 10 * 1024 * 1024;
        let mut buf = vec![0u8; DATA_LEN];
        let seed = RandomSeed::from(&[0u8; RANDOM_SEED_SIZE]);
        Crypto::random_buf_deterministic(&mut buf, &seed);

        // write
        let now = Instant::now();
        {
            let txid = Txid::from(100);
            storage.begin_trans(txid).unwrap();
            storage.write(&id, 0, &buf, txid).unwrap();
            storage.commit_trans(txid).unwrap();
        }
        let write_time = now.elapsed();

        // read
        let now = Instant::now();
        {
            let txid = Txid::new_empty();
            storage.read(&id, 0, &mut buf, txid).unwrap();
        }
        let read_time = now.elapsed();

        println!(
            "Memory storage perf: read: {}, write: {}",
            speed_str(&read_time, DATA_LEN),
            speed_str(&write_time, DATA_LEN)
        );
    }
}
