use std::sync::{Arc, RwLock};
use std::fmt::{self, Debug};
use std::path::Path;
use std::error::Error as StdError;
use std::io::{Error as IoError, ErrorKind, Read, Result as IoResult, Write};
use std::cmp::min;

use serde::{Deserialize, Serialize};
use rmp_serde::{Deserializer, Serializer};
use lz4::{Decoder as Lz4Decoder, Encoder as Lz4Encoder,
          EncoderBuilder as Lz4EncoderBuilder};

use error::{Error, Result};
use base::{IntoRef, Time, Version};
use base::crypto::{Cipher, Cost, Crypto, Key};
use trans::{Eid, Id, Loc, Txid};
use super::super_blk::SuperBlk;
use super::storage::{FileStorage, MemStorage, StorageRef};
use super::emap::Emap;
use super::txlog::TxLogMgr;

/// Volume metadata
#[derive(Debug, Default, Clone)]
pub struct Meta {
    pub id: Eid,
    pub ver: Version,
    pub uri: String,
    pub cost: Cost,
    pub cipher: Cipher,
    pub ctime: Time,
}

/// Volume
#[derive(Debug)]
pub struct Volume {
    meta: Meta,
    emap: Emap,
    txlog_mgr: TxLogMgr,
    storage: StorageRef,
    crypto: Crypto,
    key: Key,
}

impl Volume {
    // subkey ids identifiers
    const SUBKEY_ID_EMAP: u64 = 42;
    const SUBKEY_ID_TXLOG: u64 = 43;

    /// Create volume object
    pub fn new(uri: &str) -> Result<Self> {
        let mut vol = Volume::default();

        // create storage and associate it to emap and txlog manager
        vol.storage = Self::create_storage(uri)?;
        vol.emap.set_storage(&vol.storage);
        vol.txlog_mgr.set_storage(&vol.storage);

        vol.meta.id = Eid::new();
        vol.meta.uri = uri.to_string();

        Ok(vol)
    }

    /// Check specified volume if it exists
    pub fn exists(uri: &str) -> Result<bool> {
        let storage = Self::create_storage(uri)?;
        SuperBlk::exists(&storage)
    }

    // create storage specified by URI
    fn create_storage(uri: &str) -> Result<StorageRef> {
        if uri.starts_with("file://") {
            let path = Path::new(&uri[7..]);
            Ok(FileStorage::new(path).into_ref())
        } else if uri.starts_with("mem://") {
            Ok(MemStorage::new().into_ref())
        } else {
            Err(Error::InvalidUri)
        }
    }

    /// Initialise volume
    pub fn init(&mut self, cost: Cost, cipher: Cipher) -> Result<()> {
        // create crypto and generate random master key
        self.crypto = Crypto::new(cost, cipher)?;
        self.key = Key::new();

        self.meta.ver = Version::current();
        self.meta.cost = cost;
        self.meta.cipher = cipher;

        // set crypto context for emap and txlog manager
        self.emap
            .set_crypto_ctx(&self.crypto, &self.key, Self::SUBKEY_ID_EMAP);
        self.txlog_mgr.set_crypto_ctx(
            &self.crypto,
            &self.key,
            Self::SUBKEY_ID_TXLOG,
        );

        // initialise txlog manager
        self.txlog_mgr.init();

        Ok(())
    }

    /// Open volume, return last txid and super block payload
    pub fn open(&mut self, pwd: &str) -> Result<(Txid, Vec<u8>)> {
        // load super block from storage
        let super_blk = SuperBlk::load(pwd, &self.storage)?;

        // check volume version
        if !super_blk.ver.match_current_minor() {
            return Err(Error::WrongVersion);
        }

        // set volume properties
        self.meta.id = super_blk.volume_id.clone();
        self.meta.ver = super_blk.ver;
        self.meta.cost = super_blk.crypto.cost;
        self.meta.cipher = super_blk.crypto.cipher;
        self.meta.ctime = super_blk.ctime;
        self.crypto = super_blk.crypto.clone();
        self.key.copy_from(&super_blk.key);

        // set emap and txlog manager crypto context
        self.emap
            .set_crypto_ctx(&self.crypto, &self.key, Self::SUBKEY_ID_EMAP);
        self.txlog_mgr.set_crypto_ctx(
            &self.crypto,
            &self.key,
            Self::SUBKEY_ID_TXLOG,
        );

        // open txlog manager
        let txid_wm = self.txlog_mgr.open()?;

        // redo aborting uncompleted trans if any
        self.redo_abort_trans()?;

        Ok((txid_wm, super_blk.payload.clone()))
    }

    /// Write super block with initial payload
    pub fn init_payload(&mut self, pwd: &str, payload: &[u8]) -> Result<()> {
        let mut super_blk =
            SuperBlk::new(&self.meta.id, &self.key, &self.crypto, payload)?;
        self.meta.ctime = super_blk.ctime;
        super_blk.save(pwd, &self.storage)
    }

    /// Get volume metadata
    #[inline]
    pub fn meta(&self) -> Meta {
        self.meta.clone()
    }

    /// Reset volume password
    pub fn reset_password(
        &mut self,
        old_pwd: &str,
        new_pwd: &str,
        cost: Cost,
    ) -> Result<()> {
        // load current super block from storage
        let mut super_blk = SuperBlk::load(old_pwd, &self.storage)?;

        // create new crypto with new cost, but keep cipher as same
        super_blk.crypto = Crypto::new(cost, self.crypto.cipher)?;
        super_blk.save(new_pwd, &self.storage)?;

        // update volume cost and crypto
        self.meta.cost = super_blk.crypto.cost;
        self.crypto = super_blk.crypto.clone();

        Ok(())
    }

    /// Get volume reader
    pub fn reader(id: &Eid, txid: Txid, vol: &VolumeRef) -> Reader {
        Reader::new(Loc::new(id, txid), vol)
    }

    // read entity data
    fn read(
        &mut self,
        dst: &mut [u8],
        loc: &Loc,
        offset: u64,
    ) -> Result<usize> {
        let cell = self.emap.get(loc)?;
        let mut storage = self.storage.write().unwrap();
        let loc_id = Loc::new(&loc.eid, cell.txid).id();
        storage.get(dst, &loc_id, offset)
    }

    // write entity data
    fn write(&mut self, loc: &Loc, buf: &[u8], offset: u64) -> Result<usize> {
        match self.emap.get(loc) {
            Ok(cell) => {
                // add an update txlog entry
                if cell.txid < loc.txid {
                    assert_eq!(offset, 0);
                    self.txlog_mgr.add_update_entry(
                        loc,
                        cell.txid,
                        cell.pre_txid,
                    )?;
                }
            }
            Err(ref err) if *err == Error::NotFound => {
                // add a new txlog entry
                assert_eq!(offset, 0);
                self.txlog_mgr.add_new_entry(loc)?;
            }
            Err(err) => return Err(err),
        }

        // write entity data to storage
        let written = {
            let mut storage = self.storage.write().unwrap();
            storage.put(&loc.id(), buf, offset)?
        };

        // update emap
        self.emap.put(&loc)?;

        Ok(written)
    }

    /// Delete entity
    pub fn del(&mut self, loc: &Loc) -> Result<Option<Eid>> {
        match self.emap.get(loc) {
            Ok(cell) => {
                // add a delete txlog entry
                self.txlog_mgr
                    .add_delete_entry(loc, cell.txid, cell.pre_txid)?;
            }
            Err(ref err) if *err == Error::NotFound => return Ok(None),
            Err(err) => return Err(err),
        }

        self.emap.del(loc)?;
        Ok(Some(loc.eid.clone()))
    }

    pub fn begin_trans(&mut self, txid: Txid) -> Result<()> {
        debug!("begin tx#{}", txid);

        // redo abort all uncompleted trans if any
        self.redo_abort_trans()?;

        self.txlog_mgr.start_log(txid)
    }

    pub fn commit_trans(&mut self, txid: Txid) -> Result<()> {
        debug!("start committing tx#{}", txid);
        {
            let txlog = self.txlog_mgr.active_logs().get(&txid).unwrap();
            for ent in txlog.iter() {
                // commit emap
                self.emap.commit(&Loc::new(ent.id(), txid))?;
            }
        }

        // recyle trans
        let to_removed = self.txlog_mgr.recycle()?;
        for id in to_removed {
            self.emap.remove(&id)?;
        }

        // commit txlog
        self.txlog_mgr.commit(txid)?;
        debug!("tx#{} is committed", txid);

        Ok(())
    }

    // redo abort all uncompleted trans
    fn redo_abort_trans(&mut self) -> Result<()> {
        let uncompleted: Vec<Txid> =
            self.txlog_mgr.inactive_logs().keys().map(|t| *t).collect();
        if !uncompleted.is_empty() {
            debug!("found {} uncompleted trans", uncompleted.len());
            for txid in uncompleted {
                debug!("redo abort tx#{}", txid);
                self.abort_trans(txid)?;
            }
        }
        Ok(())
    }

    pub fn abort_trans(&mut self, txid: Txid) -> Result<()> {
        debug!("abort tx#{}", txid);
        self.txlog_mgr.deactivate(txid);

        // abort emap
        {
            let txlog = self.txlog_mgr.inactive_logs().get(&txid).unwrap();
            for ent in txlog.iter() {
                self.emap.abort(&Loc::new(ent.id(), txid))?;
            }
        }

        // abort txlog
        self.txlog_mgr.abort(txid)
    }
}

impl Default for Volume {
    fn default() -> Self {
        let storage = MemStorage::new().into_ref();
        Volume {
            meta: Meta::default(),
            emap: Emap::new(storage.clone()),
            txlog_mgr: TxLogMgr::new(storage.clone()),
            storage,
            crypto: Crypto::default(),
            key: Key::new_empty(),
        }
    }
}

impl IntoRef for Volume {}

/// Volume reference type
pub type VolumeRef = Arc<RwLock<Volume>>;

// encrypt/decrypt frame buffer size
const CRYPT_FRAME_SIZE: usize = 64 * 1024;

/// Crypto Reader
struct CryptoReader {
    loc: Loc,
    read: u64, // accumulate bytes read from underlying storage
    vol: VolumeRef,

    // encrypted frame read from storage, max length is CRYPT_FRAME_SIZE
    frame: Vec<u8>,

    // decrypted frame, max length is encrypted_len(CRYPT_FRAME_SIZE)
    dec: Vec<u8>,
    dec_offset: usize,
    dec_len: usize,
}

impl CryptoReader {
    fn new(loc: Loc, vol: &VolumeRef) -> Self {
        let decbuf_len = {
            let vol = vol.read().unwrap();
            vol.crypto.encrypted_len(CRYPT_FRAME_SIZE)
        };
        let mut ret = CryptoReader {
            loc,
            read: 0,
            vol: vol.clone(),
            frame: vec![0u8; CRYPT_FRAME_SIZE],
            dec: vec![0u8; decbuf_len],
            dec_offset: 0,
            dec_len: 0,
        };
        ret.frame.shrink_to_fit();
        ret.dec.shrink_to_fit();
        ret
    }

    // copy decrypted data to destination
    fn copy_dec_to(&mut self, dst: &mut [u8]) -> usize {
        let copy_len = min(self.dec_len - self.dec_offset, dst.len());
        dst[..copy_len].copy_from_slice(
            &self.dec[self.dec_offset..self.dec_offset + copy_len],
        );
        self.dec_offset += copy_len;
        if self.dec_offset >= self.dec_len {
            // frame has been exhausted, advance to next frame
            self.dec_offset = 0;
            self.dec_len = 0;
        }
        copy_len
    }
}

impl Read for CryptoReader {
    fn read(&mut self, buf: &mut [u8]) -> IoResult<usize> {
        let mut copied = 0;
        if (self.dec_len - self.dec_offset) > 0 {
            copied = self.copy_dec_to(buf);
            if copied >= buf.len() {
                return Ok(copied);
            }
        }

        // read a frame
        let vol = self.vol.clone();
        let mut vol = vol.write().unwrap();
        let mut frame_offset = 0;
        {
            loop {
                let read = map_io_err!(vol.read(
                    &mut self.frame[frame_offset..],
                    &self.loc,
                    self.read,
                ))?;
                frame_offset += read;
                self.read += read as u64;
                if read == 0 || frame_offset >= CRYPT_FRAME_SIZE {
                    break;
                }
            }
        }
        if frame_offset == 0 {
            return Ok(copied);
        }

        // decrypt frame
        assert_eq!(self.dec_offset, 0);
        assert_eq!(self.dec_len, 0);
        self.dec_len = map_io_err!(vol.crypto.decrypt_to(
            &mut self.dec,
            &self.frame[..frame_offset as usize],
            &vol.key,
        ))?;

        // copy decrypted data to destination
        copied += self.copy_dec_to(&mut buf[copied..]);
        Ok(copied)
    }
}

/// Volume Reader
pub struct Reader {
    rdr: Lz4Decoder<CryptoReader>,
}

impl Reader {
    fn new(loc: Loc, vol: &VolumeRef) -> Self {
        Reader {
            rdr: Lz4Decoder::new(CryptoReader::new(loc, vol)).unwrap(),
        }
    }
}

impl Read for Reader {
    fn read(&mut self, buf: &mut [u8]) -> IoResult<usize> {
        self.rdr.read(buf)
    }
}

/// Crypto Writer
struct CryptoWriter {
    loc: Loc,
    vol: VolumeRef,

    // encrypted frame, max length is CRYPT_FRAME_SIZE
    frame: Vec<u8>,
    frame_offset: u64,

    // source data buffer, max length is decrypted_len(CRYPT_FRAME_SIZE)
    src: Vec<u8>,
    src_written: usize,
}

impl CryptoWriter {
    fn new(loc: Loc, vol: &VolumeRef) -> Self {
        let src_len = {
            let vol = vol.read().unwrap();
            vol.crypto.decrypted_len(CRYPT_FRAME_SIZE)
        };
        let mut ret = CryptoWriter {
            loc,
            vol: vol.clone(),
            frame: vec![0u8; CRYPT_FRAME_SIZE],
            frame_offset: 0,
            src: vec![0u8; src_len],
            src_written: 0,
        };
        ret.frame.shrink_to_fit();
        ret.src.shrink_to_fit();
        ret
    }
}

impl Write for CryptoWriter {
    fn write(&mut self, buf: &[u8]) -> IoResult<usize> {
        let copy_len = min(self.src.len() - self.src_written, buf.len());
        self.src[self.src_written..self.src_written + copy_len]
            .copy_from_slice(&buf[..copy_len]);
        self.src_written += copy_len;
        if self.src_written >= self.src.len() {
            // source buffer is full, need to flush to storage
            self.flush()?;
            self.src_written = 0;
        }
        Ok(copy_len)
    }

    fn flush(&mut self) -> IoResult<()> {
        if self.src_written == 0 {
            return Ok(());
        }

        let vol = self.vol.clone();
        let mut vol = vol.write().unwrap();
        map_io_err!(vol.crypto.encrypt_to(
            &mut self.frame,
            &self.src[..self.src_written],
            &vol.key,
        )).and_then(|enc_len| {
            map_io_err!(vol.write(
                &self.loc,
                &self.frame[..enc_len],
                self.frame_offset,
            ))
        })
            .and_then(|written| {
                self.frame_offset += written as u64;
                Ok(())
            })
    }
}

/// Volume writer
pub struct Writer {
    wtr: Option<Lz4Encoder<CryptoWriter>>,
}

impl Writer {
    pub fn new(id: &Eid, txid: Txid, vol: &VolumeRef) -> Self {
        let loc = Loc::new(id, txid);
        let crypto_wtr = CryptoWriter::new(loc, vol);
        Writer {
            wtr: Some(
                Lz4EncoderBuilder::new()
                    .level(0)
                    .auto_flush(true)
                    .build(crypto_wtr)
                    .unwrap(),
            ),
        }
    }
}

impl Write for Writer {
    fn write(&mut self, buf: &[u8]) -> IoResult<usize> {
        match self.wtr {
            Some(ref mut wtr) => wtr.write(buf),
            None => unreachable!(),
        }
    }

    fn flush(&mut self) -> IoResult<()> {
        match self.wtr {
            Some(ref mut wtr) => wtr.flush(),
            None => unreachable!(),
        }
    }
}

impl Debug for Writer {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "VolumeWriter()")
    }
}

impl Drop for Writer {
    fn drop(&mut self) {
        let wtr = self.wtr.take().unwrap();
        let (mut crypto_wtr, result) = wtr.finish();
        result.unwrap();
        crypto_wtr.flush().unwrap();
    }
}

/// Trait for entity which can be persisted to volume
pub trait Persistable<'de>: Id + Deserialize<'de> + Serialize {
    fn load(id: &Eid, txid: Txid, vol: &VolumeRef) -> Result<Self> {
        let mut buf = Vec::new();
        let read = Volume::reader(id, txid, vol).read_to_end(&mut buf)?;
        let mut de = Deserializer::new(&buf[..read]);
        let ret: Self = Deserialize::deserialize(&mut de)?;
        Ok(ret)
    }

    fn save(&self, txid: Txid, vol: &VolumeRef) -> Result<()> {
        let mut buf = Vec::new();
        self.serialize(&mut Serializer::new(&mut buf))?;
        let mut writer = Writer::new(self.id(), txid, vol);
        writer.write_all(&buf)?;
        Ok(())
    }

    fn remove(id: &Eid, txid: Txid, vol: &VolumeRef) -> Result<Option<Eid>> {
        let mut vol = vol.write().unwrap();
        vol.del(&Loc::new(id, txid))
    }
}

#[cfg(test)]
mod tests2 {
    use std::{thread, time};
    use std::time::{Duration, Instant};

    use base::init_env;
    use base::crypto::{Cost, Crypto, Hash, RandomSeed, RANDOM_SEED_SIZE};
    use super::*;

    fn setup_mem_vol() -> VolumeRef {
        init_env();
        let uri = "mem://test".to_string();
        let cost = Cost::default();
        let mut vol = Volume::new(&uri).unwrap();
        vol.init(cost, Cipher::Xchacha).unwrap();
        vol.into_ref()
    }

    fn write_to_entity(id: &Eid, buf: &[u8], txid: Txid, vol: &VolumeRef) {
        {
            let mut vol = vol.write().unwrap();
            vol.begin_trans(txid).unwrap();
        }
        {
            let mut wtr = Writer::new(&id, txid, &vol);
            wtr.write_all(&buf).unwrap();
        }
        {
            let mut vol = vol.write().unwrap();
            vol.commit_trans(txid).unwrap();
        }
    }

    fn verify_entity_in_tx(id: &Eid, buf: &[u8], txid: Txid, vol: &VolumeRef) {
        let loc = Loc::new(&id, txid);
        let mut rdr = Reader::new(loc, &vol);
        let mut dst = Vec::new();
        rdr.read_to_end(&mut dst).unwrap();
        assert_eq!(&dst[..], &buf[..]);
    }

    fn verify_entity(id: &Eid, buf: &[u8], vol: &VolumeRef) {
        verify_entity_in_tx(id, buf, Txid::new_empty(), vol);
    }

    #[test]
    fn read_write() {
        let vol = setup_mem_vol();
        let id = Eid::new();
        let buf = [1, 2, 3];
        let buf2 = [4, 5, 6];

        // tx #1, new
        write_to_entity(&id, &buf, Txid::from(1), &vol);
        verify_entity(&id, &buf, &vol);

        // tx #2, update
        write_to_entity(&id, &buf2, Txid::from(2), &vol);
        verify_entity(&id, &buf2, &vol);

        // delete
        {
            let txid = Txid::from(3);
            let loc = Loc::new(&id, txid);
            let mut vol = vol.write().unwrap();
            vol.begin_trans(txid).unwrap();
            vol.del(&loc).unwrap();
            vol.commit_trans(txid).unwrap();
            let loc = Loc::new(&id, Txid::from(0));
            assert_eq!(vol.emap.get(&loc).unwrap_err(), Error::NotFound);
        }
    }

    #[test]
    fn abort_trans_on_failure() {
        let vol = setup_mem_vol();
        let id = Eid::new();
        let buf = [1, 2, 3];
        let buf2 = [4, 5, 6];

        // tx #1, new
        write_to_entity(&id, &buf, Txid::from(1), &vol);

        // tx #2, update and then abort
        {
            let txid = Txid::from(2);
            {
                let mut vol = vol.write().unwrap();
                vol.begin_trans(txid).unwrap();
            }
            {
                let mut wtr = Writer::new(&id, txid, &vol);
                wtr.write_all(&buf2).unwrap();
            }

            {
                let mut vol = vol.write().unwrap();
                vol.abort_trans(txid).unwrap();
            }

            verify_entity(&id, &buf, &vol);
        }

        // tx #3, update again
        write_to_entity(&id, &buf2, Txid::from(3), &vol);
        verify_entity(&id, &buf2, &vol);
    }

    #[test]
    fn multi_thread_emap_mask() {
        let vol = setup_mem_vol();
        let id = Eid::new();
        let buf = [1, 2, 3];
        let buf2 = [4, 5, 6];
        let mut children = vec![];

        // tx #1, new
        write_to_entity(&id, &buf, Txid::from(1), &vol);

        // thread #1, tx #2, update but don't commit
        {
            let id = id.clone();
            let vol = vol.clone();
            children.push(thread::spawn(move || {
                let txid = Txid::from(2);
                {
                    let mut vol = vol.write().unwrap();
                    vol.begin_trans(txid).unwrap();
                }
                {
                    let mut wtr = Writer::new(&id, txid, &vol);
                    wtr.write_all(&buf2).unwrap();
                }
                verify_entity_in_tx(&id, &buf2, txid, &vol);
            }));
        }

        thread::sleep(time::Duration::from_millis(200));

        // thread #2, read
        {
            let id = id.clone();
            let vol = vol.clone();
            children.push(thread::spawn(move || {
                verify_entity(&id, &buf, &vol);
            }));
        }

        for child in children {
            let _ = child.join();
        }
    }

    fn speed_str(duration: &Duration, data_len: usize) -> String {
        let secs = duration.as_secs() as f32
            + duration.subsec_nanos() as f32 / 1_000_000_000.0;
        let speed = data_len as f32 / (1024.0 * 1024.0) / secs;
        format!("{} MB/s", speed)
    }

    fn print_result(
        prefix: &str,
        read_time: &Duration,
        write_time: &Duration,
        data_len: usize,
    ) {
        println!(
            "{} perf: read: {}, write: {}",
            prefix,
            speed_str(&read_time, data_len),
            speed_str(&write_time, data_len)
        );
    }

    fn read_write_perf(vol: &VolumeRef, prefix: &str) {
        const DATA_LEN: usize = 10 * 1024 * 1024;
        let id = Eid::new();
        let mut buf = vec![0u8; DATA_LEN];
        let seed = RandomSeed::from(&[0u8; RANDOM_SEED_SIZE]);
        Crypto::random_buf_deterministic(&mut buf, &seed);

        // write
        let now = Instant::now();
        write_to_entity(&id, &buf, Txid::from(1), &vol);
        let write_time = now.elapsed();

        // read
        let now = Instant::now();
        {
            let txid = Txid::new_empty();
            let mut rdr = Volume::reader(&id, txid, &vol);
            let mut dst = Vec::new();
            rdr.read_to_end(&mut dst).unwrap();
        }
        let read_time = now.elapsed();

        print_result(prefix, &read_time, &write_time, DATA_LEN);
    }

    #[test]
    fn mem_storage_perf() {
        let vol = setup_mem_vol();
        read_write_perf(&vol, "Volume (memory storage)");
    }

    const RND_DATA_LEN: usize = 4 * 1024 * 1024;
    const DATA_LEN: usize = 2 * RND_DATA_LEN;

    #[derive(Debug, Clone)]
    enum Action {
        New,
        Update,
        Delete,
    }

    #[derive(Debug, Clone)]
    struct Span {
        pos: usize,
        len: usize,
    }

    #[derive(Debug, Clone)]
    struct Entry {
        id: Eid,
        acts: Vec<(Action, u64, Span)>,
        hash: Hash,
    }

    // pick a random action
    fn random_action() -> Action {
        match Crypto::random_u32(3) {
            0 => Action::New,
            1 => Action::Update,
            2 => Action::Delete,
            _ => unreachable!(),
        }
    }

    // pick a random entry
    fn random_ent(ents: &mut Vec<Entry>) -> &mut Entry {
        let idx = Crypto::random_usize() % ents.len();
        &mut ents[idx]
    }

    fn random_slice(vec: &Vec<u8>) -> (usize, &[u8]) {
        let pos = Crypto::random_usize() % vec.len();
        let len = Crypto::random_usize() % (vec.len() - pos);
        (pos, &vec[pos..(pos + len)])
    }

    // make random test data
    // return: (random seed, permutation sequence, data buffer)
    // item in permutation sequence:
    //   (span in random data buffer, position in data buffer)
    fn make_test_data() -> (RandomSeed, Vec<(Span, usize)>, Vec<u8>) {
        // init random data buffer
        let mut rnd_data = vec![0u8; RND_DATA_LEN];
        let seed = RandomSeed::new();
        Crypto::random_buf_deterministic(&mut rnd_data, &seed);

        // init data buffer
        let mut data = vec![0u8; DATA_LEN];
        let mut permu = Vec::new();
        for _ in 0..5 {
            let pos = Crypto::random_usize() % DATA_LEN;
            let rnd_pos = Crypto::random_usize() % RND_DATA_LEN;
            let max_len = min(DATA_LEN - pos, RND_DATA_LEN - rnd_pos);
            let len = Crypto::random_u32(max_len as u32) as usize;
            permu.push((Span { pos: rnd_pos, len }, pos));
            &mut data[pos..pos + len]
                .copy_from_slice(&rnd_data[rnd_pos..rnd_pos + len]);
        }

        (seed, permu, data)
    }

    // reproduce test data
    fn reprod_test_data(
        seed: RandomSeed,
        permu: Vec<(Span, usize)>,
    ) -> Vec<u8> {
        // init random data buffer
        let mut rnd_data = vec![0u8; RND_DATA_LEN];
        Crypto::random_buf_deterministic(&mut rnd_data, &seed);

        // init data buffer
        let mut data = vec![0u8; DATA_LEN];
        for opr in permu {
            let pos = opr.1;
            let rnd_pos = opr.0.pos;
            let len = opr.0.len;
            &mut data[pos..pos + len]
                .copy_from_slice(&rnd_data[rnd_pos..rnd_pos + len]);
        }

        data
    }

    #[test]
    fn fuzz_read_write() {
        let vol = setup_mem_vol();

        // setup
        // -----------
        // make random test data
        let (_seed, _permu, data) = make_test_data();
        //println!("seed: {:?}", _seed);
        //println!("permu: {:?}", _permu);

        // init test entry list
        let mut ents: Vec<Entry> = Vec::new();

        // start fuzz rounds
        // ------------------
        let rounds = 10;
        for round in 0..rounds {
            let txid = Txid::from(round);
            {
                let mut vol = vol.write().unwrap();
                vol.begin_trans(txid).unwrap();
            }

            let act = random_action();
            match act {
                Action::New => {
                    let (pos, buf) = random_slice(&data);
                    let ent = Entry {
                        id: Eid::new(),
                        acts: vec![
                            (
                                act,
                                round,
                                Span {
                                    pos,
                                    len: buf.len(),
                                },
                            ),
                        ],
                        hash: Crypto::hash(buf),
                    };
                    ents.push(ent.clone());
                    {
                        let mut wtr = Writer::new(&ent.id, txid, &vol);
                        wtr.write_all(&buf).unwrap();
                    }
                }
                Action::Update => {
                    if ents.is_empty() {
                        let mut vol = vol.write().unwrap();
                        vol.abort_trans(txid).unwrap();
                        continue;
                    }
                    let ent = random_ent(&mut ents);
                    let (pos, buf) = random_slice(&data);
                    ent.acts.push((
                        act,
                        round,
                        Span {
                            pos,
                            len: buf.len(),
                        },
                    ));
                    ent.hash = Crypto::hash(buf);
                    {
                        let mut wtr = Writer::new(&ent.id, txid, &vol);
                        wtr.write_all(&buf).unwrap();
                    }
                }
                Action::Delete => {
                    if ents.is_empty() {
                        let mut vol = vol.write().unwrap();
                        vol.abort_trans(txid).unwrap();
                        continue;
                    }
                    let ent = random_ent(&mut ents);
                    ent.acts.push((act, round, Span { pos: 0, len: 0 }));
                    ent.hash = Hash::new_empty();
                    let mut v = vol.write().unwrap();
                    v.del(&Loc::new(&ent.id, txid)).unwrap();
                }
            }

            {
                let mut vol = vol.write().unwrap();
                vol.commit_trans(txid).unwrap();
            }
        }

        // verify
        // ------------------
        //println!("ents: {:?}", ents);
        let txid = Txid::new_empty();
        for ent in ents {
            let (ref last_act, _, _) = *ent.acts.last().unwrap();
            match *last_act {
                Action::New | Action::Update => {
                    let mut rdr = Volume::reader(&ent.id, txid, &vol);
                    let mut dst = Vec::new();
                    let read = rdr.read_to_end(&mut dst).unwrap();
                    assert_eq!(read, dst.len());
                    let hash = Crypto::hash(&dst);
                    assert_eq!(&ent.hash, &hash);
                }
                Action::Delete => {
                    let mut rdr = Volume::reader(&ent.id, txid, &vol);
                    let mut dst = Vec::new();
                    assert!(rdr.read_to_end(&mut dst).is_err());
                }
            }
        }
    }

    // this function is to reproduce the bug found during fuzz testing.
    // copy random seed, permutation list and action list to reproduce
    // the bug.
    //#[test]
    #[cfg_attr(rustfmt, rustfmt_skip)]
    #[allow(dead_code)]
    fn reproduce_bug() {
        let vol = setup_mem_vol();

        // reproduce random data buffer
        let seed = RandomSeed::from(
            &[215, 143, 220, 115, 128, 115, 81, 12, 25, 79, 170, 253, 93, 52,
            196, 20, 69, 75, 173, 154, 105, 48, 129, 115, 152, 58, 252, 31,
            39, 65, 16, 8],
        );
        let permu = vec![(Span { pos: 2418638, len: 1094344  }, 6050331),
        (Span { pos: 984012, len: 1456744  }, 5046992),
        (Span { pos: 669298, len: 250308  }, 3122817),
        (Span { pos: 828089, len: 2265  }, 8383468),
        (Span { pos: 1568637, len: 1092468  }, 4202651)];
        let data = reprod_test_data(seed, permu);

        // entities and actions
        let mut ents = vec![Entry { id: Eid::new(),
        acts: vec![(Action::New, 2, Span { pos: 6441525, len: 69696  })],
        hash: Hash::new_empty()  }];
        let mut rounds: Vec<u64> = ents.iter()
            .flat_map(|e| e.acts.iter().map(|a| a.1))
            .collect();
        rounds.sort();

        // repeat rounds
        for round in rounds {
            let ent = ents.iter_mut()
                .find(|e| e.acts.iter().find(|a| a.1 == round).is_some())
                .unwrap();
            let act = ent.acts.iter().find(|a| a.1 == round).unwrap().clone();

            let txid = Txid::from(round);
            {
                let mut vol = vol.write().unwrap();
                vol.begin_trans(txid).unwrap();
            }

            match act.0 {
                Action::New | Action::Update => {
                    let Span { pos, len } = act.2;
                    let buf = &data[pos..pos + len];
                    ent.hash = Crypto::hash(buf);
                    {
                        let mut wtr = Writer::new(&ent.id, txid, &vol);
                        wtr.write_all(&buf).unwrap();
                    }
                }
                Action::Delete => {
                    let mut v = vol.write().unwrap();
                    v.del(&Loc::new(&ent.id, txid)).unwrap();
                }
            }

            {
                let mut vol = vol.write().unwrap();
                vol.commit_trans(txid).unwrap();
            }
        }

        // verify
        // ------------------
        let txid = Txid::new_empty();
        for ent in ents {
            let (ref last_act, _, _) = *ent.acts.last().unwrap();
            match *last_act {
                Action::New | Action::Update => {
                    let mut rdr = Volume::reader(&ent.id, txid, &vol);
                    let mut dst = Vec::new();
                    let read = rdr.read_to_end(&mut dst).unwrap();
                    assert_eq!(read, dst.len());
                    let hash = Crypto::hash(&dst);
                    assert_eq!(&ent.hash, &hash);
                }
                Action::Delete => {
                    let mut rdr = Volume::reader(&ent.id, txid, &vol);
                    let mut dst = Vec::new();
                    assert!(rdr.read_to_end(&mut dst).is_err());
                }
            }
        }
    }
}
