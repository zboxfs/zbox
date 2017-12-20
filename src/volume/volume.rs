use std::sync::{Arc, RwLock};
use std::fmt::{self, Debug};
use std::path::Path;
use std::error::Error as StdError;
use std::io::{Read, Write, Error as IoError, ErrorKind, Result as IoResult};
use std::cmp::min;

use bytes::{BufMut, ByteOrder, LittleEndian};
use serde::{Deserialize, Serialize};
use rmp_serde::{Deserializer, Serializer};
use lz4::{EncoderBuilder as Lz4EncoderBuilder, Encoder as Lz4Encoder,
          Decoder as Lz4Decoder};

use error::{Error, Result};
use base::{IntoRef, Time, Version};
use base::crypto::{Crypto, Key, KEY_SIZE, Salt, SALT_SIZE, PwdHash, Cost,
                   Cipher};
use trans::{Eid, Id, Txid};
use super::storage::{Storage, FileStorage, MemStorage};

// subkey id for key derivation
const SUBKEY_ID: u64 = 42;

/// Super block
#[derive(Debug)]
struct SuperBlk {
    volume_id: Eid,
    pwd_hash: PwdHash,
    key: Key,
    crypto: Crypto,
    ver: Version,
    ctime: Time,
    payload: Vec<u8>,
}

impl SuperBlk {
    // header: salt + cost + cipher
    const HEADER_LEN: usize = SALT_SIZE + Cost::BYTES_LEN + Cipher::BYTES_LEN;

    // body: volume id + version + ctime + master key
    const BODY_LEN: usize = Eid::EID_SIZE + Version::BYTES_LEN +
        Time::BYTES_LEN + KEY_SIZE;

    fn new(
        volume_id: &Eid,
        pwd: &str,
        key: &Key,
        crypto: &Crypto,
        payload: &[u8],
    ) -> Result<SuperBlk> {
        // hash user specified plaintext password
        let pwd_hash = crypto.hash_pwd(pwd, &Salt::new())?;

        Ok(SuperBlk {
            volume_id: volume_id.clone(),
            pwd_hash,
            key: key.clone(),
            crypto: crypto.clone(),
            ver: Version::current(),
            ctime: Time::now(),
            payload: payload.to_vec(),
        })
    }

    fn serialize(&self) -> Result<Vec<u8>> {
        // encrypt body using volume key which is the user password hash
        let vkey = &self.pwd_hash.value;
        let mut body = Vec::with_capacity(SuperBlk::BODY_LEN);
        body.put(self.volume_id.as_ref());
        body.put(&self.ver.serialize()[..]);
        body.put_u64::<LittleEndian>(self.ctime.as_secs());
        body.put(self.key.as_slice());
        let enc_body = self.crypto.encrypt_with_ad(
            &body,
            vkey,
            &[Self::BODY_LEN as u8],
        )?;

        // encrypt payload using volume key
        let enc_payload = self.crypto.encrypt(&self.payload, vkey)?;

        // serialize super block
        let len = Self::HEADER_LEN + enc_body.len() + enc_payload.len();
        let mut ret = Vec::with_capacity(len);
        ret.put(self.pwd_hash.salt.as_ref());
        ret.put_u8(self.crypto.cost.to_u8());
        ret.put_u8(self.crypto.cipher.to_u8());
        ret.put(&enc_body);
        ret.put(&enc_payload);

        Ok(ret)
    }

    fn deserialize(buf: &[u8], pwd: &str) -> Result<Self> {
        if buf.len() < Self::HEADER_LEN {
            return Err(Error::InvalidSuperBlk);
        }

        // read header
        let mut pos = 0;
        let salt = Salt::from_slice(&buf[..SALT_SIZE]);
        pos += SALT_SIZE;
        let cost = Cost::from_u8(buf[pos])?;
        pos += Cost::BYTES_LEN;
        let cipher = Cipher::from_u8(buf[pos])?;
        pos += Cipher::BYTES_LEN;

        // create crypto
        let crypto = Crypto::new(cost, cipher)?;

        // read encryped body
        let enc_body_len = crypto.encrypted_len(Self::BODY_LEN);
        if (buf.len() - pos) < enc_body_len {
            return Err(Error::InvalidSuperBlk);
        }
        let body_buf = &buf[pos..pos + enc_body_len];
        pos += enc_body_len;
        let payload_buf = &buf[pos..];

        // derive volume key and use it to decrypt body
        let pwd_hash = crypto.hash_pwd(pwd, &salt)?;
        let vkey = &pwd_hash.value;
        let body = crypto.decrypt_with_ad(
            body_buf,
            vkey,
            &[Self::BODY_LEN as u8],
        )?;
        pos = Eid::EID_SIZE;
        let volume_id = Eid::from_slice(&body[..pos]);
        let ver = Version::deserialize(&body[pos..pos + Version::BYTES_LEN]);
        pos += Version::BYTES_LEN;
        let ctime =
            Time::from_secs(LittleEndian::read_u64(&body[pos..pos + 8]));
        pos += 8;
        let key = Key::from_slice(&body[pos..pos + KEY_SIZE]);

        // decrypt payload using volume key
        let payload = if payload_buf.is_empty() {
            Vec::new()
        } else {
            crypto.decrypt(payload_buf, vkey)?
        };

        Ok(SuperBlk {
            volume_id,
            pwd_hash: PwdHash::new(),
            key,
            crypto,
            ver,
            ctime,
            payload,
        })
    }
}

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
    key: Key, // master key
    crypto: Crypto,
    storage: Box<Storage + Send + Sync>,
}

impl Volume {
    /// Create volume object
    pub fn new(uri: &str) -> Result<Self> {
        let mut vol = Volume::default();

        vol.storage = if uri.starts_with("file://") {
            let path = Path::new(&uri[7..]);
            Box::new(FileStorage::new(path))
        } else if uri.starts_with("mem://") {
            Box::new(MemStorage::new())
        } else {
            return Err(Error::InvalidUri);
        };

        vol.meta.id = Eid::new();
        vol.meta.uri = uri.to_string();

        Ok(vol)
    }

    /// Check volume if it exists
    pub fn exists(uri: &str) -> Result<bool> {
        if uri.starts_with("file://") {
            let path = Path::new(&uri[7..]);
            Ok(FileStorage::new(path).exists(path.to_str().unwrap()))
        } else if uri.starts_with("mem://") {
            Ok(MemStorage::new().exists(&uri[6..]))
        } else {
            Err(Error::InvalidUri)
        }
    }

    /// Initialise volume
    pub fn init(&mut self, cost: Cost, cipher: Cipher) -> Result<()> {
        self.crypto = Crypto::new(cost, cipher)?;

        // generate random master key
        self.key = Key::new();

        // derive storage key from master key and initialise storage
        let skey = Crypto::derive_from_key(&self.key, SUBKEY_ID)?;
        self.storage.init(&self.meta.id, &self.crypto, &skey)?;

        self.meta.ver = Version::current();
        self.meta.cost = cost;
        self.meta.cipher = cipher;

        Ok(())
    }

    /// Open volume
    pub fn open(&mut self, pwd: &str) -> Result<(Txid, Vec<u8>)> {
        // read super block from storage
        let super_blk =
            SuperBlk::deserialize(&self.storage.get_super_blk()?, pwd)?;

        // check volume version if it is match
        if !super_blk.ver.match_current_minor() {
            return Err(Error::WrongVersion);
        }

        // derive storage key from master key and open storage
        let skey = Crypto::derive_from_key(&super_blk.key, SUBKEY_ID)?;
        let last_txid = self.storage.open(
            &super_blk.volume_id,
            &super_blk.crypto,
            &skey,
        )?;

        // set volume properties
        self.meta.id = super_blk.volume_id.clone();
        self.meta.ver = super_blk.ver;
        self.meta.cost = super_blk.crypto.cost;
        self.meta.cipher = super_blk.crypto.cipher;
        self.meta.ctime = super_blk.ctime;
        self.crypto = super_blk.crypto.clone();
        self.key.copy_from(&super_blk.key);

        Ok((last_txid, super_blk.payload.clone()))
    }

    /// Write super block with payload
    pub fn write_payload(&mut self, pwd: &str, payload: &[u8]) -> Result<()> {
        let super_blk = SuperBlk::new(
            &self.meta.id,
            pwd,
            &self.key,
            &self.crypto,
            payload,
        )?;
        let buf = super_blk.serialize()?;
        self.storage.put_super_blk(&buf)
    }

    /// Get volume metadata
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
        // read existing super block from storage
        let old_super_blk =
            SuperBlk::deserialize(&self.storage.get_super_blk()?, old_pwd)?;

        // create new crypto with new cost, but keep cipher as same
        let crypto = Crypto::new(cost, self.crypto.cipher)?;

        // create a new super block and save it to storage
        let new_super_blk = SuperBlk::new(
            &self.meta.id,
            new_pwd,
            &self.key,
            &crypto,
            &old_super_blk.payload,
        )?;
        let buf = new_super_blk.serialize()?;
        self.storage.put_super_blk(&buf)?;

        // update volume
        self.meta.cost = crypto.cost;
        self.crypto = crypto;

        Ok(())
    }

    /// Get volume reader
    pub fn reader(id: &Eid, txid: Txid, vol: &VolumeRef) -> Reader {
        Reader::new(id, txid, vol)
    }

    /// Get volume writer
    pub fn writer(id: &Eid, txid: Txid, vol: &VolumeRef) -> Writer {
        Writer::new(id, txid, vol)
    }

    /// Delete entity
    pub fn del(&mut self, id: &Eid, txid: Txid) -> Result<Option<Eid>> {
        self.storage.del(id, txid)
    }

    pub fn begin_trans(&mut self, txid: Txid) -> Result<()> {
        self.storage.begin_trans(txid)
    }

    pub fn abort_trans(&mut self, txid: Txid) -> Result<()> {
        self.storage.abort_trans(txid)
    }

    pub fn commit_trans(&mut self, txid: Txid) -> Result<()> {
        self.storage.commit_trans(txid)
    }
}

impl Default for Volume {
    fn default() -> Self {
        let storage = MemStorage::new();
        Volume {
            meta: Meta::default(),
            crypto: Crypto::default(),
            key: Key::new_empty(),
            storage: Box::new(storage),
        }
    }
}

impl IntoRef for Volume {}

/// Volume reference type
pub type VolumeRef = Arc<RwLock<Volume>>;

// encrypt/decrypt frame size
const CRYPT_FRAME_SIZE: usize = 32 * 1024;

/// Crypto Reader
struct CryptoReader {
    id: Eid,
    read: u64, // accumulate bytes read from underlying storage
    txid: Txid,
    vol: VolumeRef,

    // encrypted frame read from storage, max length is CRYPT_FRAME_SIZE
    frame: Vec<u8>,

    // decrypted frame, max length is encrypted_len(CRYPT_FRAME_SIZE)
    dec: Vec<u8>,
    dec_offset: usize,
    dec_len: usize,
}

impl CryptoReader {
    fn new(id: &Eid, txid: Txid, vol: &VolumeRef) -> Self {
        let decbuf_len = {
            let vol = vol.read().unwrap();
            vol.crypto.encrypted_len(CRYPT_FRAME_SIZE)
        };
        let mut ret = CryptoReader {
            id: id.clone(),
            read: 0,
            txid,
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
                let read = vol.storage.read(
                    &self.id,
                    self.read,
                    &mut self.frame[frame_offset..],
                    self.txid,
                )?;
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
    fn new(id: &Eid, txid: Txid, vol: &VolumeRef) -> Self {
        Reader {
            rdr: Lz4Decoder::new(CryptoReader::new(id, txid, vol)).unwrap(),
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
    id: Eid,
    txid: Txid,
    vol: VolumeRef,

    // encrypted frame, max length is CRYPT_FRAME_SIZE
    frame: Vec<u8>,
    frame_offset: u64,

    // source data buffer, max length is decrypted_len(CRYPT_FRAME_SIZE)
    src: Vec<u8>,
    src_written: usize,
}

impl CryptoWriter {
    fn new(id: &Eid, txid: Txid, vol: &VolumeRef) -> Self {
        let src_len = {
            let vol = vol.read().unwrap();
            vol.crypto.decrypted_len(CRYPT_FRAME_SIZE)
        };
        let mut ret = CryptoWriter {
            id: id.clone(),
            txid,
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
            vol.storage.write(
                &self.id,
                self.frame_offset,
                &self.frame[..enc_len],
                self.txid,
            )
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
    fn new(id: &Eid, txid: Txid, vol: &VolumeRef) -> Self {
        let crypto_wtr = CryptoWriter::new(id, txid, vol);
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
        let mut writer = Volume::writer(self.id(), txid, vol);
        writer.write_all(&buf)?;
        Ok(())
    }

    fn remove(id: &Eid, txid: Txid, vol: &VolumeRef) -> Result<Option<Eid>> {
        let mut vol = vol.write().unwrap();
        vol.del(id, txid)
    }
}

#[cfg(test)]
mod tests {
    extern crate tempdir;

    use self::tempdir::TempDir;

    use base::init_env;
    use base::crypto::{Crypto, RandomSeed, Hash, Cost};
    use super::*;

    fn setup_mem() -> VolumeRef {
        init_env();
        let uri = "mem://test".to_string();
        let cost = Cost::default();
        let mut vol = Volume::new(&uri).unwrap();
        vol.init(cost, Cipher::Xchacha).unwrap();
        vol.into_ref()
    }

    fn setup_file() -> (VolumeRef, TempDir) {
        init_env();
        let tmpdir = TempDir::new("zbox_test").expect("Create temp dir failed");
        let dir = tmpdir.path().to_path_buf();
        /*let dir = ::std::path::PathBuf::from("./tt");
        if dir.exists() {
            ::std::fs::remove_dir_all(&dir).unwrap();
        }*/
        let uri = "file://".to_string() + dir.to_str().unwrap();

        let cost = Cost::default();
        let mut vol = Volume::new(&uri).unwrap();
        vol.init(cost, Cipher::Xchacha).unwrap();

        (vol.into_ref(), tmpdir)
    }

    fn read_write(vol: VolumeRef) {
        // round #1
        let id = Eid::new();
        let buf = [1, 2, 3];
        {
            // write
            let txid = Txid::from(42);
            let mut wtr = Volume::writer(&id, txid, &vol);
            {
                let mut vol = vol.write().unwrap();
                vol.begin_trans(txid).unwrap();
            }
            wtr.write_all(&buf).unwrap();
            drop(wtr);
            {
                let mut vol = vol.write().unwrap();
                vol.commit_trans(txid).unwrap();
            }
        }
        {
            // read
            let txid = Txid::new_empty();
            let mut rdr = Volume::reader(&id, txid, &vol);
            let mut dst = Vec::new();
            rdr.read_to_end(&mut dst).unwrap();
            assert_eq!(&dst[..], &buf[..]);
        }

        // round #2
        let id = Eid::new();
        let buf_len = CRYPT_FRAME_SIZE + 42;
        let mut buf = vec![3u8; buf_len];
        buf[0] = 42u8;
        buf[buf_len - 1] = 42u8;
        {
            // write
            let txid = Txid::from(43);
            let mut wtr = Volume::writer(&id, txid, &vol);
            {
                let mut vol = vol.write().unwrap();
                vol.begin_trans(txid).unwrap();
            }
            wtr.write_all(&buf).unwrap();
            drop(wtr);
            {
                let mut vol = vol.write().unwrap();
                vol.commit_trans(txid).unwrap();
            }
        }
        {
            // read
            let txid = Txid::new_empty();
            let mut rdr = Volume::reader(&id, txid, &vol);
            let mut dst = Vec::new();
            rdr.read_to_end(&mut dst).unwrap();
            assert_eq!(&dst[..], &buf[..]);
        }
    }

    #[test]
    fn read_write_mem() {
        let vol = setup_mem();
        read_write(vol);
    }

    #[test]
    fn read_write_file() {
        let (vol, tmpdir) = setup_file();
        read_write(vol);
        drop(tmpdir);
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
            &mut data[pos..pos + len].copy_from_slice(
                &rnd_data[rnd_pos..rnd_pos + len],
            );
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
            &mut data[pos..pos + len].copy_from_slice(
                &rnd_data[rnd_pos..rnd_pos + len],
            );
        }

        data
    }

    #[test]
    fn fuzz_read_write() {
        let (vol, tmpdir) = setup_file();

        // setup
        // -----------
        // make random test data
        let (seed, permu, data) = make_test_data();
        //println!("seed: {:?}", seed);
        //println!("permu: {:?}", permu);
        let _ = seed;
        let _ = permu;

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
                                }
                            ),
                        ],
                        hash: Crypto::hash(buf),
                    };
                    ents.push(ent.clone());
                    {
                        let mut wtr = Volume::writer(&ent.id, txid, &vol);
                        wtr.write_all(buf).unwrap();
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
                        let mut wtr = Volume::writer(&ent.id, txid, &vol);
                        wtr.write_all(buf).unwrap();
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
                    ent.hash = Hash::new();
                    let mut v = vol.write().unwrap();
                    v.del(&ent.id, txid).unwrap();
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

        drop(tmpdir);
    }

    // this function is to reproduce the bug found during fuzz testing.
    // copy random seed, permutation list and action list to reproduce
    // the bug.
    //#[test]
    #[cfg_attr(rustfmt, rustfmt_skip)]
    #[allow(dead_code)]
    fn reproduce_bug() {
        let (vol, tmpdir) = setup_file();

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
        hash: Hash::new()  }];
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
                        let mut wtr =
                            Volume::writer(&ent.id, txid, &vol);
                        wtr.write_all(buf).unwrap();
                    }
                }
                Action::Delete => {
                    let mut v = vol.write().unwrap();
                    v.del(&ent.id, txid).unwrap();
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

        drop(tmpdir);
    }
}
