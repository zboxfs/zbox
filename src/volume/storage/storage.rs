use std::cmp::min;
use std::fmt::{self, Debug, Display};
use std::io::{Error as IoError, ErrorKind, Read, Result as IoResult, Write};
use std::sync::{Arc, RwLock, Weak};

use rmp_serde::{Deserializer, Serializer};
use serde::{Deserialize, Serialize};

use super::{DummyStorage, Storable};
use base::crypto::{Cipher, Cost, Crypto, Key};
use base::lru::{CountMeter, Lru, Meter, PinChecker};
use base::utils::align_ceil_chunk;
use base::IntoRef;
use error::{Error, Result};
use trans::{Eid, Finish};
use volume::address::Addr;
use volume::{Allocator, AllocatorRef, BLKS_PER_FRAME, BLK_SIZE, FRAME_SIZE};

// parse storage part in uri
fn parse_uri(uri: &str) -> Result<Box<dyn Storable>> {
    if !uri.is_ascii() {
        return Err(Error::InvalidUri);
    }

    // extract storage string
    let idx = uri.find("://").ok_or(Error::InvalidUri)?;
    let loc = &uri[idx + 3..];
    if loc.is_empty() {
        return Err(Error::InvalidUri);
    }
    let storage_type = &uri[..idx];

    match storage_type {
        "mem" => {
            #[cfg(feature = "storage-mem")]
            {
                Ok(Box::new(super::mem::MemStorage::new(loc)))
            }
            #[cfg(not(feature = "storage-mem"))]
            {
                Err(Error::InvalidUri)
            }
        }
        "file" => {
            #[cfg(feature = "storage-file")]
            {
                let path = std::path::Path::new(loc);
                let depot = super::file::FileStorage::new(path);
                Ok(Box::new(depot))
            }
            #[cfg(not(feature = "storage-file"))]
            {
                Err(Error::InvalidUri)
            }
        }
        "sqlite" => {
            #[cfg(feature = "storage-sqlite")]
            {
                let depot = super::sqlite::SqliteStorage::new(loc);
                Ok(Box::new(depot))
            }
            #[cfg(not(feature = "storage-sqlite"))]
            {
                Err(Error::InvalidUri)
            }
        }
        "redis" => {
            #[cfg(feature = "storage-redis")]
            {
                let depot = super::redis::RedisStorage::new(loc)?;
                Ok(Box::new(depot))
            }
            #[cfg(not(feature = "storage-redis"))]
            {
                Err(Error::InvalidUri)
            }
        }
        "faulty" => {
            #[cfg(feature = "storage-faulty")]
            {
                let depot = super::faulty::FaultyStorage::new(loc);
                Ok(Box::new(depot))
            }
            #[cfg(not(feature = "storage-faulty"))]
            {
                Err(Error::InvalidUri)
            }
        }
        "zbox" => {
            #[cfg(feature = "storage-zbox")]
            {
                let depot = super::zbox::ZboxStorage::new(loc)?;
                Ok(Box::new(depot))
            }
            #[cfg(not(feature = "storage-zbox"))]
            {
                Err(Error::InvalidUri)
            }
        }
        _ => Err(Error::InvalidUri),
    }
}

// frame cache meter, measured by frame byte size
#[derive(Debug, Default)]
struct FrameCacheMeter;

impl Meter<Vec<u8>> for FrameCacheMeter {
    #[inline]
    fn measure(&self, item: &Vec<u8>) -> isize {
        item.len() as isize
    }
}

/// Storage
pub struct Storage {
    // underlying storage layer
    depot: Box<dyn Storable>,

    // block allocator
    allocator: AllocatorRef,

    // crypto context
    crypto: Crypto,
    key: Key,

    // decrypted frame cache, key is the begin block index
    frame_cache: Lru<usize, Vec<u8>, FrameCacheMeter, PinChecker<Vec<u8>>>,

    // entity address cache
    addr_cache: Lru<Eid, Addr, CountMeter<Addr>, PinChecker<Addr>>,
}

impl Storage {
    // frame cache size, in bytes
    const FRAME_CACHE_SIZE: usize = 4 * 1024 * 1024;

    // frame cache threshold size, in bytes
    // if the entity size is larger than this, its frames won't be
    // put in frame cache
    const FRAME_CACHE_THRESHOLD: usize = 512 * 1024;

    // address cache size
    const ADDRESS_CACHE_SIZE: usize = 64;

    pub fn new(uri: &str) -> Result<Self> {
        let depot = parse_uri(uri)?;
        let frame_cache = Lru::new(Self::FRAME_CACHE_SIZE);

        Ok(Storage {
            depot,
            allocator: Allocator::new().into_ref(),
            crypto: Crypto::default(),
            key: Key::new_empty(),
            frame_cache,
            addr_cache: Lru::new(Self::ADDRESS_CACHE_SIZE),
        })
    }

    #[inline]
    pub fn get_key(&self) -> &Key {
        &self.key
    }

    #[inline]
    pub fn exists(&self) -> Result<bool> {
        self.depot.exists()
    }

    #[inline]
    pub fn connect(&mut self, force: bool) -> Result<()> {
        self.depot.connect(force)
    }

    pub fn init(&mut self, cost: Cost, cipher: Cipher) -> Result<()> {
        // create crypto and master key
        self.crypto = Crypto::new(cost, cipher)?;
        self.key = Crypto::gen_master_key();

        // initialise depot
        self.depot.init(self.crypto.clone(), self.key.derive(0))
    }

    pub fn open(
        &mut self,
        cost: Cost,
        cipher: Cipher,
        key: Key,
        force: bool,
    ) -> Result<()> {
        self.crypto = Crypto::new(cost, cipher)?;
        self.key = key;

        // open depot
        self.depot
            .open(self.crypto.clone(), self.key.derive(0), force)
    }

    #[inline]
    pub fn get_allocator(&self) -> AllocatorRef {
        self.allocator.clone()
    }

    #[inline]
    pub fn get_super_block(&mut self, suffix: u64) -> Result<Vec<u8>> {
        self.depot.get_super_block(suffix)
    }

    #[inline]
    pub fn put_super_block(
        &mut self,
        super_blk: &[u8],
        suffix: u64,
    ) -> Result<()> {
        self.depot.put_super_block(super_blk, suffix)
    }

    // read entity address from depot and save to address cache
    fn get_address(&mut self, id: &Eid) -> Result<Addr> {
        // get from address cache first
        if let Some(addr) = self.addr_cache.get_refresh(id) {
            return Ok(addr.clone());
        }

        // if not in the cache, load if from depot
        let buf = self.depot.get_address(id)?;
        let buf = self.crypto.decrypt(&buf, &self.key)?;
        let mut de = Deserializer::new(&buf[..]);
        let addr: Addr = Deserialize::deserialize(&mut de)?;

        // and then insert into address cache
        self.addr_cache.insert(id.clone(), addr.clone());

        Ok(addr)
    }

    // write entity address to depot
    fn put_address(&mut self, id: &Eid, addr: &Addr) -> Result<()> {
        // serialize address and encrypt address
        let mut buf = Vec::new();
        addr.serialize(&mut Serializer::new(&mut buf))?;
        let buf = self.crypto.encrypt(&buf, &self.key)?;

        // write to depot and remove address from cache
        self.depot.put_address(id, &buf)?;
        self.addr_cache.insert(id.clone(), addr.clone());

        Ok(())
    }

    // remove all blocks in a address
    fn remove_address_blocks(&mut self, addr: &Addr) -> Result<()> {
        let mut inaddr_idx = 0;
        for loc_span in addr.iter() {
            let blk_cnt = loc_span.span.cnt;

            // delete blocks
            self.depot.del_blocks(loc_span.span)?;

            let mut blk_idx = loc_span.span.begin;
            let end_idx = inaddr_idx + blk_cnt;

            while inaddr_idx < end_idx {
                let offset = inaddr_idx % BLKS_PER_FRAME;
                if offset == 0 {
                    self.frame_cache.remove(&blk_idx);
                }
                let step = min(end_idx - inaddr_idx, BLKS_PER_FRAME - offset);
                inaddr_idx += step;
                blk_idx += step;
            }
        }
        Ok(())
    }

    #[inline]
    pub fn del_wal(&mut self, id: &Eid) -> Result<()> {
        self.depot.del_wal(id)
    }

    // delete an entity, including data and address
    pub fn del(&mut self, id: &Eid) -> Result<()> {
        // get address first
        let addr = match self.get_address(id) {
            Ok(addr) => addr,
            Err(ref err) if *err == Error::NotFound => return Ok(()),
            Err(err) => return Err(err),
        };

        // remove blocks in the address
        self.remove_address_blocks(&addr)?;

        // remove address
        self.depot.del_address(id)?;
        self.addr_cache.remove(id);

        Ok(())
    }

    // flush underlying storage
    #[inline]
    pub fn flush(&mut self) -> Result<()> {
        self.depot.flush()
    }

    #[inline]
    pub fn destroy(&mut self) -> Result<()> {
        self.depot.destroy()
    }
}

impl Default for Storage {
    #[inline]
    fn default() -> Self {
        Storage {
            depot: Box::new(DummyStorage::default()),
            allocator: Allocator::default().into_ref(),
            crypto: Crypto::default(),
            key: Key::new_empty(),
            frame_cache: Lru::default(),
            addr_cache: Lru::default(),
        }
    }
}

impl Debug for Storage {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("Storage")
            .field("depot", &self.depot)
            .field("allocator", &self.allocator)
            .finish()
    }
}

impl Display for Storage {
    #[inline]
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Storage({:?})", self.depot)
    }
}

impl IntoRef for Storage {}

/// Storage reference type
pub type StorageRef = Arc<RwLock<Storage>>;
pub type StorageWeakRef = Weak<RwLock<Storage>>;

/// Storage Wal Reader
#[derive(Debug)]
pub struct WalReader {
    id: Eid,
    storage: StorageRef,
    read: usize,
    wal: Vec<u8>,
}

impl WalReader {
    pub fn new(id: &Eid, storage: &StorageRef) -> Self {
        WalReader {
            id: id.clone(),
            storage: storage.clone(),
            read: 0,
            wal: Vec::new(),
        }
    }
}

impl Read for WalReader {
    fn read(&mut self, buf: &mut [u8]) -> IoResult<usize> {
        if self.wal.is_empty() {
            let mut storage = self.storage.write().unwrap();

            // read wal bytes from underlying storage layer
            let wal = storage.depot.get_wal(&self.id).map_err(|err| {
                if err == Error::NotFound {
                    IoError::new(ErrorKind::NotFound, "Wal not found")
                } else {
                    IoError::new(ErrorKind::Other, err.to_string())
                }
            })?;

            // decrypt wal
            self.wal =
                map_io_err!(storage.crypto.decrypt(&wal, &storage.key,))?;
        }

        let copy_len = min(self.wal.len() - self.read, buf.len());
        buf[..copy_len]
            .copy_from_slice(&self.wal[self.read..self.read + copy_len]);
        self.read += copy_len;

        Ok(copy_len)
    }
}

/// Storage Reader
#[derive(Debug)]
pub struct Reader {
    storage: StorageRef,

    // addresses split into frames
    addrs: Vec<Addr>,

    // entity length in storage
    ent_len: usize,

    // encrypted frame read from depot
    frame: Vec<u8>,

    // frame index
    frm_idx: usize,

    // frame cache key, the 1st block index in the frame
    frm_key: usize,

    // decrypted frame
    dec_frame: Vec<u8>,
    dec_frame_len: usize,

    // total decryped bytes read out so far
    read: usize,
}

impl Reader {
    pub fn new(id: &Eid, storage: &StorageRef) -> Result<Self> {
        let (addr, dec_frame_size) = {
            let mut storage = storage.write().unwrap();
            let addr = storage.get_address(id)?;
            (addr, storage.crypto.decrypted_len(FRAME_SIZE))
        };

        // split address to frames and set the first frame key
        let addrs = addr.divide_to_frames();
        let frm_key = addrs[0].list[0].span.begin;

        let mut rdr = Reader {
            storage: storage.clone(),
            addrs,
            ent_len: addr.len,
            frame: vec![0u8; FRAME_SIZE],
            frm_idx: 0,
            frm_key,
            dec_frame: vec![0u8; dec_frame_size],
            dec_frame_len: 0,
            read: 0,
        };

        rdr.frame.shrink_to_fit();

        Ok(rdr)
    }

    // copy data out from decrypte frame to destination
    // return copied bytes length and flag if frame is exhausted
    fn copy_frame_out(
        &self,
        dst: &mut [u8],
        dec_frame: &[u8],
    ) -> (usize, bool) {
        let begin = self.read % self.dec_frame.len();
        let copy_len = min(dst.len(), dec_frame.len() - begin);
        let end = begin + copy_len;
        dst[..copy_len].copy_from_slice(&dec_frame[begin..end]);
        (copy_len, end >= dec_frame.len())
    }
}

impl Read for Reader {
    fn read(&mut self, buf: &mut [u8]) -> IoResult<usize> {
        if self.frm_idx >= self.addrs.len() || buf.is_empty() {
            return Ok(0);
        }

        let mut storage = self.storage.write().unwrap();

        // if decrypted frame has been exhausted and the
        // frame is not in the frame cache, read it from underlying depot
        // and save to cache if it is necessary
        if self.dec_frame_len == 0
            && !storage.frame_cache.contains_key(&self.frm_key)
        {
            // read a frame from depot
            let mut read = 0;
            for loc_span in self.addrs[self.frm_idx].iter() {
                let read_len = loc_span.span.bytes_len();
                storage
                    .depot
                    .get_blocks(
                        &mut self.frame[read..read + read_len],
                        loc_span.span,
                    )
                    .map_err(|err| {
                        if err == Error::NotFound {
                            IoError::new(
                                ErrorKind::NotFound,
                                "Blocks not found",
                            )
                        } else {
                            IoError::new(ErrorKind::Other, err.to_string())
                        }
                    })?;
                read += read_len;
            }

            // decrypt frame
            self.dec_frame_len = map_io_err!(storage.crypto.decrypt_to(
                &mut self.dec_frame,
                &self.frame[..self.addrs[self.frm_idx].len],
                &storage.key,
            ))?;

            // and then add the decrypted frame to cache if it is not too big
            if self.ent_len < Storage::FRAME_CACHE_THRESHOLD {
                storage.frame_cache.insert(
                    self.frm_key,
                    self.dec_frame[..self.dec_frame_len].to_vec(),
                );
            }
        }

        // copy decryped frame out to destination
        let (copy_len, frm_is_exhausted) =
            if self.ent_len < Storage::FRAME_CACHE_THRESHOLD {
                let dec_frame =
                    storage.frame_cache.get_refresh(&self.frm_key).unwrap();
                self.copy_frame_out(buf, dec_frame)
            } else {
                self.copy_frame_out(buf, &self.dec_frame[..self.dec_frame_len])
            };
        self.read += copy_len;

        // if frame is exhausted, advance to the next frame
        if frm_is_exhausted {
            self.frm_idx += 1;
            self.dec_frame_len = 0;
            if self.frm_idx < self.addrs.len() {
                self.frm_key = self.addrs[self.frm_idx].list[0].span.begin;
            }
        }

        Ok(copy_len)
    }
}

/// Storage Wal Writer
pub struct WalWriter {
    id: Eid,
    storage: StorageRef,
    wal: Vec<u8>,
}

impl WalWriter {
    pub fn new(id: &Eid, storage: &StorageRef) -> Self {
        WalWriter {
            id: id.clone(),
            storage: storage.clone(),
            wal: Vec::new(),
        }
    }
}

impl Write for WalWriter {
    fn write(&mut self, buf: &[u8]) -> IoResult<usize> {
        self.wal.extend_from_slice(buf);
        Ok(buf.len())
    }

    #[inline]
    fn flush(&mut self) -> IoResult<()> {
        // no-op here, call finish() to complete the writing
        Ok(())
    }
}

impl Finish for WalWriter {
    fn finish(self) -> Result<()> {
        let mut storage = self.storage.write().unwrap();

        // encrypt wal and save to underlying storage
        let enc = storage.crypto.encrypt(&self.wal, &storage.key)?;
        storage.depot.put_wal(&self.id, &enc)
    }
}

/// Storage Writer
pub struct Writer {
    id: Eid,
    addr: Addr,
    storage: StorageWeakRef,

    // encrypted frame
    frame: Vec<u8>,

    // stage data buffer, length is decrypted_len(FRAME_SIZE)
    stg: Vec<u8>,
    stg_len: usize,
}

impl Writer {
    pub fn new(id: &Eid, storage: &StorageWeakRef) -> Result<Self> {
        let stg_size = {
            let storage = storage.upgrade().ok_or(Error::RepoClosed)?;
            let storage = storage.read().unwrap();
            storage.crypto.decrypted_len(FRAME_SIZE)
        };
        let mut wtr = Writer {
            id: id.clone(),
            addr: Addr::default(),
            storage: storage.clone(),
            frame: vec![0u8; FRAME_SIZE],
            stg: vec![0u8; stg_size],
            stg_len: 0,
        };
        wtr.frame.shrink_to_fit();
        wtr.stg.shrink_to_fit();
        Ok(wtr)
    }

    // encrypt to frame and write to depot
    fn write_frame(&mut self) -> Result<()> {
        if self.stg_len == 0 {
            return Ok(());
        }

        let storage = self.storage.upgrade().ok_or(Error::RepoClosed)?;
        let mut storage = storage.write().unwrap();

        // encrypt source data to frame
        let enc_len = storage.crypto.encrypt_to(
            &mut self.frame,
            &self.stg[..self.stg_len],
            &storage.key,
        )?;

        let blk_cnt = align_ceil_chunk(enc_len, BLK_SIZE);
        let aligned_len = blk_cnt * BLK_SIZE;

        // add padding bytes
        Crypto::random_buf(&mut self.frame[enc_len..aligned_len]);

        // allocate blocks
        let span = {
            let allocator_ref = storage.get_allocator();
            let mut allocator = allocator_ref.write().unwrap();
            allocator.allocate(blk_cnt)
        };

        // write frame to depot
        storage.depot.put_blocks(span, &self.frame[..aligned_len])?;

        // append to address and reset stage buffer
        self.addr.append(span, enc_len);
        self.stg_len = 0;

        Ok(())
    }
}

impl Write for Writer {
    fn write(&mut self, buf: &[u8]) -> IoResult<usize> {
        let copy_len = min(self.stg.len() - self.stg_len, buf.len());
        self.stg[self.stg_len..self.stg_len + copy_len]
            .copy_from_slice(&buf[..copy_len]);
        self.stg_len += copy_len;
        if self.stg_len >= self.stg.len() {
            // stage buffer is full, encrypt to frame and write to depot
            map_io_err!(self.write_frame())?;
        }
        Ok(copy_len)
    }

    #[inline]
    fn flush(&mut self) -> IoResult<()> {
        // no-op here, call finish() to complete the writing
        Ok(())
    }
}

impl Finish for Writer {
    fn finish(mut self) -> Result<()> {
        // write data frame
        self.write_frame()?;

        // if the old address exists, remove all of its blocks
        let storage = self.storage.upgrade().ok_or(Error::RepoClosed)?;
        let mut storage = storage.write().unwrap();
        match storage.get_address(&self.id) {
            Ok(old_addr) => {
                storage.remove_address_blocks(&old_addr)?;
            }
            Err(ref err) if *err == Error::NotFound => {}
            Err(err) => return Err(err),
        }

        // write new address
        storage.put_address(&self.id, &self.addr)
    }
}

#[cfg(test)]
mod tests {
    extern crate tempdir;

    use std::time::Instant;

    #[cfg(feature = "storage-file")]
    use self::tempdir::TempDir;
    use super::*;
    use base::crypto::{Cipher, Cost, Crypto, RandomSeed, RANDOM_SEED_SIZE};
    use base::init_env;
    use base::utils::speed_str;
    use volume::address::Span;

    struct SizeVar {
        blk_size: usize,
        frm_size: usize,
        enc_blk_size: usize,
        enc_frm_size: usize,
        dec_blk_size: usize,
        dec_frm_size: usize,
    }

    impl SizeVar {
        fn new(storage: &StorageRef) -> Self {
            let storage = storage.read().unwrap();
            let crypto = &storage.crypto;
            SizeVar {
                blk_size: BLK_SIZE,
                frm_size: FRAME_SIZE,
                enc_blk_size: crypto.encrypted_len(BLK_SIZE),
                enc_frm_size: crypto.encrypted_len(FRAME_SIZE),
                dec_blk_size: crypto.decrypted_len(BLK_SIZE),
                dec_frm_size: crypto.decrypted_len(FRAME_SIZE),
            }
        }
    }

    fn single_read_write(buf_len: usize, storage: &StorageRef) {
        let id = Eid::new();
        let mut buf = vec![0u8; buf_len];
        *buf.first_mut().unwrap() = 42;
        *buf.last_mut().unwrap() = 42;

        // write
        let mut wtr = Writer::new(&id, &Arc::downgrade(storage)).unwrap();
        wtr.write_all(&buf).unwrap();
        wtr.finish().unwrap();

        // read
        let mut rdr = Reader::new(&id, storage).unwrap();
        let mut dst = Vec::new();
        rdr.read_to_end(&mut dst).unwrap();
        assert_eq!(&buf[..], &dst[..]);
    }

    fn multi_read_write(buf_len: usize, frm_size: usize, storage: &StorageRef) {
        let (id, id2) = (Eid::new(), Eid::new());

        let mut buf = vec![0u8; buf_len];
        let mut buf2 = vec![0u8; buf_len];
        *buf.first_mut().unwrap() = 42;
        *buf.last_mut().unwrap() = 42;
        *buf2.first_mut().unwrap() = 43;
        *buf2.last_mut().unwrap() = 43;

        // write
        let mut wtr = Writer::new(&id, &Arc::downgrade(storage)).unwrap();
        let mut wtr2 = Writer::new(&id2, &Arc::downgrade(storage)).unwrap();
        let mut written = 0;
        while written < buf_len {
            let wlen = min(frm_size, buf_len - written);
            let w = wtr.write(&buf[written..written + wlen]).unwrap();
            let w2 = wtr2.write(&buf2[written..written + wlen]).unwrap();
            assert_eq!(w, w2);
            written += w;
        }
        wtr.finish().unwrap();
        wtr2.finish().unwrap();

        // read
        let mut rdr = Reader::new(&id, storage).unwrap();
        let mut rdr2 = Reader::new(&id2, storage).unwrap();
        let mut dst = Vec::new();
        rdr.read_to_end(&mut dst).unwrap();
        assert_eq!(buf.len(), dst.len());
        assert_eq!(&buf[..], &dst[..]);
        dst.truncate(0);
        rdr2.read_to_end(&mut dst).unwrap();
        assert_eq!(buf2.len(), dst.len());
        assert_eq!(&buf2[..], &dst[..]);
    }

    fn single_span_addr_test(storage: &StorageRef) {
        let size = SizeVar::new(storage);

        // case #1, basic
        single_read_write(3, storage);

        // case #2, block boundary
        single_read_write(size.blk_size, storage);
        single_read_write(size.enc_blk_size, storage);
        single_read_write(size.dec_blk_size, storage);
        single_read_write(2 * size.blk_size, storage);
        single_read_write(2 * size.enc_blk_size, storage);
        single_read_write(2 * size.dec_blk_size, storage);

        // case #3, frame boundary
        single_read_write(size.frm_size, storage);
        single_read_write(size.enc_frm_size, storage);
        single_read_write(size.dec_frm_size, storage);
        single_read_write(2 * size.frm_size, storage);
        single_read_write(2 * size.enc_frm_size, storage);
        single_read_write(2 * size.dec_frm_size, storage);
    }

    fn multi_span_addr_test(storage: &StorageRef) {
        let size = SizeVar::new(storage);
        multi_read_write(size.frm_size, size.frm_size, storage);
        multi_read_write(size.enc_frm_size, size.frm_size, storage);
        multi_read_write(size.dec_frm_size, size.frm_size, storage);
        multi_read_write(2 * size.frm_size, size.frm_size, storage);
        multi_read_write(2 * size.enc_frm_size, size.frm_size, storage);
        multi_read_write(2 * size.dec_frm_size, size.frm_size, storage);
    }

    fn overwrite_test(storage: &StorageRef) {
        let id = Eid::new();
        let mut buf = vec![0u8; 3];
        *buf.first_mut().unwrap() = 42;
        *buf.last_mut().unwrap() = 42;

        // write #1
        let mut wtr = Writer::new(&id, &Arc::downgrade(storage)).unwrap();
        wtr.write_all(&buf).unwrap();
        wtr.finish().unwrap();

        // read
        let mut rdr = Reader::new(&id, storage).unwrap();
        let mut dst = Vec::new();
        rdr.read_to_end(&mut dst).unwrap();
        assert_eq!(&buf[..], &dst[..]);

        // write #2
        *buf.first_mut().unwrap() = 43;
        *buf.last_mut().unwrap() = 43;
        let mut wtr = Writer::new(&id, &Arc::downgrade(storage)).unwrap();
        wtr.write_all(&buf).unwrap();
        wtr.finish().unwrap();

        // read
        let mut rdr = Reader::new(&id, storage).unwrap();
        let mut dst = Vec::new();
        rdr.read_to_end(&mut dst).unwrap();
        assert_eq!(&buf[..], &dst[..]);
    }

    fn delete_test(storage: &StorageRef) {
        let id = Eid::new();
        let buf = vec![0u8; 3];

        // write #1
        let mut wtr = Writer::new(&id, &Arc::downgrade(storage)).unwrap();
        wtr.write_all(&buf).unwrap();
        wtr.finish().unwrap();

        // read
        let mut rdr = Reader::new(&id, storage).unwrap();
        let mut dst = Vec::new();
        rdr.read_to_end(&mut dst).unwrap();

        // delete
        {
            let mut storage = storage.write().unwrap();
            storage.del(&id).unwrap();
            // delete again
            storage.del(&id).unwrap();
        }

        // read again will fail
        assert_eq!(Reader::new(&id, storage).unwrap_err(), Error::NotFound);
    }

    fn test_depot(storage: StorageRef) {
        single_span_addr_test(&storage);
        multi_span_addr_test(&storage);
        overwrite_test(&storage);
        delete_test(&storage);
    }

    #[test]
    fn mem_depot() {
        init_env();
        let mut storage = Storage::new("mem://storage.mem_depot").unwrap();
        assert!(!storage.exists().unwrap());
        storage.init(Cost::default(), Cipher::default()).unwrap();
        test_depot(storage.into_ref());
    }

    #[cfg(feature = "storage-file")]
    #[test]
    fn file_depot() {
        init_env();
        let tmpdir = TempDir::new("zbox_test").expect("Create temp dir failed");
        let uri = format!("file://{}", tmpdir.path().display());
        let storage = Storage::new(&uri).unwrap();
        test_depot(storage.into_ref());
    }

    #[cfg(feature = "storage-sqlite")]
    #[test]
    fn sqlite_depot() {
        init_env();
        let mut storage = Storage::new("sqlite://:memory:").unwrap();
        storage.connect(false).unwrap();
        storage.init(Cost::default(), Cipher::default()).unwrap();
        test_depot(storage.into_ref());
    }

    #[cfg(feature = "storage-redis")]
    #[test]
    fn redis_depot() {
        init_env();
        let mut storage = Storage::new("redis://127.0.0.1").unwrap();
        storage.connect(false).unwrap();
        storage.init(Cost::default(), Cipher::default()).unwrap();
        test_depot(storage.into_ref());
    }

    #[cfg(feature = "storage-zbox")]
    #[test]
    fn zbox_depot() {
        init_env();
        let mut storage = Storage::new(
            "zbox://accessKey456@repo456?cache_type=mem&cache_size=1mb",
        )
        .unwrap();
        storage.connect(false).unwrap();
        storage.init(Cost::default(), Cipher::default()).unwrap();
        test_depot(storage.into_ref());
    }

    fn perf_test(storage: &StorageRef, prefix: &str) {
        const DATA_LEN: usize = 36 * 1024 * 1024;
        let mut buf = vec![0u8; DATA_LEN];
        let seed = RandomSeed::from(&[0u8; RANDOM_SEED_SIZE]);
        Crypto::random_buf_deterministic(&mut buf, &seed);
        let id = Eid::new();

        // write
        let now = Instant::now();
        let mut wtr = Writer::new(&id, &Arc::downgrade(storage)).unwrap();
        wtr.write_all(&buf).unwrap();
        wtr.finish().unwrap();
        let write_time = now.elapsed();

        // read
        let now = Instant::now();
        let mut rdr = Reader::new(&id, storage).unwrap();
        let mut dst = Vec::new();
        let read = rdr.read_to_end(&mut dst).unwrap();
        assert_eq!(read, buf.len());
        let read_time = now.elapsed();

        println!(
            "{} perf: read: {}, write: {}",
            prefix,
            speed_str(&read_time, DATA_LEN),
            speed_str(&write_time, DATA_LEN)
        );
    }

    #[test]
    fn mem_perf() {
        init_env();
        let mut storage = Storage::new("mem://storage.mem_perf").unwrap();
        storage.init(Cost::default(), Cipher::default()).unwrap();
        let storage = storage.into_ref();
        perf_test(&storage, "Memory storage");
    }

    #[cfg(feature = "storage-file")]
    #[test]
    fn file_perf() {
        init_env();
        let tmpdir = TempDir::new("zbox_test").expect("Create temp dir failed");
        let uri = format!("file://{}", tmpdir.path().display());
        let mut storage = Storage::new(&uri).unwrap();
        storage.init(Cost::default(), Cipher::default()).unwrap();
        let storage = storage.into_ref();
        perf_test(&storage, "File storage");
    }

    #[test]
    #[ignore]
    fn crypto_perf_test() {
        init_env();

        let crypto = Crypto::new(Cost::default(), Cipher::Aes).unwrap();
        let key = Key::new_empty();
        let mut depot = super::super::mem::MemStorage::new("crypto_perf_test");
        depot.init(crypto.clone(), key.derive(0)).unwrap();

        const DATA_LEN: usize = 32 * 1024 * 1024;
        let chunk_size = crypto.decrypted_len(FRAME_SIZE);
        let data_size = DATA_LEN / FRAME_SIZE * chunk_size;
        let mut data = vec![0u8; data_size];
        let seed = RandomSeed::from(&[0u8; RANDOM_SEED_SIZE]);
        Crypto::random_buf_deterministic(&mut data, &seed);

        // write
        let mut buf = vec![0u8; FRAME_SIZE];
        let mut blk_cnt = 0;
        let now = Instant::now();
        for frame in data.chunks(chunk_size) {
            let _enc_len = crypto.encrypt_to(&mut buf, frame, &key).unwrap();
            depot
                .put_blocks(Span::new(blk_cnt, BLKS_PER_FRAME), &buf)
                .unwrap();
            blk_cnt += BLKS_PER_FRAME;
        }
        let write_time = now.elapsed();

        // read
        let mut dst = vec![0u8; chunk_size];
        let now = Instant::now();
        for frm_idx in 0..blk_cnt / BLKS_PER_FRAME {
            depot
                .get_blocks(
                    &mut buf,
                    Span::new(frm_idx * BLKS_PER_FRAME, BLKS_PER_FRAME),
                )
                .unwrap();
            crypto.decrypt_to(&mut dst, &buf, &key).unwrap();
        }
        let read_time = now.elapsed();

        println!(
            "Raw crypto + mem storage perf: read: {}, write: {}",
            speed_str(&read_time, DATA_LEN),
            speed_str(&write_time, DATA_LEN)
        );
    }
}
