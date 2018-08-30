use std::cmp::min;
use std::error::Error as StdError;
use std::fmt::{self, Debug};
use std::io::{Error as IoError, ErrorKind, Read, Result as IoResult, Write};
use std::ops::DerefMut;
use std::path::Path;
use std::sync::{Arc, RwLock};

use rmp_serde::{Deserializer, Serializer};
use serde::{Deserialize, Serialize};

use super::file::FileStorage;
use super::mem::MemStorage;
use super::Storable;
use base::crypto::{Cipher, Cost, Crypto, Key};
use base::lru::{CountMeter, Lru, Meter, PinChecker};
use base::utils::align_ceil_chunk;
use base::IntoRef;
use error::{Error, Result};
use trans::{Eid, Finish};
use volume::address::Addr;
use volume::{Allocator, AllocatorRef, BLKS_PER_FRAME, BLK_SIZE, FRAME_SIZE};

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
    depot: Box<Storable>,

    // block allocator
    allocator: AllocatorRef,

    // crypto context
    crypto: Crypto,
    key: Key,

    // decrypted frame cache, key is the begin block index
    frame_cache: Lru<u64, Vec<u8>, FrameCacheMeter, PinChecker<Vec<u8>>>,

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
        let depot: Box<Storable> = if uri.starts_with("file://") {
            let path = Path::new(&uri[7..]);
            let depot = FileStorage::new(path);
            Box::new(depot)
        } else if uri.starts_with("mem://") {
            let depot = MemStorage::new();
            Box::new(depot)
        } else if uri.starts_with("faulty://") {
            #[cfg(feature = "storage-faulty")]
            {
                let depot = super::faulty::FaultyStorage::new(&uri[9..]);
                Box::new(depot)
            }
            #[cfg(not(feature = "storage-faulty"))]
            {
                return Err(Error::InvalidUri);
            }
        } else {
            return Err(Error::InvalidUri);
        };

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
    pub fn depot_mut(&mut self) -> &mut Storable {
        self.depot.deref_mut()
    }

    #[inline]
    pub fn crypto_ctx(&self) -> (&Crypto, &Key) {
        (&self.crypto, &self.key)
    }

    #[inline]
    pub fn exists(&self) -> Result<bool> {
        self.depot.exists()
    }

    pub fn init(&mut self, cost: Cost, cipher: Cipher) -> Result<()> {
        // create crypto and master key
        self.crypto = Crypto::new(cost, cipher)?;
        self.key = Key::new();

        // initialise depot
        self.depot.init(self.crypto.clone(), self.key.derive(0))
    }

    pub fn open(&mut self, cost: Cost, cipher: Cipher, key: Key) -> Result<()> {
        self.crypto = Crypto::new(cost, cipher)?;
        self.key = key;

        // open depot
        self.depot.open(self.crypto.clone(), self.key.derive(0))
    }

    #[inline]
    pub fn allocator(&self) -> AllocatorRef {
        self.allocator.clone()
    }

    fn allocate_blocks(&mut self, blk_cnt: usize) -> u64 {
        let mut allocator = self.allocator.write().unwrap();
        allocator.allocate(blk_cnt)
    }

    // read entity address from depot and save to address cache
    fn get_addr(&mut self, id: &Eid) -> Result<Addr> {
        // get from address cache first
        if let Some(addr) = self.addr_cache.get_refresh(id) {
            return Ok(addr.clone());
        }

        // if not in the cache, load if from depot
        let buf = self.depot.get_addr(id)?;
        let buf = self.crypto.decrypt(&buf, &self.key)?;
        let mut de = Deserializer::new(&buf[..]);
        let addr: Addr = Deserialize::deserialize(&mut de)?;

        // and then insert into address cache
        self.addr_cache.insert(id.clone(), addr.clone());

        Ok(addr)
    }

    // write entity address to depot
    fn put_addr(&mut self, id: &Eid, addr: &Addr) -> Result<()> {
        // serialize address and encrypt address
        let mut buf = Vec::new();
        addr.serialize(&mut Serializer::new(&mut buf))?;
        let buf = self.crypto.encrypt(&buf, &self.key)?;

        // write to depot and remove address from cache
        self.depot.put_addr(id, &buf)?;
        self.addr_cache.insert(id.clone(), addr.clone());

        Ok(())
    }

    // remove all blocks in a address
    fn remove_address_blocks(&mut self, addr: &Addr) -> Result<()> {
        let mut inaddr_idx = 0;
        for span in addr.iter() {
            let blk_cnt = span.block_count();

            // delete blocks
            self.depot.del_blocks(span.begin, blk_cnt)?;

            let mut blk_idx = span.begin;
            let end_idx = inaddr_idx + blk_cnt as u64;

            while inaddr_idx < end_idx {
                let offset = inaddr_idx % BLKS_PER_FRAME as u64;
                if offset == 0 {
                    self.frame_cache.remove(&blk_idx);
                }
                let step =
                    min(end_idx - inaddr_idx, BLKS_PER_FRAME as u64 - offset);
                inaddr_idx += step;
                blk_idx += step;
            }
        }
        Ok(())
    }

    pub fn del(&mut self, id: &Eid) -> Result<()> {
        // get address first
        let addr = match self.get_addr(id) {
            Ok(addr) => addr,
            Err(ref err) if *err == Error::NotFound => return Ok(()),
            Err(err) => return Err(err),
        };

        // remove blocks in the address
        self.remove_address_blocks(&addr)?;

        // remove address
        self.depot.del_addr(id)?;
        self.addr_cache.remove(id);

        Ok(())
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

impl IntoRef for Storage {}

/// Storage reference type
pub type StorageRef = Arc<RwLock<Storage>>;

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
    frm_key: u64,

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
            let addr = storage.get_addr(id)?;
            (addr, storage.crypto.decrypted_len(FRAME_SIZE))
        };

        // split address to frames and set the first frame key
        let addrs = addr.split_to_frames();
        let frm_key = addrs[0].list[0].begin;

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

    // read a frame, decrypt and save it to storage frame cache
    // if it is not too big
    fn read_frame(&mut self) -> Result<()> {
        let mut storage = self.storage.write().unwrap();

        // if decrypted frame hasn't been exhausted yet or
        // frame is already in the frame cache
        if self.dec_frame_len > 0
            || storage.frame_cache.contains_key(&self.frm_key)
        {
            return Ok(());
        }

        // read a frame from depot
        let mut read = 0;
        for span in self.addrs[self.frm_idx].iter() {
            let read_len = span.block_len();
            storage.depot.get_blocks(
                &mut self.frame[read..read + read_len],
                span.begin,
                span.block_count(),
            )?;
            read += read_len;
        }

        // decrypt frame
        self.dec_frame_len = storage.crypto.decrypt_to(
            &mut self.dec_frame,
            &self.frame[..self.addrs[self.frm_idx].len],
            &storage.key,
        )?;

        // and then add the decrypted frame to cache if it is not too big
        if self.ent_len < Storage::FRAME_CACHE_THRESHOLD {
            storage.frame_cache.insert(
                self.frm_key,
                self.dec_frame[..self.dec_frame_len].to_vec(),
            );
        }

        Ok(())
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

        // read frame into frame cache
        self.read_frame().map_err(|err| {
            if err == Error::NotFound {
                IoError::new(ErrorKind::NotFound, "Blocks not found")
            } else {
                IoError::new(ErrorKind::Other, err.description())
            }
        })?;

        // copy decryped frame out to destination
        let (copy_len, frm_is_exhausted) =
            if self.ent_len < Storage::FRAME_CACHE_THRESHOLD {
                let mut storage = self.storage.write().unwrap();
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
                self.frm_key = self.addrs[self.frm_idx].list[0].begin;
            }
        }

        Ok(copy_len)
    }
}

/// Storage Writer
pub struct Writer {
    id: Eid,
    addr: Addr,
    storage: StorageRef,

    // encrypted frame
    frame: Vec<u8>,

    // stage data buffer, length is decrypted_len(FRAME_SIZE)
    stg: Vec<u8>,
    stg_len: usize,
}

impl Writer {
    pub fn new(id: &Eid, storage: &StorageRef) -> Self {
        let stg_size;
        {
            let storage = storage.read().unwrap();
            stg_size = storage.crypto.decrypted_len(FRAME_SIZE);
        }
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
        wtr
    }

    // encrypt to frame and write to depot
    fn write_frame(&mut self) -> Result<()> {
        if self.stg_len == 0 {
            return Ok(());
        }

        let mut storage = self.storage.write().unwrap();

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
        let begin_blk_idx = storage.allocate_blocks(blk_cnt);

        // write frame to depot
        storage.depot.put_blocks(
            begin_blk_idx,
            blk_cnt,
            &self.frame[..aligned_len],
        )?;

        // append to address and reset stage buffer
        self.addr.append(begin_blk_idx, blk_cnt, enc_len);
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

    fn flush(&mut self) -> IoResult<()> {
        // do nothing, use finish() to finalise writing
        Ok(())
    }
}

impl Finish for Writer {
    fn finish(mut self) -> Result<usize> {
        // flush frame to depot
        self.write_frame()?;

        let mut storage = self.storage.write().unwrap();

        // if the old address exists, remove all of its blocks
        match storage.get_addr(&self.id) {
            Ok(old_addr) => {
                storage.remove_address_blocks(&old_addr)?;
            }
            Err(ref err) if *err == Error::NotFound => {}
            Err(err) => return Err(err),
        }

        // write new address
        storage.put_addr(&self.id, &self.addr)?;

        Ok(self.addr.len)
    }
}

#[cfg(test)]
mod tests {
    extern crate tempdir;

    use std::time::Instant;
    use std::fs;
    use std::env;

    use self::tempdir::TempDir;
    use super::*;
    use base::crypto::{Cipher, Cost, Crypto, RandomSeed, RANDOM_SEED_SIZE};
    use base::init_env;
    use base::utils::speed_str;

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
        let mut wtr = Writer::new(&id, storage);
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
        let mut wtr = Writer::new(&id, storage);
        let mut wtr2 = Writer::new(&id2, storage);
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
        let mut wtr = Writer::new(&id, storage);
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
        let mut wtr = Writer::new(&id, storage);
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
        let mut wtr = Writer::new(&id, storage);
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

    #[test]
    fn mem_depot() {
        init_env();
        let storage = Storage::new("mem://foo").unwrap();
        assert!(!storage.exists().unwrap());
        let storage = storage.into_ref();

        single_span_addr_test(&storage);
        multi_span_addr_test(&storage);
        overwrite_test(&storage);
        delete_test(&storage);
    }

    #[test]
    fn file_depot() {
        init_env();
        let tmpdir = TempDir::new("zbox_test").expect("Create temp dir failed");
        let uri = format!("file://{}", tmpdir.path().display());
        let storage = Storage::new(&uri).unwrap().into_ref();

        single_span_addr_test(&storage);
        multi_span_addr_test(&storage);
        overwrite_test(&storage);
        delete_test(&storage);
    }

    fn perf_test(storage: &StorageRef, prefix: &str) {
        const DATA_LEN: usize = 36 * 1024 * 1024;
        let mut buf = vec![0u8; DATA_LEN];
        let seed = RandomSeed::from(&[0u8; RANDOM_SEED_SIZE]);
        Crypto::random_buf_deterministic(&mut buf, &seed);
        let id = Eid::new();

        // write
        let now = Instant::now();
        let mut wtr = Writer::new(&id, storage);
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
        let mut storage = Storage::new("mem://foo").unwrap();
        storage.init(Cost::default(), Cipher::default()).unwrap();
        let storage = storage.into_ref();
        perf_test(&storage, "Memory storage");
    }

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

        let mut dir = env::temp_dir();
        dir.push("zbox_crypto_perf_test");
        if dir.exists() {
            fs::remove_dir_all(&dir).unwrap();
        }
        fs::create_dir(&dir).unwrap();

        let crypto = Crypto::new(Cost::default(), Cipher::Aes).unwrap();
        let key = Key::new();
        let mut depot = FileStorage::new(&dir);
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
            depot.put_blocks(blk_cnt, BLKS_PER_FRAME, &buf).unwrap();
            blk_cnt += BLKS_PER_FRAME as u64;
        }
        let write_time = now.elapsed();

        // read
        let mut dst = vec![0u8; chunk_size];
        let now = Instant::now();
        for frm_idx in 0..blk_cnt / BLKS_PER_FRAME as u64 {
            depot
                .get_blocks(
                    &mut buf,
                    frm_idx * BLKS_PER_FRAME as u64,
                    BLKS_PER_FRAME,
                )
                .unwrap();
            crypto.decrypt_to(&mut dst, &buf, &key).unwrap();
        }
        let read_time = now.elapsed();

        println!(
            "Raw crypto + file storage perf: read: {}, write: {}",
            speed_str(&read_time, DATA_LEN),
            speed_str(&write_time, DATA_LEN)
        );

        fs::remove_dir_all(&dir).unwrap();
    }
}
