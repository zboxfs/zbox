use std::fmt::{self, Debug};
use std::io::{Read, Result as IoResult, Write};
use std::sync::{Arc, RwLock};

use lz4::{
    Decoder as Lz4Decoder, Encoder as Lz4Encoder,
    EncoderBuilder as Lz4EncoderBuilder,
};

use super::allocator::AllocatorRef;
use super::storage::{self, Storage, StorageRef};
use super::super_block::SuperBlk;
use base::crypto::{Cipher, Cost, Salt};
use base::{IntoRef, Time, Version};
use error::{Error, Result};
use fs::Config;
use trans::{Eid, Finish};

/// Volume info
#[derive(Debug, Clone, Default)]
pub struct Info {
    pub id: Eid,
    pub ver: Version,
    pub uri: String,
    pub compress: bool,
    pub cost: Cost,
    pub cipher: Cipher,
    pub ctime: Time,
}

/// Volume
#[derive(Debug)]
pub struct Volume {
    info: Info,
    storage: StorageRef,
}

impl Volume {
    /// Create volume instance
    pub fn new(uri: &str) -> Result<Self> {
        let mut info = Info::default();
        info.uri = uri.to_string();
        let storage = Storage::new(uri)?.into_ref();

        Ok(Volume { info, storage })
    }

    /// Initialise volume
    pub fn init(
        &mut self,
        pwd: &str,
        cfg: &Config,
        payload: &[u8],
    ) -> Result<()> {
        let mut storage = self.storage.write().unwrap();
        storage.connect()?;

        // initialise storage
        storage.init(cfg.cost, cfg.cipher)?;

        // initialise info
        self.info.id = Eid::new();
        self.info.ver = Version::current_repo_version();
        self.info.compress = cfg.compress;
        self.info.cost = cfg.cost;
        self.info.cipher = cfg.cipher;
        self.info.ctime = Time::now();

        // initialise super block
        let mut super_blk = SuperBlk::default();
        super_blk.head.salt = Salt::new();
        super_blk.head.cost = cfg.cost;
        super_blk.head.cipher = cfg.cipher;
        super_blk.body.volume_id = self.info.id.clone();
        super_blk.body.ver = self.info.ver.clone();
        super_blk.body.key = storage.get_key().clone();
        super_blk.body.uri = self.info.uri.clone();
        super_blk.body.compress = cfg.compress;
        super_blk.body.ctime = self.info.ctime;
        super_blk.body.payload = payload.to_vec();

        // save super block twice to save its both arms
        super_blk
            .save(pwd, &mut storage)
            .and(super_blk.save(pwd, &mut storage))?;

        debug!("volume initialised");

        Ok(())
    }

    /// Open volume, return super block payload and meta payload
    pub fn open(&mut self, pwd: &str) -> Result<Vec<u8>> {
        let mut storage = self.storage.write().unwrap();
        storage.connect()?;

        // load super block from storage
        let super_blk = SuperBlk::load(pwd, &mut storage)?;

        // check volume version
        if !super_blk.body.ver.match_repo_version() {
            return Err(Error::WrongVersion);
        }

        // open storage
        storage.open(
            super_blk.head.cost,
            super_blk.head.cipher,
            super_blk.body.key.clone(),
        )?;

        // set up info
        self.info.id = super_blk.body.volume_id.clone();
        self.info.ver = super_blk.body.ver;
        self.info.compress = super_blk.body.compress;
        self.info.cost = super_blk.head.cost;
        self.info.cipher = super_blk.head.cipher;
        self.info.ctime = super_blk.body.ctime;

        debug!("volume opened");

        Ok(super_blk.body.payload.clone())
    }

    /// Check specified volume if it exists
    pub fn exists(&self) -> Result<bool> {
        let storage = self.storage.read().unwrap();
        storage.exists()
    }

    /// Reset volume password
    pub fn reset_password(
        &mut self,
        old_pwd: &str,
        new_pwd: &str,
        cost: Cost,
    ) -> Result<()> {
        let mut storage = self.storage.write().unwrap();

        // load old super block
        let mut super_blk = SuperBlk::load(old_pwd, &mut storage)?;

        // save new super block with new password and cost
        super_blk.head.cost = cost;
        super_blk.save(new_pwd, &mut storage)?;

        self.info.cost = cost;

        Ok(())
    }

    // get volume info
    pub fn info(&self) -> Info {
        let mut ret = self.info.clone();

        // mask secrets in uri
        if let Some(end) = ret.uri.find("@") {
            let begin = ret.uri.find("://").unwrap() + 3;
            ret.uri.replace_range(begin..end, "***");
        }

        ret
    }

    // get allocator from storage
    #[inline]
    pub fn get_allocator(&self) -> AllocatorRef {
        let storage = self.storage.read().unwrap();
        storage.get_allocator()
    }

    #[inline]
    pub fn del_wal(&mut self, id: &Eid) -> Result<()> {
        let mut storage = self.storage.write().unwrap();
        storage.del_wal(id)
    }

    // delete an entity
    #[inline]
    pub fn del(&mut self, id: &Eid) -> Result<()> {
        let mut storage = self.storage.write().unwrap();
        storage.del(id)
    }

    #[inline]
    pub fn flush(&mut self) -> Result<()> {
        let mut storage = self.storage.write().unwrap();
        storage.flush()
    }
}

impl Default for Volume {
    fn default() -> Self {
        let storage = Storage::new("mem://dummy").unwrap().into_ref();
        Volume {
            info: Info::default(),
            storage,
        }
    }
}

impl IntoRef for Volume {}

/// Volume reference type
pub type VolumeRef = Arc<RwLock<Volume>>;

/// Volume Wal Reader
pub struct WalReader {
    inner: storage::WalReader,
}

impl WalReader {
    #[inline]
    pub fn new(id: &Eid, vol: &VolumeRef) -> Self {
        let vol = vol.read().unwrap();
        WalReader {
            inner: storage::WalReader::new(id, &vol.storage),
        }
    }
}

impl Read for WalReader {
    #[inline]
    fn read(&mut self, buf: &mut [u8]) -> IoResult<usize> {
        self.inner.read(buf)
    }
}

/// Volume Reader
pub struct Reader {
    inner: Box<Read>,
}

impl Reader {
    pub fn new(id: &Eid, vol: &VolumeRef) -> Result<Self> {
        let vol = vol.read().unwrap();
        let rdr = storage::Reader::new(id, &vol.storage)?;
        if vol.info.compress {
            Ok(Reader {
                inner: Box::new(Lz4Decoder::new(rdr).unwrap()),
            })
        } else {
            Ok(Reader {
                inner: Box::new(rdr),
            })
        }
    }
}

impl Read for Reader {
    #[inline]
    fn read(&mut self, buf: &mut [u8]) -> IoResult<usize> {
        self.inner.read(buf)
    }
}

impl Debug for Reader {
    #[inline]
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "VolReader")
    }
}

/// Volume Wal Writer
pub struct WalWriter {
    inner: storage::WalWriter,
}

impl WalWriter {
    #[inline]
    pub fn new(id: &Eid, vol: &VolumeRef) -> Self {
        let vol = vol.read().unwrap();
        WalWriter {
            inner: storage::WalWriter::new(id, &vol.storage),
        }
    }
}

impl Write for WalWriter {
    #[inline]
    fn write(&mut self, buf: &[u8]) -> IoResult<usize> {
        self.inner.write(buf)
    }

    #[inline]
    fn flush(&mut self) -> IoResult<()> {
        self.inner.flush()
    }
}

impl Finish for WalWriter {
    #[inline]
    fn finish(self) -> Result<()> {
        self.inner.finish()
    }
}

// volume inner writer wrapper
enum InnerWriter {
    Compress(Lz4Encoder<storage::Writer>),
    NoCompress(storage::Writer),
}

/// Volume writer
pub struct Writer {
    inner: InnerWriter,
}

impl Writer {
    pub fn new(id: &Eid, vol: &VolumeRef) -> Result<Self> {
        let vol = vol.read().unwrap();
        let wtr = storage::Writer::new(id, &vol.storage);
        let inner = if vol.info.compress {
            let comp = Lz4EncoderBuilder::new()
                .level(0)
                .auto_flush(true)
                .build(wtr)?;
            InnerWriter::Compress(comp)
        } else {
            InnerWriter::NoCompress(wtr)
        };
        Ok(Writer { inner })
    }
}

impl Write for Writer {
    fn write(&mut self, buf: &[u8]) -> IoResult<usize> {
        match self.inner {
            InnerWriter::Compress(ref mut inner) => inner.write(buf),
            InnerWriter::NoCompress(ref mut inner) => inner.write(buf),
        }
    }

    fn flush(&mut self) -> IoResult<()> {
        match self.inner {
            InnerWriter::Compress(ref mut inner) => inner.flush(),
            InnerWriter::NoCompress(ref mut inner) => inner.flush(),
        }
    }
}

impl Finish for Writer {
    fn finish(self) -> Result<()> {
        match self.inner {
            InnerWriter::Compress(inner) => {
                let (mut wtr, result) = inner.finish();
                result.map_err(|err| Error::from(err))?;
                wtr.finish()
            }
            InnerWriter::NoCompress(inner) => inner.finish(),
        }
    }
}

impl Debug for Writer {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "VolumeWriter()")
    }
}

#[cfg(test)]
mod tests {
    extern crate tempdir;

    use std::fs;
    //use std::path::PathBuf;
    use std::time::Instant;

    use self::tempdir::TempDir;
    use super::*;
    use base::crypto::{Crypto, RandomSeed, RANDOM_SEED_SIZE};
    use base::init_env;
    use base::utils::speed_str;

    fn setup_mem_vol() -> VolumeRef {
        init_env();
        let uri = "mem://test".to_string();
        let mut vol = Volume::new(&uri).unwrap();
        vol.init("pwd", &Config::default(), &Vec::new()).unwrap();
        vol.into_ref()
    }

    fn setup_file_vol(pwd: &str, payload: &[u8]) -> (VolumeRef, TempDir) {
        init_env();
        let tmpdir = TempDir::new("zbox_test").expect("Create temp dir failed");
        let dir = tmpdir.path().to_path_buf();
        //let dir = PathBuf::from("./tt");
        if dir.exists() {
            fs::remove_dir_all(&dir).unwrap();
        }
        let uri = format!("file://{}", dir.display());
        let mut vol = Volume::new(&uri).unwrap();
        vol.init(pwd, &Config::default(), payload).unwrap();
        (vol.into_ref(), tmpdir)
    }

    fn write_to_entity(id: &Eid, buf: &[u8], vol: &VolumeRef) {
        let mut wtr = Writer::new(&id, &vol).unwrap();
        wtr.write_all(&buf).unwrap();
        wtr.finish().unwrap();
    }

    fn verify_entity(id: &Eid, buf: &[u8], vol: &VolumeRef) {
        let mut dst = Vec::new();
        let mut rdr = Reader::new(&id, &vol).unwrap();
        rdr.read_to_end(&mut dst).unwrap();
        assert_eq!(&dst[..], &buf[..]);
    }

    fn read_write_test(vol: &VolumeRef) {
        let id = Eid::new();
        let buf = [1, 2, 3];
        let buf2 = [4, 5, 6];

        // #1, write and read
        write_to_entity(&id, &buf, &vol);
        verify_entity(&id, &buf, &vol);

        // #2, write and read on same entity again
        write_to_entity(&id, &buf2, &vol);
        verify_entity(&id, &buf2, &vol);

        // #3, delete entity
        {
            let mut vol = vol.write().unwrap();
            vol.del(&id).unwrap();
        }
        assert_eq!(Reader::new(&id, &vol).unwrap_err(), Error::NotFound);
    }

    fn reopen_test(pwd: &str, payload: &[u8], vol: VolumeRef) {
        let id = Eid::new();
        let buf = [1, 2, 3];

        read_write_test(&vol);
        write_to_entity(&id, &buf, &vol);

        {
            let mut vol = vol.write().unwrap();
            vol.flush().unwrap();
        }

        let (uri, _info, wmark) = {
            let vol = vol.read().unwrap();
            let storage = vol.storage.read().unwrap();
            let allocator_ref = storage.get_allocator();
            let allocator = allocator_ref.read().unwrap();
            (vol.info.uri.clone(), vol.info(), allocator.block_wmark())
        };

        // re-open volume
        drop(vol);
        let mut vol = Volume::new(&uri).unwrap();
        let buf = vol.open(&pwd).unwrap();
        assert_eq!(&buf[..], &payload[..]);
        {
            let storage = vol.storage.write().unwrap();
            let allocator_ref = storage.get_allocator();
            let mut allocator = allocator_ref.write().unwrap();
            allocator.set_block_wmark(wmark);
        }
        let vol = vol.into_ref();

        read_write_test(&vol);
        verify_entity(&id, &buf, &vol);
    }

    #[test]
    fn mem_volume() {
        let vol = setup_mem_vol();
        read_write_test(&vol);
    }

    #[test]
    fn file_volume() {
        let pwd = "pwd";
        let payload = [1, 2, 3];
        let (vol, _tmpdir) = setup_file_vol(&pwd, &payload);
        reopen_test(&pwd, &payload, vol);
    }

    #[cfg(feature = "storage-zbox")]
    #[test]
    fn zbox_volume() {
        init_env();
        let pwd = "pwd";
        let payload = [1, 2, 3];
        let uri = "zbox://accessKey456@repo456?cache_type=mem&cache_size=1";
        let mut vol = Volume::new(&uri).unwrap();
        vol.init(&pwd, &Config::default(), &payload).unwrap();
        let vol = vol.into_ref();

        reopen_test(&pwd, &payload, vol);
    }

    fn perf_test(vol: VolumeRef, prefix: &str) {
        const DATA_LEN: usize = 36 * 1024 * 1024;
        let mut buf = vec![0u8; DATA_LEN];
        let seed = RandomSeed::from(&[0u8; RANDOM_SEED_SIZE]);
        Crypto::random_buf_deterministic(&mut buf, &seed);
        let id = Eid::new();

        // write
        let now = Instant::now();
        write_to_entity(&id, &buf, &vol);
        let write_time = now.elapsed();

        // read
        let now = Instant::now();
        {
            let mut rdr = Reader::new(&id, &vol).unwrap();
            let mut dst = Vec::new();
            rdr.read_to_end(&mut dst).unwrap();
        }
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
        let vol = setup_mem_vol();
        perf_test(vol, "Memory volume");
    }

    #[test]
    fn file_perf() {
        let (vol, _tmpdir) = setup_file_vol("pwd", &Vec::new());
        perf_test(vol, "File volume");
    }
}
