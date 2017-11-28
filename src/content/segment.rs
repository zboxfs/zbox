use std::fmt::{self, Debug};
use std::sync::{Arc, RwLock};
use std::ops::{Index, IndexMut, Range};
use std::io::{Read, Write, Error as IoError, ErrorKind, Result as IoResult};
use std::error::Error as StdError;

use error::Result;
use base::IntoRef;
use base::lru::{Lru, Meter, PinChecker};
use trans::{Eid, Id, CloneNew, TxMgrRef, Txid};
use trans::cow::{CowRef, IntoCow, CowCache};
use volume::{Volume, VolumeRef, Persistable, Writer as VolWriter};
use super::chunk::Chunk;

// maximum number of chunks in a segment
const MAX_CHUNKS: usize = 256;

/// Segment Data
#[derive(Debug, Default, Clone, Deserialize, Serialize)]
pub struct SegData {
    id: Eid,
    data: Vec<u8>,
}

impl SegData {
    // Note: offset is in the segment data
    pub fn read(&self, dst: &mut [u8], offset: usize) -> usize {
        let read_len = dst.len();
        assert!(offset + read_len <= self.data.len());
        dst.copy_from_slice(&self.data[offset..(offset + read_len)]);
        read_len
    }
}

impl Id for SegData {
    fn id(&self) -> &Eid {
        &self.id
    }

    fn id_mut(&mut self) -> &mut Eid {
        &mut self.id
    }
}

impl IntoRef for SegData {}

impl<'de> Persistable<'de> for SegData {
    fn load(id: &Eid, txid: Txid, vol: &VolumeRef) -> Result<Self> {
        let mut data = Vec::new();
        Volume::reader(id, txid, vol).read_to_end(&mut data)?;
        Ok(SegData {
            id: id.clone(),
            data,
        })
    }

    fn save(&self, txid: Txid, vol: &VolumeRef) -> Result<()> {
        let mut writer = Volume::writer(self.id(), txid, vol);
        writer.write_all(&self.data)?;
        Ok(())
    }

    fn remove(id: &Eid, txid: Txid, vol: &VolumeRef) -> Result<Option<Eid>> {
        let mut vol = vol.write().unwrap();
        vol.del(id, txid)
    }
}

/// Segment data reference type
pub type SegDataRef = Arc<RwLock<SegData>>;

// Segment data meter, measured by segment data bytes size
#[derive(Debug)]
pub struct SegDataMeter;

impl Meter<SegDataRef> for SegDataMeter {
    fn measure(&self, item: &SegDataRef) -> isize {
        let seg_data = item.read().unwrap();
        seg_data.data.len() as isize
    }
}

impl Default for SegDataMeter {
    fn default() -> Self {
        SegDataMeter {}
    }
}

// Segment data LRU
type SegDataLru = Lru<Eid, SegDataRef, SegDataMeter, PinChecker<SegDataRef>>;

/// Segment data cache
#[derive(Debug, Clone, Default)]
pub struct DataCache {
    lru: Arc<RwLock<SegDataLru>>,
}

impl DataCache {
    pub fn new(capacity: usize) -> Self {
        DataCache { lru: Arc::new(RwLock::new(SegDataLru::new(capacity))) }
    }

    pub fn get(&self, id: &Eid, vol: &VolumeRef) -> Result<SegDataRef> {
        let mut lru = self.lru.write().unwrap();

        // get from cache first
        if let Some(val) = lru.get_refresh(id) {
            return Ok(val.clone());
        }

        // if not in cache, load it from volume
        // then insert into cache
        let txid = Txid::current_or_empty();
        let ent = SegData::load(id, txid, vol)?.into_ref();
        lru.insert(id.clone(), ent.clone());

        Ok(ent)
    }

    pub fn remove(&self, id: &Eid) -> Option<SegDataRef> {
        let mut lru = self.lru.write().unwrap();
        lru.remove(id)
    }
}

/// Segment
#[derive(Clone, Default, Deserialize, Serialize)]
pub struct Segment {
    id: Eid,
    len: usize, // segment data length, in bytes,
    used: usize, // currently used segment data length, in bytes
    data_id: Eid,
    chunks: Vec<Chunk>,
}

impl Segment {
    pub fn new() -> Self {
        Segment {
            id: Eid::new(),
            len: 0,
            used: 0,
            data_id: Eid::new(),
            chunks: Vec::with_capacity(MAX_CHUNKS),
        }
    }

    #[inline]
    pub fn seg_data_id(&self) -> &Eid {
        &self.data_id
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.len
    }

    #[inline]
    pub fn chunk_cnt(&self) -> usize {
        self.chunks.len()
    }

    #[inline]
    pub fn is_full(&self) -> bool {
        self.chunks.len() >= MAX_CHUNKS
    }

    #[inline]
    pub fn is_orphan(&self) -> bool {
        self.used == 0
    }

    // append chunk data to segment
    pub fn append(&mut self, data: &[u8]) {
        let data_len = data.len();
        let mut chunk = Chunk::new(self.len, data_len);
        chunk.inc_ref().unwrap();
        self.chunks.push(chunk);
        self.len += data_len;
        self.used += data_len;
    }

    pub fn ref_chunk(&mut self, idx: usize) -> Result<u32> {
        let refcnt = self[idx].inc_ref()?;
        if refcnt == 1 {
            self.used += self[idx].len;
        }
        Ok(refcnt)
    }

    pub fn ref_chunks(&mut self, range: Range<usize>) -> Result<()> {
        for idx in range {
            self.ref_chunk(idx)?;
        }
        Ok(())
    }

    pub fn deref_chunk(&mut self, idx: usize) -> Result<u32> {
        let refcnt = self[idx].dec_ref()?;
        if refcnt == 0 {
            self.used -= self[idx].len;
        }
        Ok(refcnt)
    }

    pub fn deref_chunks(&mut self, range: Range<usize>) -> Result<()> {
        for idx in range {
            self.deref_chunk(idx)?;
        }
        Ok(())
    }
}

impl Debug for Segment {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "Segment(id: {:?}, len: {}, used: {}, data_id: {:?}, [\n",
            self.id,
            self.len,
            self.used,
            self.data_id
        ).unwrap();
        for val in self.chunks.iter() {
            Debug::fmt(val, f).unwrap();
            writeln!(f, ",").unwrap();
        }
        write!(f, "]")
    }
}

impl Index<usize> for Segment {
    type Output = Chunk;

    fn index(&self, index: usize) -> &Chunk {
        &self.chunks[index]
    }
}

impl IndexMut<usize> for Segment {
    fn index_mut(&mut self, index: usize) -> &mut Chunk {
        &mut self.chunks[index]
    }
}

impl Id for Segment {
    fn id(&self) -> &Eid {
        &self.id
    }

    fn id_mut(&mut self) -> &mut Eid {
        &mut self.id
    }
}

impl CloneNew for Segment {}

impl<'de> IntoCow<'de> for Segment {}

impl<'de> Persistable<'de> for Segment {}

/// Segment reference type
pub type SegRef = CowRef<Segment>;

/// Segment Writer
#[derive(Debug)]
pub struct Writer {
    seg: SegRef,
    data_wtr: VolWriter, // writer for segment data
    txid: Txid,
    txmgr: TxMgrRef,
    vol: VolumeRef,
}

impl Writer {
    pub fn new(txid: Txid, txmgr: &TxMgrRef, vol: &VolumeRef) -> Result<Self> {
        let seg = Segment::new();
        let data_wtr = Volume::writer(&seg.data_id, txid, vol);

        Ok(Writer {
            seg: seg.into_cow(&txmgr)?,
            data_wtr,
            txid,
            txmgr: txmgr.clone(),
            vol: vol.clone(),
        })
    }

    pub fn seg(&self) -> SegRef {
        self.seg.clone()
    }

    pub fn save_seg(&self) -> Result<()> {
        let seg = self.seg.read().unwrap();
        seg.save(self.txid, &self.vol)
    }

    pub fn renew(&mut self) -> Result<()> {
        // save current segment
        self.save_seg()?;

        // create new segment and segment data
        let seg = Segment::new();
        self.data_wtr = Volume::writer(&seg.data_id, self.txid, &self.vol);
        self.seg = seg.into_cow(&self.txmgr)?;

        Ok(())
    }
}

impl Write for Writer {
    fn write(&mut self, chunk: &[u8]) -> IoResult<usize> {
        let mut seg = self.seg.write().unwrap();
        if seg.is_full() {
            return Ok(0);
        }
        map_io_err!(seg.make_mut())?.append(chunk);
        self.data_wtr.write(chunk)
    }

    fn flush(&mut self) -> IoResult<()> {
        // As the auto-flush flag is set for underneath LZ4 data writer,
        // nothing need to do here
        Ok(())
    }
}

/// Segment cache
pub type Cache = CowCache<Segment>;
