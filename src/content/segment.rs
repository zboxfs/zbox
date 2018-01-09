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
    #[inline]
    fn id(&self) -> &Eid {
        &self.id
    }

    #[inline]
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
            chunks: Vec::new(),
        }
    }

    #[inline]
    pub fn data_id(&self) -> &Eid {
        &self.data_id
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

    #[inline]
    pub fn is_shrinkable(&self) -> bool {
        self.used < self.len >> 2
    }

    // create a new chunk and append to segment
    fn append_chunk(&mut self, data_len: usize) {
        let mut chunk = Chunk::new(self.len, data_len);
        chunk.inc_ref().unwrap(); // initialise reference count to 1
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

    fn deref_chunk(&mut self, idx: usize) -> Result<u32> {
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

    // shrink segment by creating a new segment data, return retired chunks
    // indices
    pub fn shrink(
        &mut self,
        seg_data_ref: &SegDataRef,
        vol: &VolumeRef,
    ) -> Result<Vec<usize>> {
        let seg_data = seg_data_ref.write().unwrap();
        let txid = Txid::current()?;
        let mut buf = Vec::new();
        let mut retired = Vec::new();

        debug!(
            "shrink segment {:#?} from {} to {}",
            self.id,
            self.len,
            self.used
        );

        // re-position chunks
        for (idx, chunk) in self.chunks.iter_mut().enumerate() {
            if chunk.is_orphan() {
                retired.push(idx);
            } else {
                buf.extend_from_slice(
                    &seg_data.data[chunk.pos..chunk.end_pos()],
                );
                chunk.pos = buf.len() - chunk.len;
            }
        }

        // write new segment data to volume
        let new_data_id = Eid::new();
        let mut data_wtr = Volume::writer(&new_data_id, txid, vol);
        data_wtr.write_all(&buf)?;

        // remove old segment data from volume
        SegData::remove(seg_data.id(), txid, vol)?;

        // update segment
        self.len = self.used;
        self.data_id = new_data_id;

        Ok(retired)
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
        if self.chunks.len() > 10 {
            for val in self.chunks[..3].iter() {
                Debug::fmt(val, f).unwrap();
                writeln!(f, ",").unwrap();
            }
            writeln!(f, "...{} chunks..", self.chunks.len() - 6).unwrap();
            for val in self.chunks[self.chunks.len() - 3..].iter() {
                Debug::fmt(val, f).unwrap();
                writeln!(f, ",").unwrap();
            }
        } else {
            for val in self.chunks.iter() {
                Debug::fmt(val, f).unwrap();
                writeln!(f, ",").unwrap();
            }
        }
        write!(f, "]")
    }
}

impl Index<usize> for Segment {
    type Output = Chunk;

    #[inline]
    fn index(&self, index: usize) -> &Chunk {
        &self.chunks[index]
    }
}

impl IndexMut<usize> for Segment {
    #[inline]
    fn index_mut(&mut self, index: usize) -> &mut Chunk {
        &mut self.chunks[index]
    }
}

impl Index<Range<usize>> for Segment {
    type Output = [Chunk];

    #[inline]
    fn index(&self, index: Range<usize>) -> &[Chunk] {
        &self.chunks[index]
    }
}

impl Id for Segment {
    #[inline]
    fn id(&self) -> &Eid {
        &self.id
    }

    #[inline]
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

    #[inline]
    pub fn seg(&self) -> SegRef {
        self.seg.clone()
    }

    pub fn save_seg(&self) -> Result<()> {
        let seg = self.seg.read().unwrap();
        seg.save_cow(self.txid, &self.vol)
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
        map_io_err!(seg.make_mut())?.append_chunk(chunk.len());
        self.data_wtr.write(chunk)
    }

    fn flush(&mut self) -> IoResult<()> {
        // As the auto-flush flag is set for underlying LZ4 data writer,
        // nothing need to do here
        Ok(())
    }
}

/// Segment cache
pub type Cache = CowCache<Segment>;

#[cfg(test)]
mod tests {
    use content::span::{Extent, Span};
    use content::entry::{CutableList, EntryList};
    use super::*;

    fn test_split_off(
        elst: &EntryList,
        seg_begin: &Segment,
        seg_end: &Segment,
    ) {
        // split at the beginning
        let mut dst = elst.clone();
        let split = dst.split_off(0, seg_begin);
        dst.check();
        split.check();
        assert!(dst.is_empty());
        assert_eq!(split.len(), elst.len());
        if !split.is_empty() {
            assert_eq!(split[0].seg_id(), elst[0].seg_id());
        }

        // split at the end
        let mut dst = elst.clone();
        let split = dst.split_off(elst.len(), seg_end);
        dst.check();
        split.check();
        assert_eq!(dst.len(), elst.len());
        assert!(split.is_empty());
        assert_eq!(split.offset(), elst.len());
    }

    fn test_split_to(elst: &EntryList, seg_begin: &Segment, seg_end: &Segment) {
        // split at the beginning
        let mut dst = elst.clone();
        let split = dst.split_to(0, seg_begin);
        dst.check();
        split.check();
        assert!(split.is_empty());
        assert_eq!(dst.len(), elst.len());

        // split at the end
        let mut dst = elst.clone();
        let split = dst.split_to(elst.len(), seg_end);
        dst.check();
        split.check();
        assert_eq!(split.len(), elst.len());
        assert!(dst.is_empty());
        assert_eq!(dst.offset(), elst.len());
    }

    fn single_span() {
        let mut seg = Segment::new();
        seg.append_chunk(10);
        let mut elst = EntryList::new();
        elst.append(seg.id(), &Span::new(0, 1, 0, 10, 0));
        test_split_off(&elst, &seg, &seg);
        test_split_to(&elst, &seg, &seg);

        // split off in the middle
        let half = elst.len() / 2;
        let mut dst = elst.clone();
        let split = dst.split_off(half, &seg);
        dst.check();
        split.check();
        assert_eq!(dst.len(), half);
        assert!(split.is_empty());
        assert_eq!(split.offset(), elst.len());

        // split off again
        let at = elst.len() / 3;
        let split = dst.split_off(at, &seg);
        dst.check();
        split.check();
        assert_eq!(dst.len(), at);
        assert!(split.is_empty());
        assert_eq!(split.offset(), elst.len());

        // split to in the middle
        let half = elst.len() / 2;
        let mut dst = elst.clone();
        let split = dst.split_to(half, &seg);
        dst.check();
        split.check();
        assert_eq!(dst.len(), half);
        assert_eq!(dst.offset(), half);
        assert!(split.is_empty());
        assert_eq!(split.offset(), 0);

        // split to again
        let at = half + elst.len() / 3;
        let split = dst.split_to(at, &seg);
        dst.check();
        split.check();
        assert_eq!(dst.offset(), at);
        assert_eq!(dst.len(), elst.len() - at);
        assert!(split.is_empty());
        assert_eq!(split.offset(), half);
    }

    fn multiple_spans() {
        let mut seg = Segment::new();
        seg.append_chunk(5);
        seg.append_chunk(5);
        seg.append_chunk(5);
        let mut elst = EntryList::new();
        elst.append(seg.id(), &Span::new(0, 1, 0, 5, 0));
        elst.append(seg.id(), &Span::new(2, 3, 0, 5, 5));
        test_split_off(&elst, &seg, &seg);
        test_split_to(&elst, &seg, &seg);

        // split off in the middle
        let half = elst.len() / 2;
        let mut dst = elst.clone();
        let split = dst.split_off(half, &seg);
        dst.check();
        split.check();
        assert_eq!(dst.len(), half);
        assert_eq!(dst.offset(), 0);
        assert_eq!(split.len(), half);
        assert_eq!(split.offset(), half);

        // split off at 1/3
        let at = elst.len() / 3;
        let mut dst = elst.clone();
        let split = dst.split_off(at, &seg);
        dst.check();
        split.check();
        assert_eq!(dst.len(), at);
        assert_eq!(dst.offset(), 0);
        assert_eq!(dst.iter().nth(0).unwrap().iter().count(), 1);
        assert_eq!(split.len(), 5);
        assert_eq!(split.offset(), 5);

        // split off at 2/3
        let at = elst.len() * 2 / 3;
        let mut dst = elst.clone();
        let split = dst.split_off(at, &seg);
        dst.check();
        split.check();
        assert_eq!(dst.len(), at);
        assert_eq!(dst.offset(), 0);
        assert_eq!(dst.iter().nth(0).unwrap().iter().count(), 2);
        assert_eq!(split.len(), 0);
        assert_eq!(split.offset(), elst.len());
        assert_eq!(split.iter().count(), 0);

        // split to in the middle
        let half = elst.len() / 2;
        let mut dst = elst.clone();
        let split = dst.split_to(half, &seg);
        dst.check();
        split.check();
        assert_eq!(dst.len(), half);
        assert_eq!(dst.offset(), half);
        assert_eq!(split.len(), half);
        assert_eq!(split.offset(), 0);

        // split to at 1/3
        let at = elst.len() / 3;
        let mut dst = elst.clone();
        let split = dst.split_to(at, &seg);
        dst.check();
        split.check();
        assert_eq!(dst.len(), elst.len() - at);
        assert_eq!(dst.offset(), at);
        assert_eq!(dst.iter().nth(0).unwrap().iter().count(), 2);
        assert_eq!(split.len(), 0);
        assert_eq!(split.offset(), 0);
        assert_eq!(split.iter().count(), 0);

        // split to at 2/3
        let at = elst.len() * 2 / 3;
        let mut dst = elst.clone();
        let split = dst.split_to(at, &seg);
        dst.check();
        split.check();
        assert_eq!(dst.len(), elst.len() - at);
        assert_eq!(dst.offset(), at);
        assert_eq!(dst.iter().nth(0).unwrap().iter().count(), 1);
        assert_eq!(split.len(), 5);
        assert_eq!(split.offset(), 0);
        assert_eq!(split.iter().nth(0).unwrap().iter().count(), 1);
    }

    fn multiple_segs_spans() {
        let mut seg = Segment::new();
        seg.append_chunk(5);
        seg.append_chunk(5);
        seg.append_chunk(5);
        let mut seg2 = Segment::new();
        seg2.append_chunk(5);
        seg2.append_chunk(5);
        seg2.append_chunk(5);
        seg2.append_chunk(5);
        let mut elst = EntryList::new();
        elst.append(seg.id(), &Span::new(0, 1, 0, 5, 0));
        elst.append(seg.id(), &Span::new(2, 3, 0, 5, 5));
        elst.append(seg2.id(), &Span::new(1, 2, 0, 5, 10));
        elst.append(seg2.id(), &Span::new(3, 4, 0, 5, 15));
        test_split_off(&elst, &seg, &seg2);
        test_split_to(&elst, &seg, &seg2);

        // split off in the middle
        let half = elst.len() / 2;
        let mut dst = elst.clone();
        let split = dst.split_off(half, &seg);
        dst.check();
        split.check();
        assert_eq!(dst.len(), half);
        assert_eq!(dst.offset(), 0);
        assert_eq!(dst.iter().count(), 1);
        assert_eq!(dst.iter().nth(0).unwrap().iter().count(), 2);
        assert_eq!(split.len(), half);
        assert_eq!(split.offset(), half);
        assert_eq!(split.iter().count(), 1);
        assert_eq!(split.iter().nth(0).unwrap().iter().count(), 2);

        // split off at 1/3
        let at = elst.len() / 3;
        let mut dst = elst.clone();
        let split = dst.split_off(at, &seg);
        dst.check();
        split.check();
        assert_eq!(dst.len(), at);
        assert_eq!(dst.offset(), 0);
        assert_eq!(dst.iter().count(), 1);
        assert_eq!(dst.iter().nth(0).unwrap().iter().count(), 2);
        assert_eq!(split.len(), 10);
        assert_eq!(split.offset(), 10);
        assert_eq!(split.iter().count(), 1);
        assert_eq!(split.iter().nth(0).unwrap().iter().count(), 2);

        // split off at 2/3
        let at = elst.len() * 2 / 3;
        let mut dst = elst.clone();
        let split = dst.split_off(at, &seg2);
        dst.check();
        split.check();
        assert_eq!(dst.len(), at);
        assert_eq!(dst.offset(), 0);
        assert_eq!(dst.iter().count(), 2);
        assert_eq!(dst.iter().nth(0).unwrap().iter().count(), 2);
        assert_eq!(dst.iter().nth(1).unwrap().iter().count(), 1);
        assert_eq!(split.len(), 5);
        assert_eq!(split.offset(), 15);
        assert_eq!(split.iter().count(), 1);
        assert_eq!(split.iter().nth(0).unwrap().iter().count(), 1);

        // split to in the middle
        let half = elst.len() / 2;
        let mut dst = elst.clone();
        let split = dst.split_to(half, &seg);
        dst.check();
        split.check();
        assert_eq!(dst.len(), half);
        assert_eq!(dst.offset(), half);
        assert_eq!(split.len(), half);
        assert_eq!(split.offset(), 0);

        // split to at 1/3
        let at = elst.len() / 3;
        let mut dst = elst.clone();
        let split = dst.split_to(at, &seg);
        dst.check();
        split.check();
        assert_eq!(dst.len(), elst.len() - at);
        assert_eq!(dst.offset(), at);
        assert_eq!(dst.iter().count(), 2);
        assert_eq!(dst.iter().nth(0).unwrap().iter().count(), 1);
        assert_eq!(dst.iter().nth(1).unwrap().iter().count(), 2);
        assert_eq!(split.len(), 5);
        assert_eq!(split.offset(), 0);
        assert_eq!(split.iter().count(), 1);

        // split to at 2/3
        let at = elst.len() * 2 / 3;
        let mut dst = elst.clone();
        let split = dst.split_to(at, &seg2);
        dst.check();
        split.check();
        assert_eq!(dst.len(), elst.len() - at);
        assert_eq!(dst.offset(), at);
        assert_eq!(dst.iter().nth(0).unwrap().iter().count(), 2);
        assert_eq!(dst.iter().nth(0).unwrap().seg_id(), seg2.id());
        assert_eq!(split.len(), 10);
        assert_eq!(split.offset(), 0);
        assert_eq!(split.iter().nth(0).unwrap().iter().count(), 2);
        assert_eq!(split.iter().nth(0).unwrap().seg_id(), seg.id());
    }

    #[test]
    fn split_entry_list() {
        single_span();
        multiple_spans();
        multiple_segs_spans();
    }
}
