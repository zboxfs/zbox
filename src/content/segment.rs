use std::error::Error as StdError;
use std::fmt::{self, Debug};
use std::io::{Error as IoError, ErrorKind, Read, Result as IoResult, Write};
use std::ops::{Index, IndexMut, Range};
use std::sync::{Arc, RwLock};

use super::chunk::Chunk;
use super::StoreRef;
use base::lru::{Lru, Meter, PinChecker};
use base::IntoRef;
use error::Result;
use trans::cow::{CowCache, CowRef, Cowable, IntoCow};
use trans::trans::{Action, Transable};
use trans::{Eid, EntityType, Finish, Id, TxMgrRef, Txid};
use volume::{Arm, Reader as VolReader, VolumeRef, Writer as VolWriter};

/// Segment Data
#[derive(Default)]
pub struct SegData {
    id: Eid,
    action: Option<Action>,
    data: Vec<u8>,
}

impl SegData {
    fn new(id: &Eid) -> Self {
        SegData {
            id: id.clone(),
            action: None,
            data: Vec::new(),
        }
    }

    // Note: offset is in the segment data
    #[inline]
    pub fn read(&self, dst: &mut [u8], offset: usize) -> usize {
        let read_len = dst.len();
        assert!(offset + read_len <= self.data.len());
        dst.copy_from_slice(&self.data[offset..(offset + read_len)]);
        read_len
    }

    pub fn add_to_trans(
        data_id: &Eid,
        action: Action,
        txid: Txid,
        txmgr: &TxMgrRef,
    ) -> Result<()> {
        let mut seg_data = Self::new(data_id); // dummy segment data
        seg_data.action = Some(action);
        let mut txmgr = txmgr.write().unwrap();
        txmgr.add_to_trans(
            data_id,
            txid,
            seg_data.into_ref(),
            action,
            EntityType::Direct,
            Arm::default(),
        )
    }

    fn load(id: &Eid, vol: &VolumeRef) -> Result<Self> {
        let mut rdr = VolReader::new(id, vol)?;
        let mut buf = Vec::new();
        rdr.read_to_end(&mut buf)?;

        Ok(SegData {
            id: id.clone(),
            action: None,
            data: buf,
        })
    }

    fn save(&self, vol: &VolumeRef) -> Result<()> {
        let mut wtr = VolWriter::new(&self.id, vol)?;
        wtr.write_all(&self.data[..])?;
        wtr.finish()?;
        Ok(())
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

impl Transable for SegData {
    #[inline]
    fn action(&self) -> Action {
        self.action.clone().unwrap()
    }

    fn commit(&mut self, _vol: &VolumeRef) -> Result<()> {
        match self.action {
            Some(action) => match action {
                Action::New => {
                    // do nothing here, as segment data is already
                    // written to volume directly
                    Ok(())
                }
                Action::Update => unreachable!(), // segment data never update
                Action::Delete => {
                    // do nothing here, actual deletion will be
                    // delayed after 2 txs
                    Ok(())
                }
            },
            None => unreachable!(),
        }
    }

    #[inline]
    fn complete_commit(&mut self) {
        self.action = None;
    }

    #[inline]
    fn abort(&mut self) {
        self.action = None;
    }
}

impl Debug for SegData {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("SegData")
            .field("id", &self.id)
            .field("action", &self.action)
            .field("data_len", &self.data.len())
            .finish()
    }
}

/// Segment data reference type
pub type SegDataRef = Arc<RwLock<SegData>>;

// Segment data meter, measured by segment data bytes size
#[derive(Debug, Default)]
struct SegDataMeter;

impl Meter<SegDataRef> for SegDataMeter {
    fn measure(&self, item: &SegDataRef) -> isize {
        let seg_data = item.read().unwrap();
        seg_data.data.len() as isize
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
        DataCache {
            lru: Arc::new(RwLock::new(SegDataLru::new(capacity))),
        }
    }

    pub fn get(&self, id: &Eid, vol: &VolumeRef) -> Result<SegDataRef> {
        let mut lru = self.lru.write().unwrap();

        // get from cache first
        if let Some(val) = lru.get_refresh(id) {
            return Ok(val.clone());
        }

        // if not in cache, load it from volume
        // then insert into cache
        let ent = SegData::load(id, vol)?.into_ref();
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
    len: usize,  // segment data length, in bytes,
    used: usize, // currently used segment data length, in bytes
    data_id: Eid,
    chunks: Vec<Chunk>,
}

impl Segment {
    // maximum number of chunks in a segment
    const MAX_CHUNKS: usize = 256;

    fn new() -> Self {
        Segment {
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
        self.chunks.len() >= Self::MAX_CHUNKS
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
        let chunk = Chunk::new(self.len, data_len);
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
        txid: Txid,
        txmgr: &TxMgrRef,
        vol: &VolumeRef,
    ) -> Result<Vec<usize>> {
        let mut buf = Vec::new();
        let mut retired = Vec::new();

        debug!("shrink segment from {} to {}", self.len, self.used);

        // re-position chunks
        let seg_data = seg_data_ref.read().unwrap();
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
        assert_eq!(buf.len(), self.used);

        // write new segment data to volume
        // and add a dummy segment data to transaction
        let new_data_id = Eid::new();
        let new_seg_data = SegData::new(&new_data_id);
        new_seg_data.save(vol)?;
        SegData::add_to_trans(&new_data_id, Action::New, txid, txmgr)?;

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
            "Segment(len: {}, used: {}, data_id: {:?}, [\n",
            self.len, self.used, self.data_id
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

impl Cowable for Segment {}

impl<'de> IntoCow<'de> for Segment {}

/// Segment reference type
pub type SegRef = CowRef<Segment>;

/// Segment Writer
#[derive(Debug)]
pub struct Writer {
    seg: SegRef,
    data_wtr: Option<VolWriter>, // segment data writer
    txid: Txid,
    txmgr: TxMgrRef,
    store: StoreRef,
    vol: VolumeRef,
}

impl Writer {
    pub fn new(
        txid: Txid,
        store: &StoreRef,
        txmgr: &TxMgrRef,
        vol: &VolumeRef,
    ) -> Self {
        Writer {
            seg: Arc::default(),
            data_wtr: None,
            txid,
            txmgr: txmgr.clone(),
            store: store.clone(),
            vol: vol.clone(),
        }
    }

    #[inline]
    pub fn seg(&self) -> SegRef {
        self.seg.clone()
    }

    pub fn renew(&mut self) -> Result<()> {
        // create a new segment
        let seg = Segment::new();

        // add segment data to tx, this is a dummy as the actual data will
        // directly write using volume writer instead of writing to the
        // segment data itself
        SegData::add_to_trans(
            &seg.data_id,
            Action::New,
            self.txid,
            &self.txmgr,
        )?;

        // if this is not the first-time renew, finish the last segment data
        // writer first
        if let Some(data_wtr) = self.data_wtr.take() {
            data_wtr.finish()?;
        }

        // and then create a new segment data writer and add segment to tx
        self.data_wtr = Some(VolWriter::new(&seg.data_id, &self.vol)?);
        self.seg = seg.into_cow(&self.txmgr)?;

        // inject segment to segment cache in store
        let store = self.store.read().unwrap();
        store.inject_seg_to_cache(&self.seg);

        Ok(())
    }
}

impl Write for Writer {
    fn write(&mut self, chunk: &[u8]) -> IoResult<usize> {
        // create segment and segment data if they are not created yet
        if self.data_wtr.is_none() {
            map_io_err!(self.renew())?;
        }

        let mut seg = self.seg.write().unwrap();
        if seg.is_full() {
            return Ok(0);
        }

        // write whole chunk directly to segment data
        match self.data_wtr {
            Some(ref mut data_wtr) => data_wtr.write_all(chunk)?,
            None => unreachable!(),
        }

        // and then append chunk to segment
        map_io_err!(seg.make_mut())?.append_chunk(chunk.len());

        Ok(chunk.len())
    }

    fn flush(&mut self) -> IoResult<()> {
        // nothing need to do here, use finish() to finish writing
        Ok(())
    }
}

impl Finish for Writer {
    fn finish(self) -> Result<()> {
        match self.data_wtr {
            Some(data_wtr) => data_wtr.finish(),
            None => Ok(()),
        }
    }

    fn finish_and_flush(self) -> Result<()> {
        match self.data_wtr {
            Some(data_wtr) => data_wtr.finish_and_flush(),
            None => Ok(()),
        }
    }
}

/// Segment cache
pub type Cache = CowCache<Segment>;

#[cfg(test)]
mod tests {
    use super::*;
    use base::init_env;
    use content::entry::{CutableList, EntryList};
    use content::span::{Extent, Span};

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
        let seg_id = Eid::new();
        let mut seg = Segment::new();
        seg.append_chunk(10);
        let mut elst = EntryList::new();
        elst.append(&seg_id, &Span::new(0, 1, 0, 10, 0));
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
        let seg_id = Eid::new();
        let mut seg = Segment::new();
        seg.append_chunk(5);
        seg.append_chunk(5);
        seg.append_chunk(5);
        let mut elst = EntryList::new();
        elst.append(&seg_id, &Span::new(0, 1, 0, 5, 0));
        elst.append(&seg_id, &Span::new(2, 3, 0, 5, 5));
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
        let seg_id = Eid::new();
        let seg2_id = Eid::new();
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
        elst.append(&seg_id, &Span::new(0, 1, 0, 5, 0));
        elst.append(&seg_id, &Span::new(2, 3, 0, 5, 5));
        elst.append(&seg2_id, &Span::new(1, 2, 0, 5, 10));
        elst.append(&seg2_id, &Span::new(3, 4, 0, 5, 15));
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
        assert_eq!(dst.iter().nth(0).unwrap().seg_id(), &seg2_id);
        assert_eq!(split.len(), 10);
        assert_eq!(split.offset(), 0);
        assert_eq!(split.iter().nth(0).unwrap().iter().count(), 2);
        assert_eq!(split.iter().nth(0).unwrap().seg_id(), &seg_id);
    }

    #[test]
    fn split_entry_list() {
        init_env();
        single_span();
        multiple_spans();
        multiple_segs_spans();
    }
}
