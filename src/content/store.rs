use std::collections::HashMap;
use std::fmt::{self, Debug};
use std::io::{Result as IoResult, Seek, SeekFrom, Write};

use super::chunk::ChunkMap;
use super::chunker::{Chunker, ChunkerParams};
use super::content::{
    Cache as ContentCache, ContentRef, Writer as ContentWriter,
};
use super::segment::{
    Cache as SegCache, DataCache as SegDataCache, SegData, SegDataRef, SegRef,
    Segment,
};
use super::Content;
use base::crypto::Hash;
use base::RefCnt;
use error::{Error, Result};
use trans::cow::{Cow, CowRef, Cowable, IntoCow};
use trans::trans::Action;
use trans::{Eid, Id, TxMgrRef, Txid};
use volume::VolumeRef;

/// Content map entry
#[derive(Debug, Clone, Deserialize, Serialize)]
struct ContentMapEntry {
    content_id: Eid,
    refcnt: RefCnt,
}

impl ContentMapEntry {
    fn new() -> Self {
        ContentMapEntry {
            content_id: Eid::new_empty(),
            refcnt: RefCnt::new(),
        }
    }

    #[inline]
    fn inc_ref(&mut self) -> Result<u32> {
        self.refcnt.inc_ref()
    }

    #[inline]
    fn dec_ref(&mut self) -> Result<u32> {
        self.refcnt.dec_ref()
    }
}

/// Content Store
#[derive(Default, Clone, Deserialize, Serialize)]
pub struct Store {
    chunker_params: ChunkerParams,
    content_map: HashMap<Hash, ContentMapEntry>,

    #[serde(skip_serializing, skip_deserializing, default)]
    content_cache: ContentCache,

    #[serde(skip_serializing, skip_deserializing, default)]
    seg_cache: SegCache,

    #[serde(skip_serializing, skip_deserializing, default)]
    segdata_cache: SegDataCache,

    #[serde(skip_serializing, skip_deserializing, default)]
    txmgr: TxMgrRef,

    #[serde(skip_serializing, skip_deserializing, default)]
    vol: VolumeRef,
}

impl Store {
    // segment cache size
    const SEG_CACHE_SIZE: usize = 16;

    // segment data cache size, in bytes
    const SEG_DATA_CACHE_SIZE: usize = 16 * 1024 * 1024;

    // default content cache size
    const CONTENT_CACHE_SIZE: usize = 16;

    pub fn new(txmgr: &TxMgrRef, vol: &VolumeRef) -> Self {
        Store {
            chunker_params: ChunkerParams::new(),
            content_map: HashMap::new(),
            content_cache: ContentCache::new(Self::CONTENT_CACHE_SIZE, txmgr),
            seg_cache: SegCache::new(Self::SEG_CACHE_SIZE, txmgr),
            segdata_cache: SegDataCache::new(Self::SEG_DATA_CACHE_SIZE),
            txmgr: txmgr.clone(),
            vol: vol.clone(),
        }
    }

    pub fn open(
        store_id: &Eid,
        txmgr: &TxMgrRef,
        vol: &VolumeRef,
    ) -> Result<StoreRef> {
        let store = Cow::<Store>::load(store_id, txmgr, vol)?;
        {
            let mut store_cow = store.write().unwrap();
            let store = store_cow.make_mut_naive();
            store.content_cache =
                ContentCache::new(Self::CONTENT_CACHE_SIZE, txmgr);
            store.seg_cache = SegCache::new(Self::SEG_CACHE_SIZE, txmgr);
            store.segdata_cache = SegDataCache::new(Self::SEG_DATA_CACHE_SIZE);
            store.txmgr = txmgr.clone();
            store.vol = vol.clone();
        }
        Ok(store)
    }

    #[inline]
    pub fn get_seg(&self, seg_id: &Eid) -> Result<SegRef> {
        self.seg_cache.get(seg_id, &self.vol)
    }

    // inject intermediate segment to segment cache
    #[inline]
    pub fn inject_seg_to_cache(&self, seg: &SegRef) {
        self.seg_cache.insert(seg)
    }

    #[inline]
    pub fn get_segdata(&self, segdata_id: &Eid) -> Result<SegDataRef> {
        self.segdata_cache.get(segdata_id, &self.vol)
    }

    #[inline]
    pub fn get_content(&self, content_id: &Eid) -> Result<ContentRef> {
        self.content_cache.get(content_id, &self.vol)
    }

    /// Dedup content based on its hash
    pub fn dedup_content(&mut self, content: &Content) -> Result<(bool, Eid)> {
        let mut deduped = true;
        let ent = self
            .content_map
            .entry(content.hash().clone())
            .or_insert_with(|| ContentMapEntry::new());
        ent.inc_ref()?;
        if ent.content_id.is_empty() {
            // no duplication found
            let ctn = content.clone().into_cow(&self.txmgr)?;
            let ctn = ctn.read().unwrap();
            ent.content_id = ctn.id().clone();
            deduped = false;
        }
        Ok((deduped, ent.content_id.clone()))
    }

    /// Decrease content reference in store
    ///
    /// If the content is not used anymore, remove and return it.
    pub fn deref_content(
        &mut self,
        content_id: &Eid,
    ) -> Result<Option<ContentRef>> {
        {
            let ctn_ref = self.get_content(content_id)?;
            let ctn = ctn_ref.read().unwrap();
            let refcnt = self
                .content_map
                .get_mut(ctn.hash())
                .ok_or(Error::NoContent)
                .and_then(|ent| ent.dec_ref().map_err(Error::from))?;
            if refcnt > 0 {
                return Ok(None);
            }
            self.content_map.remove(ctn.hash()).unwrap();
        }

        Ok(self.content_cache.remove(content_id))
    }

    /// Remove segment and its associated segment data
    pub fn remove_segment(&mut self, seg_cow: &mut Cow<Segment>) -> Result<()> {
        // add segment to tx for deletion
        seg_cow.make_del()?;

        // add segment data to transaction for deletion
        SegData::add_to_trans(
            seg_cow.data_id(),
            Action::Delete,
            Txid::current()?,
            &self.txmgr,
        )?;

        // remove seg and seg data from cache
        self.segdata_cache.remove(seg_cow.data_id());
        self.seg_cache.remove(seg_cow.id());

        Ok(())
    }

    /// Shrink segment
    pub fn shrink_segment(
        &mut self,
        seg_cow: &mut Cow<Segment>,
    ) -> Result<Vec<usize>> {
        let txid = Txid::current()?;

        // add segment to tx for update
        let seg = seg_cow.make_mut()?;

        // load old segment data and then remove it from cache
        let seg_data = {
            self.segdata_cache.get(seg.data_id(), &self.vol)?;
            self.segdata_cache.remove(seg.data_id()).unwrap()
        };

        // add old segment data to transaction for deletion
        SegData::add_to_trans(
            seg.data_id(),
            Action::Delete,
            txid,
            &self.txmgr,
        )?;

        // shrink segment
        seg.shrink(&seg_data, txid, &self.txmgr, &self.vol)
    }
}

impl Debug for Store {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("Store")
            .field("content_map", &self.content_map)
            .finish()
    }
}

impl Cowable for Store {}

impl<'de> IntoCow<'de> for Store {}

/// Store reference type
pub type StoreRef = CowRef<Store>;

/// Store Writer
#[derive(Debug)]
pub struct Writer {
    inner: Chunker<ContentWriter>,
}

impl Writer {
    pub fn new(
        chk_map: ChunkMap,
        txmgr: &TxMgrRef,
        store: &StoreRef,
        txid: Txid,
    ) -> Self {
        let st = store.read().unwrap();
        let ctn_wtr = ContentWriter::new(chk_map, store, txid, txmgr, &st.vol);
        Writer {
            inner: Chunker::new(st.chunker_params.clone(), ctn_wtr),
        }
    }

    pub fn finish(self) -> Result<(Content, ChunkMap)> {
        let ctn_wtr = self.inner.into_inner()?;
        ctn_wtr.finish()
    }
}

impl Write for Writer {
    fn write(&mut self, buf: &[u8]) -> IoResult<usize> {
        self.inner.write(buf)
    }

    fn flush(&mut self) -> IoResult<()> {
        self.inner.flush()
    }
}

impl Seek for Writer {
    fn seek(&mut self, pos: SeekFrom) -> IoResult<u64> {
        self.inner.seek(pos)
    }
}
