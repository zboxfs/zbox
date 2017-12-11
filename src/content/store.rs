use std::fmt::{self, Debug};
use std::io::{Write, Result as IoResult, Seek, SeekFrom};
use std::collections::HashMap;

use error::{Error, Result};
use base::RefCnt;
use base::crypto::Hash;
use trans::{Eid, Id, CloneNew, TxMgrRef, Txid};
use trans::cow::{Cow, CowRef, IntoCow};
use volume::{VolumeRef, Persistable};
use super::Content;
use super::content::{ContentRef, Cache as ContentCache,
                     Writer as ContentWriter};
use super::chunk::ChunkMap;
use super::chunker::{ChunkerParams, Chunker};
use super::segment::{SegRef, SegDataRef, Cache as SegCache,
                     DataCache as SegDataCache};

// segment cache size
const SEG_CACHE_SIZE: usize = 16;

// segment data cache size, in bytes
const SEG_DATA_CACHE_SIZE: usize = 16 * 1024 * 1024;

// default content cache size
const CONTENT_CACHE_SIZE: usize = 16;

// Content map entry
#[derive(Debug, Clone, Deserialize, Serialize)]
struct ContentMapEntry {
    content_id: Eid,
    refcnt: RefCnt,
}

impl ContentMapEntry {
    fn new(content_id: &Eid) -> Self {
        ContentMapEntry {
            content_id: content_id.clone(),
            refcnt: RefCnt::new(),
        }
    }
}

/// Content Store
#[derive(Default, Clone, Deserialize, Serialize)]
pub struct Store {
    id: Eid,
    chunker_params: ChunkerParams,
    content_map: HashMap<Hash, ContentMapEntry>,

    #[serde(skip_serializing, skip_deserializing, default)]
    content_cache: ContentCache,

    #[serde(skip_serializing, skip_deserializing, default)]
    seg_cache: SegCache,

    #[serde(skip_serializing, skip_deserializing, default)]
    segdata_cache: SegDataCache,

    #[serde(skip_serializing, skip_deserializing, default)]
    vol: VolumeRef,
}

impl Store {
    pub fn new(txmgr: &TxMgrRef, vol: &VolumeRef) -> Self {
        Store {
            id: Eid::new(),
            chunker_params: ChunkerParams::new(),
            content_map: HashMap::new(),
            content_cache: ContentCache::new(CONTENT_CACHE_SIZE, txmgr),
            seg_cache: SegCache::new(SEG_CACHE_SIZE, txmgr),
            segdata_cache: SegDataCache::new(SEG_DATA_CACHE_SIZE),
            vol: vol.clone(),
        }
    }

    pub fn load_store(
        store_id: &Eid,
        txmgr: &TxMgrRef,
        vol: &VolumeRef,
    ) -> Result<StoreRef> {
        let store = Cow::<Store>::load(store_id, txmgr, vol)?;
        {
            let mut store_cow = store.write().unwrap();
            let store = store_cow.make_mut_naive()?;
            store.content_cache = ContentCache::new(CONTENT_CACHE_SIZE, txmgr);
            store.seg_cache = SegCache::new(SEG_CACHE_SIZE, txmgr);
            store.segdata_cache = SegDataCache::new(SEG_DATA_CACHE_SIZE);
            store.vol = vol.clone();
        }
        Ok(store)
    }

    pub fn get_seg(&self, seg_id: &Eid) -> Result<SegRef> {
        Ok(self.seg_cache.get(seg_id, &self.vol)?)
    }

    pub fn get_segdata(&self, segdata_id: &Eid) -> Result<SegDataRef> {
        self.segdata_cache.get(segdata_id, &self.vol)
    }

    pub fn get_content(&self, content_id: &Eid) -> Result<ContentRef> {
        Ok(self.content_cache.get(content_id, &self.vol)?)
    }

    /// Dedup content based on its hash
    pub fn dedup_content(
        &mut self,
        content_id: &Eid,
        hash: &Hash,
    ) -> Result<Eid> {
        let ent = self.content_map.entry(hash.clone()).or_insert_with(|| {
            ContentMapEntry::new(&content_id)
        });
        ent.refcnt.inc_ref()?;
        Ok(ent.content_id.clone())
    }

    /// Decrease content reference,
    /// if the content is not used anymore, remove and return it
    pub fn deref_content(
        &mut self,
        content_id: &Eid,
    ) -> Result<Option<ContentRef>> {
        {
            let ctn_ref = self.get_content(content_id)?;
            let ctn = ctn_ref.read().unwrap();
            let refcnt = self.content_map
                .get_mut(ctn.hash())
                .ok_or(Error::NoContent)
                .and_then(
                    |ent| ent.refcnt.dec_ref().map_err(|e| Error::from(e)),
                )?;
            if refcnt > 0 {
                return Ok(None);
            }
            let _ = self.content_map.remove(ctn.hash()).unwrap();
        }

        Ok(self.content_cache.remove(content_id))
    }

    /// Remove segment and its associated segment data
    pub fn remove_segment(&mut self, seg_id: &Eid, segdata_id: &Eid) {
        self.segdata_cache.remove(segdata_id);
        self.seg_cache.remove(seg_id);
    }
}

impl Debug for Store {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("Store")
            .field("id", &self.id)
            .field("content_map", &self.content_map)
            .finish()
    }
}

impl Id for Store {
    fn id(&self) -> &Eid {
        &self.id
    }

    fn id_mut(&mut self) -> &mut Eid {
        &mut self.id
    }
}

impl CloneNew for Store {}

impl<'de> IntoCow<'de> for Store {}

impl<'de> Persistable<'de> for Store {}

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
    ) -> Result<Self> {
        let st = store.read().unwrap();
        let ctn_wtr = ContentWriter::new(chk_map, store, txid, txmgr, &st.vol)?;
        Ok(Writer {
            inner: Chunker::new(st.chunker_params.clone(), ctn_wtr),
        })
    }

    pub fn finish(self) -> Result<Content> {
        let ctn_wtr = self.inner.into_inner()?;
        let content = ctn_wtr.finish()?;
        Ok(content)
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
