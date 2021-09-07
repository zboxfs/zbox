use std::cmp::min;
use std::fmt::{self, Debug};
use std::io::{
    Error as IoError, ErrorKind, Read, Result as IoResult, Seek, SeekFrom,
    Write,
};
use std::sync::Arc;

use super::chunk::ChunkMap;
use super::entry::{CutableList, EntryList};
use super::merkle_tree::{Leaves, MerkleTree, Writer as MerkleTreeWriter};
use super::segment::Writer as SegWriter;
use super::span::{Extent, Span};
use super::{StoreRef, StoreWeakRef};
use base::crypto::{Crypto, Hash};
use error::{Error, Result};
use trans::cow::{CowCache, CowRef, Cowable, IntoCow};
use trans::{Eid, Finish, Id, TxMgrRef, TxMgrWeakRef, Txid};
use volume::VolumeWeakRef;

/// Content
#[derive(Default, Clone, Deserialize, Serialize)]
pub struct Content {
    ents: EntryList,
    mtree: MerkleTree,

    // merkle tree leaves
    #[serde(skip_serializing, skip_deserializing, default)]
    leaves: Leaves,
}

impl Content {
    pub fn new() -> Self {
        Content {
            ents: EntryList::new(),
            mtree: MerkleTree::new(),
            leaves: Leaves::new(),
        }
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.ents.len()
    }

    #[inline]
    pub fn end_offset(&self) -> usize {
        self.ents.end_offset()
    }

    #[inline]
    pub fn hash(&self) -> &Hash {
        self.mtree.root_hash()
    }

    // append chunk to content
    #[inline]
    fn append(&mut self, seg_id: &Eid, span: &Span) {
        self.ents.append(seg_id, span);
    }

    /// Write into content with another content
    pub fn merge_from(
        &mut self,
        other: &Content,
        store: &StoreRef,
    ) -> Result<()> {
        // write other content into self
        {
            let store = store.read().unwrap();
            let (_head, _tail) = self.ents.write_with(&other.ents, &store)?;
        }

        // merge merkle tree
        let mut rdr = Reader::new(self.clone(), &Arc::downgrade(store));
        self.mtree.merge(&other.leaves, &mut rdr)?;

        Ok(())
    }

    pub fn truncate(&mut self, at: usize, store: &StoreRef) -> Result<()> {
        // truncate content
        {
            let store = store.read().unwrap();
            assert!(at <= self.len());
            let pos = self.ents.locate(at);
            let seg_ref = store.get_seg(self.ents[pos].seg_id())?;
            let seg = seg_ref.read().unwrap();
            self.ents.split_off(at, &seg);
        }

        // truncate merkle tree
        let mut rdr = Reader::new(self.clone(), &Arc::downgrade(store));
        self.mtree.truncate(at, &mut rdr)?;

        Ok(())
    }

    // build reference between content and segment
    #[inline]
    pub fn link(&self, store: &StoreRef, txmgr: &TxMgrRef) -> Result<()> {
        let store = store.read().unwrap();
        self.ents.link(&store, txmgr)
    }

    // remove reference between content and segment
    #[inline]
    pub fn unlink(
        &self,
        chk_map: &mut ChunkMap,
        store: &StoreRef,
        txmgr: &TxMgrRef,
    ) -> Result<()> {
        let mut store = store.write().unwrap();
        self.ents.unlink(chk_map, store.make_mut_naive(), txmgr)
    }

    // remove weak reference between content and segment
    #[inline]
    pub fn unlink_weak(
        &self,
        chk_map: &mut ChunkMap,
        store: &StoreRef,
        txmgr: &TxMgrRef,
    ) -> Result<()> {
        let mut store = store.write().unwrap();
        self.ents
            .unlink_weak(chk_map, store.make_mut_naive(), txmgr)
    }
}

impl Seek for Content {
    #[inline]
    fn seek(&mut self, pos: SeekFrom) -> IoResult<u64> {
        self.ents.seek(pos)
    }
}

impl Debug for Content {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("Content")
            .field("hash", self.hash())
            .field("ents", &self.ents)
            .finish()
    }
}

impl Cowable for Content {}

impl<'de> IntoCow<'de> for Content {}

/// Content reference type
pub type ContentRef = CowRef<Content>;

/// Content Reader
#[derive(Debug)]
pub struct Reader {
    pos: u64,
    content: Content,
    store: StoreWeakRef,
}

impl Reader {
    pub fn new(content: Content, store: &StoreWeakRef) -> Self {
        Reader {
            pos: 0,
            content,
            store: store.clone(),
        }
    }
}

impl Read for Reader {
    fn read(&mut self, buf: &mut [u8]) -> IoResult<usize> {
        if buf.is_empty() {
            return Ok(0);
        }

        let store = map_io_err!(self.store.upgrade().ok_or(Error::RepoClosed))?;
        let store = store.read().unwrap();
        let start = self.pos as usize;
        let mut buf_read = 0;

        for ent in self
            .content
            .ents
            .iter()
            .skip_while(|e| e.end_offset() <= start)
        {
            let seg_ref = map_io_err!(store.get_seg(ent.seg_id()))?;
            let seg = seg_ref.read().unwrap();
            let segdata_ref = map_io_err!(store.get_segdata(seg.data_id()))?;
            let segdata = segdata_ref.read().unwrap();

            for span in ent.iter().skip_while(|s| s.end_offset() <= start) {
                let over_span = self.pos as usize - span.offset;
                let mut seg_offset = span.offset_in_seg(&seg) + over_span;
                let mut span_left = span.len - over_span;

                while span_left > 0 {
                    let dst = &mut buf[buf_read..];

                    // if destination buffer is full, stop reading
                    if dst.is_empty() {
                        return Ok(buf_read);
                    }

                    let read_len = min(span_left, dst.len());
                    let read = segdata.read(&mut dst[..read_len], seg_offset);
                    buf_read += read;
                    seg_offset += read;
                    span_left -= read;
                    self.pos += read as u64;
                }
            }
        }

        Ok(buf_read)
    }
}

impl Seek for Reader {
    fn seek(&mut self, pos: SeekFrom) -> IoResult<u64> {
        match pos {
            SeekFrom::Start(pos) => {
                self.pos = pos;
            }
            SeekFrom::End(pos) => {
                self.pos = (self.content.len() as i64 + pos) as u64;
            }
            SeekFrom::Current(pos) => {
                self.pos = (self.pos as i64 + pos) as u64;
            }
        }
        Ok(self.pos)
    }
}

/// Content Writer
#[derive(Debug)]
pub struct Writer {
    txid: Txid,
    ctn: Content,
    chk_map: ChunkMap,
    seg_wtr: SegWriter,
    mtree_wtr: MerkleTreeWriter,
    store: StoreWeakRef,
}

impl Writer {
    pub fn new(
        txid: Txid,
        chk_map: ChunkMap,
        store: &StoreWeakRef,
        txmgr: &TxMgrWeakRef,
        vol: &VolumeWeakRef,
    ) -> Self {
        Writer {
            txid,
            ctn: Content::new(),
            chk_map,
            seg_wtr: SegWriter::new(txid, store, txmgr, vol),
            mtree_wtr: MerkleTreeWriter::new(),
            store: store.clone(),
        }
    }

    // append chunk to segment and content
    fn append_chunk(&mut self, chunk: &[u8], hash: &Hash) -> IoResult<()> {
        let chunk_len = chunk.len();

        // write to segment, if segment is full then
        // create a new one and try it again
        let mut written = self.seg_wtr.write(chunk)?;
        if written == 0 {
            // segment is full
            map_io_err!(self.seg_wtr.renew())?;
            written = self.seg_wtr.write(chunk)?;
        }
        assert_eq!(written, chunk_len); // must written in whole

        // append chunk to content
        let seg_ref = self.seg_wtr.seg();
        let seg = seg_ref.read().unwrap();
        let begin = seg.chunk_cnt() - 1;
        let span =
            Span::new(begin, begin + 1, 0, chunk_len, self.ctn.end_offset());
        self.ctn.append(seg.id(), &span);

        // and update chunk map
        self.chk_map.insert(hash, seg.id(), begin);

        Ok(())
    }

    // finish writer, return stage content and updated chunk map
    pub fn finish(mut self) -> Result<(Content, ChunkMap)> {
        // finish segment writer
        self.seg_wtr.finish()?;

        // finish merkel tree
        self.ctn.leaves = self.mtree_wtr.finish_with_leaves();

        Ok((self.ctn, self.chk_map))
    }
}

impl Write for Writer {
    fn write(&mut self, chunk: &[u8]) -> IoResult<usize> {
        let chunk_len = chunk.len();

        // calculate chunk hash
        let hash = Crypto::hash(chunk);

        // update merkel tree
        let _ = self.mtree_wtr.write(chunk)?;

        // if duplicate chunk is found,
        if let Some(ref loc) = self.chk_map.get_refresh(&hash) {
            // get referred segment, it could be the current segment
            let store =
                map_io_err!(self.store.upgrade().ok_or(Error::RepoClosed))?;
            let store = store.read().unwrap();
            let rseg = {
                let curr_seg = self.seg_wtr.seg();
                let seg = curr_seg.read().unwrap();
                if loc.seg_id == *seg.id() {
                    curr_seg.clone()
                } else {
                    map_io_err!(store.get_seg(&loc.seg_id))?
                }
            };

            // create weak reference to chunk and append it to content,
            // strong reference will be built later when the stage content
            // is finished and deduped
            let ref_seg = rseg.write().unwrap();
            let chunk = &ref_seg[loc.idx];
            let span = Span::new(
                loc.idx,
                loc.idx + 1,
                0,
                chunk.len,
                self.ctn.end_offset(),
            );
            self.ctn.append(&loc.seg_id, &span);
            assert_eq!(chunk_len, chunk.len);
        } else {
            // no duplication found, then append chunk to content
            self.append_chunk(chunk, &hash)?;
        }

        Ok(chunk_len)
    }

    fn flush(&mut self) -> IoResult<()> {
        self.seg_wtr.flush()?;
        self.mtree_wtr.flush()
    }
}

impl Seek for Writer {
    fn seek(&mut self, pos: SeekFrom) -> IoResult<u64> {
        self.ctn.seek(pos)?;
        self.mtree_wtr.seek(pos)
    }
}

pub type Cache = CowCache<Content>;
