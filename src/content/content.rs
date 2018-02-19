use std::error::Error as StdError;
use std::fmt::{self, Debug};
use std::io::{Error as IoError, ErrorKind, Read, Result as IoResult, Seek,
              SeekFrom, Write};
use std::cmp::min;

use error::Result;
use base::crypto::{Crypto, Hash};
use volume::{Persistable, VolumeRef};
use trans::{CloneNew, Eid, Id, TxMgrRef, Txid};
use trans::cow::{CowCache, CowRef, IntoCow};
use super::StoreRef;
use super::chunk::ChunkMap;
use super::span::{Extent, Span};
use super::entry::{CutableList, EntryList};
use super::segment::Writer as SegWriter;
use super::merkle_tree::{Leaves, MerkleTree, Writer as MerkleTreeWriter};

/// Content
#[derive(Default, Clone, Deserialize, Serialize)]
pub struct Content {
    id: Eid,
    mtree: MerkleTree,
    ents: EntryList,

    // merkle tree leaves
    #[serde(skip_serializing, skip_deserializing, default)] leaves: Leaves,
}

impl Content {
    pub fn new() -> Self {
        Content {
            id: Eid::new(),
            mtree: MerkleTree::new(),
            ents: EntryList::new(),
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
    pub fn replace(
        dst: &ContentRef,
        other: &Content,
        store: &StoreRef,
    ) -> Result<()> {
        let mut new_mtree;

        // write other content into destination content
        {
            let store = store.read().unwrap();
            let mut ctn_cow = dst.write().unwrap();
            let ctn = ctn_cow.make_mut()?;
            let (head, tail) = ctn.ents.write_with(&other.ents, &store)?;
            head.link(&store).and(tail.link(&store))?;
            new_mtree = ctn.mtree.clone();
        }

        // merge merkle tree
        let mut rdr = Reader::new(dst, store);
        new_mtree.merge(&other.leaves, &mut rdr)?;
        let mut ctn_cow = dst.write().unwrap();
        let ctn = ctn_cow.make_mut()?;
        ctn.mtree = new_mtree;

        Ok(())
    }

    pub fn truncate(
        ctn: &ContentRef,
        at: usize,
        store: &StoreRef,
    ) -> Result<()> {
        let mut new_mtree;

        // truncate content
        {
            let store = store.read().unwrap();
            let mut ctn_cow = ctn.write().unwrap();
            let ctn = ctn_cow.make_mut()?;
            assert!(at <= ctn.len());
            let pos = ctn.ents.locate(at);
            let seg_ref = store.get_seg(ctn.ents[pos].seg_id())?;
            let seg = seg_ref.read().unwrap();
            ctn.ents.split_off(at, &seg);
            new_mtree = ctn.mtree.clone();
        }

        // truncate merkle tree
        let mut rdr = Reader::new(ctn, store);
        new_mtree.truncate(at, &mut rdr)?;
        let mut ctn_cow = ctn.write().unwrap();
        let ctn = ctn_cow.make_mut()?;
        ctn.mtree = new_mtree;

        Ok(())
    }

    pub fn link(ctn: ContentRef, store: &StoreRef) -> Result<()> {
        let ctn = ctn.read().unwrap();
        let mut store = store.write().unwrap();
        ctn.ents.link(&mut store)
    }

    pub fn unlink(
        content: &ContentRef,
        chk_map: &mut ChunkMap,
        store: &StoreRef,
    ) -> Result<()> {
        let mut ctn = content.write().unwrap();
        let mut st = store.write().unwrap();

        // unlink entries
        ctn.ents.unlink(chk_map, st.make_mut()?)?;

        // content is not used anymore, remove content
        ctn.make_del()
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
            .field("id", &self.id)
            .field("hash", self.hash())
            .field("ents", &self.ents)
            .finish()
    }
}

impl Id for Content {
    #[inline]
    fn id(&self) -> &Eid {
        &self.id
    }

    #[inline]
    fn id_mut(&mut self) -> &mut Eid {
        &mut self.id
    }
}

impl CloneNew for Content {}

impl<'de> IntoCow<'de> for Content {}

impl<'de> Persistable<'de> for Content {}

/// Content reference type
pub type ContentRef = CowRef<Content>;

/// Content Reader
#[derive(Debug)]
pub struct Reader {
    pos: u64,
    content: ContentRef,
    store: StoreRef,
}

impl Reader {
    pub fn new(content: &ContentRef, store: &StoreRef) -> Self {
        Reader {
            pos: 0,
            content: content.clone(),
            store: store.clone(),
        }
    }
}

impl Read for Reader {
    fn read(&mut self, buf: &mut [u8]) -> IoResult<usize> {
        if buf.len() == 0 {
            return Ok(0);
        }

        let ctn = self.content.read().unwrap();
        let store = self.store.read().unwrap();
        let start = self.pos as usize;
        let mut buf_read = 0;

        for ent in ctn.ents.iter().skip_while(|e| e.end_offset() <= start) {
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
                let ctn = self.content.read().unwrap();
                self.pos = (ctn.len() as i64 + pos) as u64;
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
    ctn: Content,
    chk_map: ChunkMap,
    seg_wtr: SegWriter,
    mtree_wtr: MerkleTreeWriter,
    txid: Txid,
    store: StoreRef,
}

impl Writer {
    pub fn new(
        chk_map: ChunkMap,
        store: &StoreRef,
        txid: Txid,
        txmgr: &TxMgrRef,
        vol: &VolumeRef,
    ) -> Result<Self> {
        Ok(Writer {
            ctn: Content::new(),
            chk_map,
            seg_wtr: SegWriter::new(txid, txmgr, vol)?,
            mtree_wtr: MerkleTreeWriter::new(),
            txid,
            store: store.clone(),
        })
    }

    // append chunk to segment and content
    // return appended chunk index in segment
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

    pub fn finish(mut self) -> Result<Content> {
        // save current segment
        self.seg_wtr.save_seg()?;

        // finish merkel tree
        self.ctn.leaves = self.mtree_wtr.finish();

        Ok(self.ctn)
    }
}

impl Write for Writer {
    fn write(&mut self, chunk: &[u8]) -> IoResult<usize> {
        let chunk_len = chunk.len();

        // calculate chunk hash
        let hash = Crypto::hash(chunk);

        // update merkel tree
        self.mtree_wtr.write(chunk)?;

        // if duplicate chunk is found
        if let Some(ref loc) = self.chk_map.get(&hash) {
            // try to increase chunk reference count
            let store = self.store.read().unwrap();
            // get referred segment, it could be the current segment
            let rseg = {
                let curr_seg = self.seg_wtr.seg();
                let seg = curr_seg.read().unwrap();
                if loc.seg_id == *seg.id() {
                    curr_seg.clone()
                } else {
                    map_io_err!(store.get_seg(&loc.seg_id))?
                }
            };

            // increase chunk reference count and append the reference
            let mut ref_seg = rseg.write().unwrap();
            if map_io_err!(ref_seg.make_mut())?.ref_chunk(loc.idx).is_ok() {
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
                return Ok(chunk_len);
            }
        }

        // no duplication found, then append chunk to content
        self.append_chunk(chunk, &hash)?;

        Ok(chunk_len)
    }

    fn flush(&mut self) -> IoResult<()> {
        self.seg_wtr.flush().and(self.mtree_wtr.flush())
    }
}

impl Seek for Writer {
    fn seek(&mut self, pos: SeekFrom) -> IoResult<u64> {
        self.ctn.seek(pos).and(self.mtree_wtr.seek(pos))
    }
}

pub type Cache = CowCache<Content>;
