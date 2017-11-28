use std::error::Error as StdError;
use std::fmt::{self, Debug};
use std::io::{Read, Write, Error as IoError, ErrorKind, Result as IoResult,
              Seek, SeekFrom};
use std::cmp::min;

use error::Result;
use base::crypto::{Crypto, Hash};
use volume::{VolumeRef, Persistable};
use trans::{Eid, Id, CloneNew, TxMgrRef, Txid};
use trans::cow::{CowRef, IntoCow, CowCache};
use super::{Store, StoreRef};
use super::chunk::ChunkMap;
use super::span::Span;
use super::entry::EntryList;
use super::segment::Writer as SegWriter;

/// Content
#[derive(Default, Clone, Deserialize, Serialize)]
pub struct Content {
    id: Eid,
    hash: Hash,
    ents: EntryList,
}

impl Content {
    pub fn new() -> Self {
        Content {
            id: Eid::new(),
            hash: Hash::new(),
            ents: EntryList::new(),
        }
    }

    #[inline]
    pub fn hash(&self) -> &Hash {
        &self.hash
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.ents.len
    }

    #[inline]
    pub fn end_offset(&self) -> usize {
        self.ents.end_offset()
    }

    pub fn split_off(&mut self, at: usize) {
        assert!(at <= self.len());
        self.ents.split_off(at);
    }

    // append chunk to content
    fn append(&mut self, seg_id: &Eid, span: &Span) {
        self.ents.append(seg_id, span);
    }

    /// Write into self with another content
    pub fn write_with(&mut self, other: &Content, store: &Store) -> Result<()> {
        let (head, tail) = self.ents.write_with(&other.ents);
        head.link(store).and(tail.link(store))
    }

    /// Calculate content hash
    pub fn calculate_hash(
        content: &ContentRef,
        store: &StoreRef,
    ) -> Result<Hash> {
        let mut rdr = Reader::new(content, store);
        let mut buf = vec![0u8; 8 * 1024];

        let mut state = Crypto::hash_init();
        loop {
            let read = rdr.read(&mut buf[..])?;
            if read == 0 {
                break;
            }
            Crypto::hash_update(&mut state, &buf[..read]);
        }
        let hash = Crypto::hash_final(&mut state);

        let mut ctn = content.write().unwrap();
        ctn.make_mut()?.hash = hash.clone();

        Ok(hash)
    }

    pub fn link(ctn: ContentRef, store: StoreRef) -> Result<()> {
        let ctn = ctn.read().unwrap();
        let mut store = store.write().unwrap();
        ctn.ents.link(&mut store)
    }

    pub fn unlink(
        content: ContentRef,
        chk_map: &mut ChunkMap,
        store: StoreRef,
    ) -> Result<()> {
        let mut ctn = content.write().unwrap();
        let mut st = store.write().unwrap();
        ctn.make_mut()?.ents.unlink(chk_map, st.make_mut()?)
    }
}

impl Seek for Content {
    fn seek(&mut self, pos: SeekFrom) -> IoResult<u64> {
        self.ents.seek(pos)
    }
}

impl Debug for Content {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("Content")
            .field("id", &self.id)
            .field("hash", &self.hash)
            .field("ents", &self.ents)
            .finish()
    }
}

impl Id for Content {
    fn id(&self) -> &Eid {
        &self.id
    }

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

    pub fn reset(&mut self) {
        self.pos = 0;
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

        for ent in ctn.ents.ents.iter().skip_while(
            |e| e.end_offset() <= start,
        )
        {
            let segref = map_io_err!(store.get_seg(&ent.seg_id))?;
            let seg = segref.read().unwrap();
            let segdata_ref =
                map_io_err!(store.get_segdata(seg.seg_data_id()))?;
            let segdata = segdata_ref.read().unwrap();

            for span in ent.spans.iter().skip_while(
                |s| s.end_offset() <= start,
            )
            {
                let over_span = self.pos as usize - span.offset;
                let mut seg_offset = span.seg_offset + over_span;
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
        let seg = self.seg_wtr.seg();
        let seg = seg.read().unwrap();
        let begin = seg.chunk_cnt() - 1;
        let span = Span::new(
            begin,
            begin + 1,
            seg.len() - chunk_len,
            chunk_len,
            self.ctn.end_offset(),
        );
        self.ctn.append(seg.id(), &span);

        // and update chunk map
        self.chk_map.insert(hash, seg.id(), begin);

        Ok(())
    }

    pub fn finish(self) -> Result<Content> {
        // save current segment
        self.seg_wtr.save_seg()?;

        Ok(self.ctn)
    }
}

impl Write for Writer {
    fn write(&mut self, chunk: &[u8]) -> IoResult<usize> {
        let chunk_len = chunk.len();

        // calculate chunk hash
        let hash = Crypto::hash(chunk);

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

            // increase chunk reference
            let mut ref_seg = rseg.write().unwrap();
            if map_io_err!(ref_seg.make_mut())?.ref_chunk(loc.idx).is_ok() {
                let chunk = &ref_seg[loc.idx];
                let span = Span::new(
                    loc.idx,
                    loc.idx + 1,
                    chunk.pos,
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
        self.seg_wtr.flush()
    }
}

impl Seek for Writer {
    fn seek(&mut self, pos: SeekFrom) -> IoResult<u64> {
        self.ctn.seek(pos)
    }
}

pub type Cache = CowCache<Content>;
