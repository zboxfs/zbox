use std::io::{Result as IoResult, Seek, SeekFrom};
use std::ops::Index;
use std::slice::Iter;

use super::chunk::ChunkMap;
use super::segment::Segment;
use super::span::{Cutable, Extent, Span};
use super::Store;
use error::Result;
use trans::{Eid, Id, TxMgrRef};

pub(super) trait CutableList: Clone + Extent {
    type Item: Extent + Cutable;

    fn items(&self) -> &Vec<Self::Item>;
    fn items_mut(&mut self) -> &mut Vec<Self::Item>;
    fn set_items(&mut self, items: Vec<Self::Item>);

    fn locate(&self, at: usize) -> usize {
        assert!(self.offset() <= at && at <= self.end_offset());
        self.items()
            .iter()
            .position(|i| i.offset() <= at && at <= i.end_offset())
            .unwrap_or(0)
    }

    fn split_off(&mut self, at: usize, seg: &Segment) -> Self {
        assert!(self.offset() <= at && at <= self.end_offset());
        let mut pos = self.locate(at);
        let split = {
            let item = &mut self.items_mut()[pos];
            item.cut_off(at, seg)
        };

        if self.items()[pos].is_empty() {
            self.items_mut().remove(pos);
        } else {
            pos += 1;
        }

        let mut ret = self.clone();
        ret.set_offset(split.offset());
        let mut tail = self.items_mut().split_off(pos);
        if !split.is_empty() {
            tail.insert(0, split);
        }
        ret.set_len(tail.iter().fold(0, |sum, ref i| sum + i.len()));
        ret.set_items(tail);

        let new_len = at - self.offset();
        self.set_len(new_len);
        if self.is_empty() {
            self.items_mut().clear();
        }

        ret
    }

    fn split_to(&mut self, at: usize, seg: &Segment) -> Self {
        assert!(self.offset() <= at && at <= self.end_offset());
        let pos = self.locate(at);
        let split = {
            let item = &mut self.items_mut()[pos];
            item.cut_to(at, seg)
        };

        if self.items()[pos].is_empty() {
            self.items_mut().remove(pos);
        }
        let tail = self.items_mut().split_off(pos);
        if !split.is_empty() {
            self.items_mut().push(split);
        }

        let mut ret = self.clone();
        ret.set_items(self.items().to_vec());
        let head_len = ret.items().iter().fold(0, |sum, ref i| sum + i.len());
        ret.set_len(head_len);

        self.set_items(tail);
        let new_len = self.end_offset() - at;
        self.set_len(new_len);
        self.set_offset(at);
        if self.is_empty() {
            self.items_mut().clear();
        }

        ret
    }
}

/// An entry in content entry list, one entry per segment
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct Entry {
    seg_id: Eid,
    len: usize,
    offset: usize, // offset in content
    spans: Vec<Span>,
}

impl Entry {
    fn new(seg_id: &Eid, offset: usize) -> Self {
        Entry {
            seg_id: seg_id.clone(),
            len: 0,
            offset,
            spans: Vec::new(),
        }
    }

    #[inline]
    pub fn seg_id(&self) -> &Eid {
        &self.seg_id
    }

    #[inline]
    pub fn iter(&self) -> Iter<Span> {
        self.spans.iter()
    }

    pub fn append(&mut self, span: &Span) {
        // try to merge with the last span
        if let Some(last) = self.spans.last_mut() {
            if last.end == span.begin && span.seg_offset == 0 {
                last.merge_up(span);
                self.len += span.len;
                return;
            }
        }

        self.spans.push(span.clone());
        self.len += span.len;
    }
}

impl Extent for Entry {
    #[inline]
    fn offset(&self) -> usize {
        self.offset
    }

    #[inline]
    fn set_offset(&mut self, offset: usize) {
        self.offset = offset;
    }

    #[inline]
    fn len(&self) -> usize {
        self.len
    }

    #[inline]
    fn set_len(&mut self, len: usize) {
        self.len = len;
    }
}

impl Cutable for Entry {
    #[inline]
    fn cut_off(&mut self, at: usize, seg: &Segment) -> Self {
        self.split_off(at, seg)
    }

    #[inline]
    fn cut_to(&mut self, at: usize, seg: &Segment) -> Self {
        self.split_to(at, seg)
    }
}

impl CutableList for Entry {
    type Item = Span;

    #[inline]
    fn items(&self) -> &Vec<Self::Item> {
        &self.spans
    }

    #[inline]
    fn items_mut(&mut self) -> &mut Vec<Self::Item> {
        &mut self.spans
    }

    #[inline]
    fn set_items(&mut self, items: Vec<Self::Item>) {
        self.spans = items;
    }
}

impl Seek for Entry {
    fn seek(&mut self, pos: SeekFrom) -> IoResult<u64> {
        let delta;

        match pos {
            SeekFrom::Start(pos) => {
                delta = pos as i64 - self.offset as i64;
                self.offset = pos as usize;
            }
            SeekFrom::End(pos) => {
                let old_offset = self.offset;
                self.offset = (self.end_offset() as i64 + pos) as usize;
                delta = old_offset as i64 - self.offset as i64;
            }
            SeekFrom::Current(pos) => {
                self.offset = (self.offset as i64 + pos) as usize;
                delta = pos;
            }
        }

        for span in self.spans.iter_mut() {
            span.seek(SeekFrom::Current(delta))?;
        }

        Ok(self.offset as u64)
    }
}

/// Entry list
#[derive(Debug, Default, Clone, Deserialize, Serialize)]
pub struct EntryList {
    len: usize,
    offset: usize,
    ents: Vec<Entry>,
}

impl EntryList {
    pub fn new() -> Self {
        EntryList::default()
    }

    #[inline]
    pub fn iter(&self) -> Iter<Entry> {
        self.ents.iter()
    }

    // append span
    pub fn append(&mut self, seg_id: &Eid, span: &Span) {
        // try to merge with the last entry
        if let Some(last_ent) = self.ents.last_mut() {
            if *seg_id == last_ent.seg_id {
                last_ent.append(span);
                self.len += span.len;
                return;
            }
        }

        let mut ent = Entry::new(seg_id, self.end_offset());
        ent.append(span);
        self.ents.push(ent);
        self.len += span.len;
    }

    fn join(&mut self, other: &EntryList) {
        assert_eq!(self.end_offset(), other.offset);
        self.len += other.len;
        self.ents.extend(other.ents.clone());
    }

    // Write another entry list to self
    // return the excluded head and tail entry list by the other
    pub fn write_with(
        &mut self,
        other: &EntryList,
        store: &Store,
    ) -> Result<(EntryList, EntryList)> {
        let at = other.offset;
        let end_at = other.end_offset();

        assert!(self.offset <= at && at <= self.end_offset());

        let mut tail = if end_at < self.end_offset() {
            self.clone()
        } else {
            EntryList::new()
        };

        if at < self.end_offset() {
            let pos = self.locate(at);
            let seg_ref = store.get_seg(&self[pos].seg_id)?;
            let seg = seg_ref.read().unwrap();
            self.split_off(at, &seg);
        }

        let head = self.clone();
        self.join(other);

        if end_at < tail.end_offset() {
            let pos = tail.locate(end_at);
            let seg_ref = store.get_seg(&tail[pos].seg_id)?;
            let seg = seg_ref.read().unwrap();
            tail.split_to(end_at, &seg);
            self.join(&tail);
        }

        Ok((head, tail))
    }

    // create reference relationship between content and segment
    pub fn link(&self, store: &Store, txmgr: &TxMgrRef) -> Result<()> {
        for ent in self.ents.iter() {
            let seg_ref = store.get_seg(&ent.seg_id)?;
            let mut seg_cow = seg_ref.write().unwrap();
            let seg = seg_cow.make_mut(txmgr)?;
            for span in ent.spans.iter() {
                seg.ref_chunks(span.begin..span.end)?;
            }
        }
        Ok(())
    }

    // remove reference between content and segment, this is reversal
    // for established references using link()
    pub fn unlink(
        &self,
        chk_map: &mut ChunkMap,
        store: &Store,
        txmgr: &TxMgrRef,
    ) -> Result<()> {
        for ent in self.ents.iter() {
            let seg_ref = store.get_seg(&ent.seg_id)?;
            let mut seg_cow = seg_ref.write().unwrap();

            {
                let seg = seg_cow.make_mut(txmgr)?;
                for span in ent.spans.iter() {
                    seg.deref_chunks(span.begin..span.end)?;
                }
            }

            if seg_cow.is_orphan() {
                // if segment is not used anymore, remove it
                Segment::remove(&mut seg_cow, txmgr)?;
                chk_map.remove_segment(seg_cow.id());
            } else if seg_cow.is_shrinkable() {
                // shrink segment if it is small enough and remove retired
                // chunks from chunk map
                let retired = Segment::shrink(&mut seg_cow, store, txmgr)?;
                chk_map.remove_chunks(seg_cow.id(), &retired);
            }
        }

        Ok(())
    }

    // remove weak reference between content and segment, the weak reference is
    // the relationship hasn't been established by link(), used for stage
    // segment dereference
    pub fn unlink_weak(
        &self,
        chk_map: &mut ChunkMap,
        store: &mut Store,
        txmgr: &TxMgrRef,
    ) -> Result<()> {
        for ent in self.ents.iter() {
            let seg_ref = store.get_seg(&ent.seg_id)?;
            let mut seg_cow = seg_ref.write().unwrap();

            if seg_cow.is_orphan() {
                // if segment is not used anymore, remove it
                Segment::remove(&mut seg_cow, txmgr)?;
                chk_map.remove_segment(seg_cow.id());
            }
        }

        Ok(())
    }

    #[allow(dead_code)]
    pub fn check(&self) {
        let mut ents_len = 0;
        let mut offset = self.offset;

        for ent in self.ents.iter() {
            assert!(!ent.is_empty());
            ents_len += ent.len;
            assert_eq!(ent.offset, offset);
            offset += ent.len;

            let mut spans_len = 0;
            let mut span_offset = ent.offset;
            for span in ent.spans.iter() {
                assert!(!span.is_empty());
                spans_len += span.len;
                assert_eq!(span.offset, span_offset);
                span_offset += span.len;
            }

            assert_eq!(spans_len, ent.len);
        }

        assert_eq!(ents_len, self.len);
    }
}

impl Extent for EntryList {
    #[inline]
    fn offset(&self) -> usize {
        self.offset
    }

    #[inline]
    fn set_offset(&mut self, offset: usize) {
        self.offset = offset;
    }

    #[inline]
    fn len(&self) -> usize {
        self.len
    }

    #[inline]
    fn set_len(&mut self, len: usize) {
        self.len = len;
    }
}

impl CutableList for EntryList {
    type Item = Entry;

    #[inline]
    fn items(&self) -> &Vec<Self::Item> {
        &self.ents
    }

    #[inline]
    fn items_mut(&mut self) -> &mut Vec<Self::Item> {
        &mut self.ents
    }

    #[inline]
    fn set_items(&mut self, items: Vec<Self::Item>) {
        self.ents = items;
    }
}

impl Index<usize> for EntryList {
    type Output = Entry;

    #[inline]
    fn index(&self, index: usize) -> &Entry {
        &self.ents[index]
    }
}

impl Seek for EntryList {
    fn seek(&mut self, pos: SeekFrom) -> IoResult<u64> {
        let delta;

        match pos {
            SeekFrom::Start(pos) => {
                delta = pos as i64 - self.offset as i64;
                self.offset = pos as usize;
            }
            SeekFrom::End(pos) => {
                let old_offset = self.offset;
                self.offset = (self.end_offset() as i64 + pos) as usize;
                delta = old_offset as i64 - self.offset as i64;
            }
            SeekFrom::Current(pos) => {
                self.offset = (self.offset as i64 + pos) as usize;
                delta = pos;
            }
        }

        for ent in self.ents.iter_mut() {
            ent.seek(SeekFrom::Current(delta))?;
        }

        Ok(self.offset as u64)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use base::init_env;

    #[test]
    fn entry_list_append() {
        init_env();

        let mut elst = EntryList::new();
        let id = Eid::new();
        let id2 = Eid::new();

        // append initial span
        let span = Span::new(0, 1, 0, 10, 0);
        elst.append(&id, &span);
        assert_eq!(elst.len, 10);
        assert_eq!(elst.ents.len(), 1);

        // append continuous span should merge
        let span = Span::new(1, 2, 0, 20, 10);
        elst.append(&id, &span);
        assert_eq!(elst.len, 30);
        assert_eq!(elst.ents.len(), 1);

        // append discontinuous span should not merge
        let span = Span::new(4, 5, 0, 30, 30);
        elst.append(&id, &span);
        assert_eq!(elst.len, 60);
        assert_eq!(elst.ents.len(), 1);
        assert_eq!(elst.ents.first().unwrap().spans.len(), 2);

        // append span in another segment
        let span = Span::new(0, 1, 0, 10, 60);
        elst.append(&id2, &span);
        assert_eq!(elst.len, 70);
        assert_eq!(elst.ents.len(), 2);
        assert_eq!(elst.ents.last().unwrap().spans.len(), 1);

        // append continuous span should merge
        let span = Span::new(1, 2, 0, 20, 70);
        elst.append(&id2, &span);
        assert_eq!(elst.len, 90);
        assert_eq!(elst.ents.len(), 2);
        assert_eq!(elst.ents.last().unwrap().spans.len(), 1);

        elst.check();
    }
}
