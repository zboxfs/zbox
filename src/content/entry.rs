use std::io::{Result as IoResult, Seek, SeekFrom};

use error::Result;
use trans::{Eid, Id};
use super::Store;
use super::span::Span;
use super::chunk::ChunkMap;

/// An entry in content entry list, one entry per segment
#[derive(Debug, Default, Clone, Deserialize, Serialize)]
pub struct Entry {
    pub(super) seg_id: Eid,
    pub(super) len: usize,
    pub(super) offset: usize, // offset in content
    pub(super) spans: Vec<Span>,
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
    fn is_empty(&self) -> bool {
        self.len == 0
    }

    #[inline]
    pub fn end_offset(&self) -> usize {
        self.offset + self.len
    }

    pub fn append(&mut self, span: &Span) {
        // try to merge with the last span
        if let Some(last) = self.spans.last_mut() {
            if last.end_seg_offset() == span.seg_offset {
                last.merge_up(span);
                self.len += span.len;
                return;
            }
        }

        self.spans.push(span.clone());
        self.len += span.len;
    }

    fn split_off(&mut self, at: usize) -> Entry {
        assert!(self.offset <= at && at < self.end_offset());
        let split_len = at - self.offset;
        let mut pos = self.spans
            .iter()
            .position(|s| s.offset <= at && at < s.end_offset())
            .unwrap();
        let split = self.spans[pos].split_off(at);
        if self.spans[pos].is_empty() {
            self.spans.remove(pos);
        } else {
            pos += 1;
        }
        self.spans.insert(pos, split);
        let spans = self.spans.split_off(pos);
        let ent = Entry {
            seg_id: self.seg_id.clone(),
            len: self.len - split_len,
            offset: self.offset + split_len,
            spans,
        };
        self.len = split_len;
        if self.is_empty() {
            self.spans.clear();
        }

        ent
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
    pub(super) len: usize,
    pub(super) offset: usize,
    pub(super) ents: Vec<Entry>,
}

impl EntryList {
    pub fn new() -> Self {
        EntryList::default()
    }

    #[inline]
    fn is_empty(&self) -> bool {
        self.len == 0
    }

    #[inline]
    pub fn end_offset(&self) -> usize {
        self.offset + self.len
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

    pub fn split_off(&mut self, at: usize) -> EntryList {
        assert!(self.offset <= at && at < self.end_offset());
        let split_len = at - self.offset;
        let mut pos = self.ents
            .iter()
            .position(|e| e.offset <= at && at < e.end_offset())
            .unwrap();
        let split = self.ents[pos].split_off(at);
        if self.ents[pos].is_empty() {
            self.ents.remove(pos);
        } else {
            pos += 1;
        }
        self.ents.insert(pos, split);
        let ents = self.ents.split_off(pos);
        let ret = EntryList {
            len: self.len - split_len,
            offset: self.offset + split_len,
            ents,
        };
        self.len = split_len;
        if self.is_empty() {
            self.ents.clear();
        }
        ret
    }

    fn join(&mut self, other: &EntryList) {
        assert_eq!(self.end_offset(), other.offset);
        self.len += other.len;
        self.ents.extend(other.ents.clone());
    }

    // Write another entry list to self
    // return the excluded head and tail entry list by the other
    pub fn write_with(&mut self, other: &EntryList) -> (EntryList, EntryList) {
        // if the write position is at the end of this entry list
        if self.end_offset() == other.offset {
            let head = self.clone();
            self.join(other);
            return (head, EntryList::new());
        }

        // cannot write beyond EOF
        assert!(
            self.offset <= other.offset && other.offset < self.end_offset()
        );

        let mut split = self.split_off(other.offset);
        let head = self.clone();
        let mut tail = EntryList::new();
        self.join(other);

        let end_offset = self.end_offset();
        if end_offset < split.end_offset() {
            tail = split.split_off(end_offset);
            self.join(&tail);
        }

        (head, tail)
    }

    pub fn link(&self, store: &Store) -> Result<()> {
        for ent in self.ents.iter() {
            let seg = store.get_seg(&ent.seg_id)?;
            let mut sg = seg.write().unwrap();
            //println!("entry.link: {:#?}", seg.id());
            for span in ent.spans.iter() {
                sg.make_mut()?.ref_chunks(span.begin..span.end)?;
            }
        }
        Ok(())
    }

    pub fn unlink(
        &mut self,
        chk_map: &mut ChunkMap,
        store: &mut Store,
    ) -> Result<()> {
        for ent in self.ents.iter() {
            let seg = store.get_seg(&ent.seg_id)?;
            let is_orphaned = {
                let mut sg = seg.write().unwrap();
                for span in ent.spans.iter() {
                    sg.make_mut()?.deref_chunks(span.begin..span.end)?;
                }
                sg.is_orphan()
            };

            // if segment is not used anymore, remove it
            if is_orphaned {
                let mut sg = seg.write().unwrap();
                sg.make_del()?;
                chk_map.remove(sg.id());
                //println!("entry.unlink: remove seg {:#?}", seg.id());
                store.remove_segment(sg.id(), sg.seg_data_id());
            }
        }

        Ok(())
    }

    #[allow(dead_code)]
    #[cfg(debug_assertions)]
    fn check(&self) {
        let mut ents_len = 0;
        let mut offset = self.offset;

        for ent in self.ents.iter() {
            assert!(!ent.is_empty());
            ents_len += ent.len;
            assert_eq!(ent.offset, offset);
            offset += ent.len;

            let mut spans_len = 0;
            let mut idx = 0;
            let mut span_offset = ent.offset;
            let mut seg_offset = 0;
            for span in ent.spans.iter() {
                assert!(!span.is_empty());
                spans_len += span.len;
                assert_eq!(span.offset, span_offset);
                span_offset += span.len;
                if span.begin > 0 {
                    assert!(span.begin > idx);
                }
                if span.seg_offset > 0 {
                    assert!(span.seg_offset > seg_offset);
                }
                seg_offset = span.seg_offset;
                idx = span.end;
            }

            assert_eq!(spans_len, ent.len);
        }

        assert_eq!(ents_len, self.len);
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
#[cfg(debug_assertions)]
mod tests {
    use super::*;

    #[test]
    fn entry_list_append() {
        let mut elst = EntryList::new();
        let id = Eid::new();
        let id2 = Eid::new();

        // append initial span
        let span = Span::new(0, 1, 0, 10, 0);
        elst.append(&id, &span);
        assert_eq!(elst.len, 10);
        assert_eq!(elst.ents.len(), 1);

        // append continuous span should merge
        let span = Span::new(1, 2, 10, 20, 10);
        elst.append(&id, &span);
        assert_eq!(elst.len, 30);
        assert_eq!(elst.ents.len(), 1);

        // append discontinuous span should not merge
        let span = Span::new(4, 5, 20, 30, 30);
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
        let span = Span::new(1, 2, 10, 20, 70);
        elst.append(&id2, &span);
        assert_eq!(elst.len, 90);
        assert_eq!(elst.ents.len(), 2);
        assert_eq!(elst.ents.last().unwrap().spans.len(), 1);

        elst.check();
    }

    fn mock_entry_list(offset: usize, len: usize) -> EntryList {
        let mut ret = EntryList::new();
        ret.offset = offset;
        ret.append(&Eid::new_empty(), &Span::new(0, 1, 0, len, offset));
        ret
    }

    fn check_result(
        dst: &EntryList,
        head: &EntryList,
        tail: &EntryList,
        other: &EntryList,
    ) {
        dst.check();
        head.check();
        tail.check();
        assert_eq!(dst.len, head.len + other.len + tail.len);
        assert_eq!(head.end_offset(), other.offset);
        if !tail.is_empty() {
            assert_eq!(tail.offset, other.end_offset());
        }
    }

    fn test_write_with(elst: &EntryList) {
        let oth_len = elst.len / 3;

        // write at the beginning
        let other = mock_entry_list(0, oth_len);
        let mut dst = elst.clone();
        let (head, tail) = dst.write_with(&other);
        check_result(&dst, &head, &tail, &other);

        // write inside
        let other = mock_entry_list(elst.len / 2, oth_len);
        let mut dst = elst.clone();
        let (head, tail) = dst.write_with(&other);
        check_result(&dst, &head, &tail, &other);

        // write in the middle, and over the end
        let other = mock_entry_list(elst.len * 4 / 5, oth_len);
        let mut dst = elst.clone();
        let (head, tail) = dst.write_with(&other);
        check_result(&dst, &head, &tail, &other);

        // write at the end
        let other = mock_entry_list(elst.len, oth_len);
        let mut dst = elst.clone();
        let (head, tail) = dst.write_with(&other);
        check_result(&dst, &head, &tail, &other);
    }

    #[test]
    fn entry_list_write_with() {
        let id = Eid::new();
        let id2 = Eid::new();

        // #1, single span
        let mut elst = EntryList::new();
        elst.append(&id, &Span::new(0, 1, 0, 10, 0));
        test_write_with(&mut elst);

        // #2, multiple spans
        let mut elst = EntryList::new();
        elst.append(&id, &Span::new(0, 1, 0, 5, 0));
        elst.append(&id, &Span::new(2, 3, 10, 5, 5));
        test_write_with(&mut elst);

        // #3, multiple segments and spans
        let mut elst = EntryList::new();
        elst.append(&id, &Span::new(0, 1, 0, 5, 0));
        elst.append(&id, &Span::new(2, 3, 10, 5, 5));
        elst.append(&id2, &Span::new(0, 1, 0, 5, 10));
        elst.append(&id2, &Span::new(2, 3, 10, 5, 15));
        test_write_with(&mut elst);
    }
}
