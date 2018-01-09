use std::io::{Result as IoResult, Seek, SeekFrom};

use super::segment::Segment;

pub(super) trait Extent {
    fn offset(&self) -> usize;
    fn set_offset(&mut self, offset: usize);

    #[inline]
    fn end_offset(&self) -> usize {
        self.offset() + self.len()
    }

    fn len(&self) -> usize;
    fn set_len(&mut self, len: usize);

    #[inline]
    fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

pub(super) trait Cutable: Clone {
    fn cut_off(&mut self, at: usize, seg: &Segment) -> Self;
    fn cut_to(&mut self, at: usize, seg: &Segment) -> Self;
}

/// Span, continuous area in a segment
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct Span {
    pub(super) begin: usize, // begin chunk index
    pub(super) end: usize, // end chunk index, exclusive
    pub(super) seg_offset: usize, // offset from begin chunk in segment
    pub(super) len: usize, // span length, in bytes
    pub(super) offset: usize, // offset in content, in bytes
}

impl Span {
    pub fn new(
        begin: usize,
        end: usize,
        seg_offset: usize,
        len: usize,
        offset: usize,
    ) -> Self {
        Span {
            begin,
            end,
            seg_offset,
            len,
            offset,
        }
    }

    #[inline]
    pub fn offset_in_seg(&self, seg: &Segment) -> usize {
        seg[self.begin].pos + self.seg_offset
    }

    fn locate(&self, at: usize, seg: &Segment) -> (usize, usize) {
        assert!(self.offset <= at && at <= self.end_offset());
        let chunks = &seg[self.begin..self.end];
        let seg_at = self.offset_in_seg(seg) + at - self.offset;
        let idx = chunks
            .iter()
            .position(|c| c.pos <= seg_at && seg_at <= c.end_pos())
            .unwrap();
        (self.begin + idx, seg_at)
    }

    fn align_up(&self, at: usize, seg: &Segment) -> (usize, usize) {
        let (idx, seg_at) = self.locate(at, seg);
        if seg_at == seg[idx].pos {
            (idx, at)
        } else {
            (idx + 1, at + seg[idx].end_pos() - seg_at)
        }
    }

    fn align_down(&self, at: usize, seg: &Segment) -> (usize, usize) {
        let (idx, seg_at) = self.locate(at, seg);
        if seg_at == seg[idx].end_pos() {
            (idx + 1, at)
        } else {
            (idx, at + seg[idx].pos - seg_at)
        }
    }

    #[inline]
    pub fn merge_up(&mut self, up: &Span) {
        assert!(self.end == up.begin && up.seg_offset == 0);
        self.end = up.end;
        self.len += up.len;
    }
}

impl Extent for Span {
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

impl Cutable for Span {
    fn cut_off(&mut self, at: usize, seg: &Segment) -> Self {
        let (align_idx, align_at) = self.align_up(at, seg);
        let mut ret = self.clone();
        ret.begin = align_idx;
        ret.seg_offset = 0;
        ret.len = if self.end_offset() > align_at {
            self.end_offset() - align_at
        } else {
            0
        };
        ret.offset = align_at;
        self.end = ret.begin;
        self.len = at - self.offset;
        ret
    }

    fn cut_to(&mut self, at: usize, seg: &Segment) -> Self {
        let (align_idx, align_at) = self.align_down(at, seg);
        let delta = at - self.offset;
        let mut ret = self.clone();
        self.begin = align_idx;
        self.seg_offset = at - align_at;
        self.len -= delta;
        self.offset = at;
        ret.end = self.begin;
        ret.len = if align_at > ret.offset {
            align_at - ret.offset
        } else {
            0
        };
        ret
    }
}

impl Seek for Span {
    fn seek(&mut self, pos: SeekFrom) -> IoResult<u64> {
        match pos {
            SeekFrom::Start(pos) => {
                self.offset = pos as usize;
            }
            SeekFrom::End(pos) => {
                self.offset = (self.end_offset() as i64 + pos) as usize;
            }
            SeekFrom::Current(pos) => {
                self.offset = (self.offset as i64 + pos) as usize;
            }
        }
        Ok(self.offset as u64)
    }
}
