use std::io::{Result as IoResult, Seek, SeekFrom};

/// Span, continuous area in a segment
#[derive(Debug, Clone, Default, Deserialize, Serialize)]
pub struct Span {
    pub(super) begin: usize, // begin chunk index
    pub(super) end: usize, // end chunk index, exclusive
    pub(super) seg_offset: usize, // offset in segment, in bytes
    pub(super) len: usize, // span length, in bytes
    pub(super) offset: usize, // offset in content
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
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    #[inline]
    pub fn end_seg_offset(&self) -> usize {
        self.seg_offset + self.len
    }

    #[inline]
    pub fn end_offset(&self) -> usize {
        self.offset + self.len
    }

    pub fn merge_up(&mut self, up: &Span) {
        assert!(self.end_seg_offset() == up.seg_offset);
        self.end = up.end;
        self.len += up.len;
    }

    pub fn split_off(&mut self, at: usize) -> Span {
        assert!(self.offset <= at && at < self.end_offset());
        let mut ret = self.clone();
        let delta = at - self.offset;
        ret.seg_offset = self.seg_offset + delta;
        ret.len = self.len - delta;
        ret.offset = self.offset + delta;
        self.len = delta;
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
