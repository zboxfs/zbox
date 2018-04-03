use std::slice::Iter;

use base::utils::align_offset_u64;

// block size, in bytes
pub const BLK_SIZE: usize = 4 * 1024;

/// Span
#[derive(Debug, Clone, Copy, Default, PartialEq, Deserialize, Serialize)]
pub struct Span {
    pub(super) begin: u64,  // begin block index
    pub(super) end: u64,    // end block index, exclusive
    pub(super) offset: u64, // offset in span list
}

impl Span {
    pub fn new(begin: u64, end: u64, offset: u64) -> Self {
        Span { begin, end, offset }
    }

    #[inline]
    pub fn end_offset(&self) -> u64 {
        self.offset + (self.blk_cnt() * BLK_SIZE) as u64
    }

    #[inline]
    pub fn blk_cnt(&self) -> usize {
        (self.end - self.begin) as usize
    }

    #[inline]
    pub fn blk_len(&self) -> usize {
        self.blk_cnt() * BLK_SIZE
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.blk_cnt() == 0
    }

    #[inline]
    pub fn offset_in_sec(&self, offset: u64, sector_blk_cnt: u64) -> u64 {
        assert!(self.offset <= offset && offset <= self.end_offset());
        align_offset_u64(self.begin, sector_blk_cnt) * BLK_SIZE as u64 + offset
            - self.offset
    }

    pub fn into_span_list(self, len: usize) -> SpanList {
        let mut spans = SpanList::new();
        spans.append(self, len);
        spans
    }

    #[inline]
    pub fn merge_up(&mut self, up: &Span) {
        assert!(self.end == up.begin);
        self.end = up.end;
    }

    pub fn split_to(&mut self, at: u64) -> Span {
        assert!(self.begin <= at && at <= self.end);
        let mut ret = self.clone();
        self.offset += (at - self.begin) * BLK_SIZE as u64;
        self.begin = at;
        ret.end = at;
        ret
    }
}

/// Span list
#[derive(Debug, Clone, Default, Deserialize, Serialize)]
pub struct SpanList {
    pub(super) len: usize,
    pub(super) list: Vec<Span>,
}

impl SpanList {
    pub fn new() -> Self {
        SpanList::default()
    }

    #[inline]
    pub fn offset(&self) -> u64 {
        self.list.first().unwrap().offset
    }

    pub fn blk_len(&self) -> usize {
        self.list.iter().fold(0, |sum, &s| sum + s.blk_len())
    }

    /// Append span to span list, merge it if possible
    pub fn append(&mut self, span: Span, len: usize) {
        if let Some(last) = self.list.last_mut() {
            if last.end == span.begin {
                last.merge_up(&span);
                self.len += len;
                return;
            }
        }

        self.len += len;
        self.list.push(span);
    }

    pub fn join(&mut self, other: &SpanList) {
        self.list.extend_from_slice(&other.list);
        self.len += other.len;
    }

    #[inline]
    pub fn iter(&self) -> Iter<Span> {
        self.list.iter()
    }
}

impl IntoIterator for SpanList {
    type Item = Span;
    type IntoIter = ::std::vec::IntoIter<Span>;

    fn into_iter(self) -> Self::IntoIter {
        self.list.into_iter()
    }
}
