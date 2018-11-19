use std::iter::IntoIterator;
use std::ops::Index;
use std::slice::Iter;

use super::{BLKS_PER_FRAME, BLK_SIZE, FRAME_SIZE};

/// Block span
#[derive(Debug, Clone, Copy, Default, PartialEq, Deserialize, Serialize)]
pub struct Span {
    pub begin: usize, // begin block index
    pub cnt: usize,   // number of blocks in the span
}

impl Span {
    #[inline]
    pub fn new(begin: usize, cnt: usize) -> Self {
        Span { begin, cnt }
    }

    #[inline]
    pub fn end(&self) -> usize {
        self.begin + self.cnt
    }

    #[inline]
    pub fn bytes_len(&self) -> usize {
        self.cnt * BLK_SIZE
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.cnt == 0
    }

    pub fn split_to(&mut self, at: usize) -> Span {
        assert!(self.begin <= at && at < self.end());
        let ret = Span {
            begin: self.begin,
            cnt: self.cnt - (self.end() - at),
        };
        self.begin = at;
        self.cnt -= ret.cnt;
        ret
    }

    #[cfg(feature = "storage-zbox")]
    pub fn intersect(&self, other: Span) -> Option<Span> {
        if self.end() < other.begin || other.end() < self.begin {
            return None;
        }
        let begin = std::cmp::max(self.begin, other.begin);
        let end = std::cmp::min(self.end(), other.end());
        Some(Span::new(begin, end - begin))
    }

    pub fn divide_by(&self, size: usize) -> Vec<Span> {
        let mut ret = Vec::new();

        if self.is_empty() {
            return ret;
        }

        let mut span = self.clone();
        let mut at = span.begin + size - span.begin % size;
        while at < span.end() {
            let split = span.split_to(at);
            ret.push(split);
            at += size;
        }
        ret.push(span);
        ret
    }
}

impl IntoIterator for Span {
    type Item = usize;
    type IntoIter = ::std::ops::Range<usize>;

    fn into_iter(self) -> Self::IntoIter {
        self.begin..self.end()
    }
}

/// Block span with offset location
#[derive(Debug, Clone, Copy, Default, PartialEq, Deserialize, Serialize)]
pub struct LocSpan {
    pub span: Span,
    pub offset: usize, // offset in span list
}

impl LocSpan {
    #[inline]
    pub fn new(begin: usize, cnt: usize, offset: usize) -> Self {
        LocSpan {
            span: Span::new(begin, cnt),
            offset,
        }
    }

    pub fn split_to(&mut self, at: usize) -> LocSpan {
        let ret = LocSpan {
            span: self.span.split_to(at),
            offset: self.offset,
        };
        self.offset += ret.span.bytes_len();
        ret
    }
}

/// Entity address
#[derive(Debug, Clone, Default, Deserialize, Serialize)]
pub struct Addr {
    pub len: usize,
    pub list: Vec<LocSpan>,
}

impl Addr {
    #[inline]
    pub fn iter(&self) -> Iter<LocSpan> {
        self.list.iter()
    }

    // append a span to address
    pub fn append(&mut self, span: Span, len: usize) {
        if self.list.is_empty() {
            self.list.push(LocSpan::new(span.begin, span.cnt, 0));
            self.len = len;
            return;
        }

        if span.begin == self.list.last_mut().unwrap().span.end() {
            // merge to the last span
            let last = self.list.last_mut().unwrap();
            last.span.cnt += span.cnt;
        } else {
            self.list.push(LocSpan::new(span.begin, span.cnt, self.len));
        }
        self.len += len;
    }

    // divide address to frames
    pub fn divide_to_frames(&self) -> Vec<Addr> {
        let mut frames = vec![Addr::default()];
        let mut frm_idx = 0;
        let mut blk_cnt = 0;

        for loc_span in self.list.iter() {
            let mut loc_span = loc_span.clone();
            loc_span.offset = frm_idx * FRAME_SIZE + blk_cnt * BLK_SIZE;

            loop {
                let blk_left = BLKS_PER_FRAME - blk_cnt;

                if loc_span.span.cnt <= blk_left {
                    // span can fit into frame
                    frames[frm_idx].list.push(loc_span);
                    blk_cnt += loc_span.span.cnt;
                    break;
                }

                // span cannot fit into frame, must split span first
                let at = loc_span.span.begin + blk_left;
                let split = loc_span.split_to(at);

                // finish current frame and start a new frame
                frames[frm_idx].list.push(split);
                frames[frm_idx].len = FRAME_SIZE;
                frames.push(Addr::default());
                frm_idx += 1;
                blk_cnt = 0;
            }
        }

        // fix the last frame's length
        frames.last_mut().unwrap().len = self.len - frm_idx * FRAME_SIZE;
        assert_eq!(self.len, frames.iter().map(|a| a.len).sum::<usize>());

        frames
    }
}

impl IntoIterator for Addr {
    type Item = LocSpan;
    type IntoIter = ::std::vec::IntoIter<LocSpan>;

    fn into_iter(self) -> Self::IntoIter {
        self.list.into_iter()
    }
}

impl Index<usize> for Addr {
    type Output = LocSpan;

    fn index(&self, index: usize) -> &LocSpan {
        &self.list[index]
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn split_addr() {
        // #1, address is smaller than a frame
        let lspan = LocSpan::new(0, 1, 0);
        let addr = Addr {
            len: 3,
            list: vec![lspan.clone()],
        };
        let frms = addr.divide_to_frames();
        assert_eq!(frms.len(), 1);
        assert_eq!(frms[0].len, addr.len);
        assert_eq!(frms[0].list[0], lspan);

        // #2, address is equal to a frame
        let lspan = LocSpan::new(0, BLKS_PER_FRAME, 0);
        let addr = Addr {
            len: FRAME_SIZE,
            list: vec![lspan.clone()],
        };
        let frms = addr.divide_to_frames();
        assert_eq!(frms.len(), 1);
        assert_eq!(frms[0].len, addr.len);
        assert_eq!(frms[0].list[0], lspan);

        // #3, address is greater than a frame
        let lspan = LocSpan::new(0, BLKS_PER_FRAME + 1, 0);
        let addr = Addr {
            len: FRAME_SIZE + 3,
            list: vec![lspan.clone()],
        };
        let frms = addr.divide_to_frames();
        assert_eq!(frms.len(), 2);
        assert_eq!(frms[0].len, FRAME_SIZE);
        assert_eq!(frms[0].list[0], LocSpan::new(0, BLKS_PER_FRAME, 0));
        assert_eq!(frms[1].len, 3);
        assert_eq!(
            frms[1].list[0],
            LocSpan::new(BLKS_PER_FRAME, 1, FRAME_SIZE)
        );

        // #4, 2 address are smaller than a frame
        let lspan = LocSpan::new(0, 1, 0);
        let lspan2 = LocSpan::new(3, 1, BLK_SIZE);
        let addr = Addr {
            len: BLK_SIZE + 3,
            list: vec![lspan.clone(), lspan2.clone()],
        };
        let frms = addr.divide_to_frames();
        assert_eq!(frms.len(), 1);
        assert_eq!(frms[0].len, addr.len);
        assert_eq!(frms[0].list.len(), 2);
        assert_eq!(frms[0].list[0], lspan);
        assert_eq!(frms[0].list[1], lspan2);

        // #5, 2 address is greater than a frame
        let lspan = LocSpan::new(0, 1, 0);
        let lspan2 = LocSpan::new(3, BLKS_PER_FRAME, BLK_SIZE);
        let addr = Addr {
            len: BLK_SIZE + FRAME_SIZE,
            list: vec![lspan.clone(), lspan2.clone()],
        };
        let frms = addr.divide_to_frames();
        assert_eq!(frms.len(), 2);
        assert_eq!(frms[0].len, FRAME_SIZE);
        assert_eq!(frms[0].list.len(), 2);
        assert_eq!(frms[0].list[0], lspan);
        assert_eq!(
            frms[0].list[1],
            LocSpan::new(lspan2.span.begin, BLKS_PER_FRAME - 1, BLK_SIZE)
        );
        assert_eq!(frms[1].len, BLK_SIZE);
        assert_eq!(frms[1].list.len(), 1);
        assert_eq!(
            frms[1].list[0],
            LocSpan::new(lspan2.span.begin + BLKS_PER_FRAME - 1, 1, FRAME_SIZE)
        );

        // #6, 1 address is greater than 2 frame
        let lspan = LocSpan::new(0, BLKS_PER_FRAME * 2 + 1, 0);
        let addr = Addr {
            len: FRAME_SIZE * 2 + 3,
            list: vec![lspan.clone()],
        };
        let frms = addr.divide_to_frames();
        assert_eq!(frms.len(), 3);
        assert_eq!(frms[0].len, FRAME_SIZE);
        assert_eq!(frms[0].list.len(), 1);
        assert_eq!(frms[0].list[0], LocSpan::new(0, BLKS_PER_FRAME, 0));
        assert_eq!(frms[1].len, FRAME_SIZE);
        assert_eq!(frms[1].list.len(), 1);
        assert_eq!(
            frms[1].list[0],
            LocSpan::new(BLKS_PER_FRAME, BLKS_PER_FRAME, FRAME_SIZE)
        );
        assert_eq!(frms[2].len, 3);
        assert_eq!(frms[2].list.len(), 1);
        assert_eq!(
            frms[2].list[0],
            LocSpan::new(BLKS_PER_FRAME * 2, 1, FRAME_SIZE * 2)
        );
    }
}
