use std::cmp::min;

use bytes::{BufMut, LittleEndian};

use base::crypto::{Crypto, HashKey};
use trans::Txid;
use super::span::SpanList;

/// Location Id
#[derive(Debug, Clone, Copy, Hash, Default, PartialEq, Eq)]
pub struct LocId {
    pub txid: Txid,
    pub idx: u64, // sector index
}

impl LocId {
    pub const BYTES_LEN: usize = 16;

    pub fn new(txid: Txid, idx: u64) -> Self {
        LocId { txid, idx }
    }

    #[inline]
    pub fn lower_blk_bound(&self, sector_blk_cnt: usize) -> u64 {
        self.idx * sector_blk_cnt as u64
    }

    #[inline]
    pub fn upper_blk_bound(&self, sector_blk_cnt: usize) -> u64 {
        (self.idx + 1) * sector_blk_cnt as u64
    }

    pub fn unique_str(&self, hash_key: &HashKey) -> String {
        let mut buf = Vec::with_capacity(16);
        buf.put_u64::<LittleEndian>(self.txid.val());
        buf.put_u64::<LittleEndian>(self.idx);
        let hash = Crypto::hash_with_key(&buf, hash_key);
        hash.to_string()
    }
}

/// Space
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct Space {
    pub txid: Txid,
    pub spans: SpanList,
}

impl Space {
    pub fn new(txid: Txid, spans: SpanList) -> Self {
        Space { txid, spans }
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.spans.len
    }

    #[inline]
    pub fn set_len(&mut self, len: usize) {
        self.spans.len = len;
    }

    pub fn append(&mut self, other: &Space) {
        assert_eq!(self.txid, other.txid);
        let offset = self.spans.len as u64;
        for span in other.spans.iter() {
            let mut span = span.clone();
            span.offset += offset;
            self.spans.append(span, 0);
        }
        self.spans.len += other.len();
    }

    // divide space into sectors
    pub fn divide_into_sectors(
        &self,
        sector_blk_cnt: usize,
    ) -> Vec<(LocId, SpanList)> {
        let mut ret: Vec<(LocId, SpanList)> = Vec::new();
        for span in self.spans.iter() {
            let mut span = span.clone();
            let begin = span.begin / sector_blk_cnt as u64;
            let end = span.end / sector_blk_cnt as u64 + 1;
            for sec_idx in begin..end {
                let sec_id = LocId::new(self.txid, sec_idx);
                let ubound =
                    min(span.end, sec_id.upper_blk_bound(sector_blk_cnt));
                let split = span.split_to(ubound);
                if split.is_empty() {
                    continue;
                }
                if let Some(&mut (loc, ref mut spans)) = ret.last_mut() {
                    if loc.idx == sec_idx {
                        spans.append(split, split.blk_len());
                        continue;
                    }
                }
                ret.push((sec_id, split.into_span_list(split.blk_len())));
            }
        }
        ret
    }
}
