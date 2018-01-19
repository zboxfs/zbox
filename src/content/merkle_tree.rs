use std::fmt::{self, Debug};
use std::cmp::{min, max};
use std::io::{Read, Write, Result as IoResult, Seek, SeekFrom};

use error::Result;
use base::crypto::{Crypto, Hash, HashState, HASH_STATE_SIZE};
use base::utils;

// data piece size, must be 2^n
const PIECE_SIZE: usize = 256 * 1024;

#[inline]
fn align_piece_offset(n: usize) -> usize {
    utils::align_offset(n, PIECE_SIZE)
}

#[inline]
fn align_piece_floor(n: usize) -> usize {
    utils::align_floor(n, PIECE_SIZE)
}

#[inline]
fn align_piece_floor_chunk(n: usize) -> usize {
    utils::align_floor_chunk(n, PIECE_SIZE)
}

#[inline]
fn align_piece_ceil_chunk(n: usize) -> usize {
    utils::align_ceil_chunk(n, PIECE_SIZE)
}

// get parent node index
fn parent(n: usize, lvl_begin: usize, lvl_node_cnt: usize) -> usize {
    let upper_lvl_node_cnt = (lvl_node_cnt + 1) / 2;
    let upper_lvl_begin = lvl_begin - upper_lvl_node_cnt;
    upper_lvl_begin + (n - lvl_begin) / 2
}

// read once data piece and calculate its hash
fn piece_hash<R: Read + Seek>(offset: usize, rdr: &mut R) -> IoResult<Hash> {
    rdr.seek(SeekFrom::Start(align_piece_floor(offset) as u64))?;
    let mut buf = vec![0u8; PIECE_SIZE];
    let mut pos = 0;
    let mut state = Crypto::hash_init();

    loop {
        let read = rdr.read(&mut buf[pos..])?;
        if read == 0 {
            break;
        }
        Crypto::hash_update(&mut state, &buf[pos..pos + read]);
        pos += read;
    }

    Ok(Crypto::hash_final(&mut state))
}

// calculate total number of tree nodes, including leaf nodes
fn tree_node_cnt(leaf_cnt: usize) -> usize {
    let mut s = 1;
    let mut n = leaf_cnt;
    while n > 1 {
        s += n;
        n = (n + 1) / 2;
    }
    s
}

#[derive(Debug, Deserialize, Serialize)]
pub struct MerkleTree {
    len: usize,
    nodes: Vec<Hash>,
}

impl MerkleTree {
    #[inline]
    pub fn root_hash(&self) -> &Hash {
        &self.nodes.first().unwrap()
    }

    #[inline]
    fn leaf_cnt(&self) -> usize {
        align_piece_ceil_chunk(self.len)
    }

    // inner nodes count
    #[inline]
    fn inner_cnt(&self) -> usize {
        self.nodes.len() - self.leaf_cnt()
    }

    // calculate hash from its children nodes' indices
    fn hash_up(
        &mut self,
        indices: &[usize],
        lvl_begin: usize,
        lvl_node_cnt: usize,
    ) {
        assert!(indices.len() == 1 || indices.len() == 2);
        let m = indices[0];
        let parent = parent(m, lvl_begin, lvl_node_cnt);
        if indices.len() == 2 {
            let mut state: HashState = [0u8; HASH_STATE_SIZE];
            Crypto::hash_init_to(&mut state);
            Crypto::hash_update(&mut state, &self.nodes[m]);
            Crypto::hash_update(&mut state, &self.nodes[m + 1]);
            Crypto::hash_final_to(&mut state, &mut self.nodes[parent]);
        } else {
            self.nodes[parent] = self.nodes[m].clone();
        }
    }

    // build merkle tree from bottom up
    fn build(len: usize, leaves: &[Hash]) -> MerkleTree {
        let leaf_cnt = leaves.len();
        let total_node_cnt = tree_node_cnt(leaf_cnt);
        let inner_node_cnt = total_node_cnt - leaf_cnt;

        let mut mtree = MerkleTree {
            len,
            nodes: vec![Hash::new_empty(); inner_node_cnt],
        };

        // append leaf nodes
        mtree.nodes.extend_from_slice(leaves);

        // calculate inner nodes hash from bottom up
        let mut begin = inner_node_cnt;
        let mut end = total_node_cnt;
        let mut lvl_node_cnt = leaf_cnt;
        while begin >= 1 {
            let indices: Vec<usize> = (begin..end).collect();
            for pair in indices.chunks(2) {
                mtree.hash_up(pair, begin, lvl_node_cnt);
            }
            end = begin;
            begin = parent(begin, begin, lvl_node_cnt);
            lvl_node_cnt = (lvl_node_cnt + 1) / 2
        }

        mtree
    }

    // merge other merkle tree to self
    pub fn merge<R: Read + Seek>(
        &mut self,
        offset: usize,
        len: usize,
        leaves: &[Hash],
        rdr: &mut R,
    ) -> Result<()> {
        assert!(offset <= self.len);

        let end_offset = max(self.len, offset + len);
        let leaf_cnt = align_piece_ceil_chunk(end_offset);
        let node_cnt = tree_node_cnt(leaf_cnt);
        let leaves_begin = node_cnt - leaf_cnt;
        let mut old_begin = self.inner_cnt();
        let old_leaf_cnt = self.leaf_cnt();

        let mut overlap_begin = leaves_begin + align_piece_floor_chunk(offset);
        let overlap_end_offset = min(self.len, offset + len);
        let mut overlap_end = leaves_begin +
            align_piece_ceil_chunk(overlap_end_offset);

        // resize nodes and move old leaf nodes
        let old_leaves = self.nodes[old_begin..].to_vec();
        self.nodes.resize(node_cnt, Hash::new_empty());
        self.nodes[leaves_begin..leaves_begin + old_leaves.len()]
            .clone_from_slice(&old_leaves[..]);

        // copy in leave nodes
        &self.nodes[overlap_begin..overlap_begin + leaves.len()]
            .clone_from_slice(&leaves[..]);

        // re-hash head and tail overlapping pieces
        let mut head_is_rehashed = false;
        if align_piece_offset(offset) != 0 {
            self.nodes[overlap_begin] = piece_hash(offset, rdr)?;
            head_is_rehashed = true;
        }
        if align_piece_offset(overlap_end_offset) != 0 &&
            !(overlap_begin == overlap_end - 1 && head_is_rehashed)
        {
            self.nodes[overlap_end - 1] = piece_hash(overlap_end_offset, rdr)?;
        }

        // re-calculate inner nodes hash from bottom up
        let mut begin = leaves_begin;
        let mut end = node_cnt;
        let mut lvl_node_cnt = leaf_cnt;
        let mut old_lvl_node_cnt = old_leaf_cnt;
        while begin >= 1 {
            let indices: Vec<usize> = (begin..end).collect();
            for pair in indices.chunks(2).rev() {
                if pair.len() == 2 && pair[1] < overlap_begin {
                    // copy hash from old tree
                    let parent_node = parent(pair[0], begin, lvl_node_cnt);
                    let old = parent(
                        old_begin + pair[0] - begin,
                        old_begin,
                        old_lvl_node_cnt,
                    );
                    assert!(parent_node >= old);
                    if old != parent_node {
                        self.nodes[parent_node] = self.nodes[old].clone();
                    }
                } else {
                    // re-calculate hash
                    self.hash_up(pair, begin, lvl_node_cnt);
                }
            }
            overlap_begin = parent(overlap_begin, begin, lvl_node_cnt);
            overlap_end = parent(overlap_end, begin, lvl_node_cnt);
            end = begin;
            begin = parent(begin, begin, lvl_node_cnt);
            lvl_node_cnt = (lvl_node_cnt + 1) / 2;
            if old_begin > 0 {
                old_begin = parent(old_begin, old_begin, old_lvl_node_cnt);
                old_lvl_node_cnt = (old_lvl_node_cnt + 1) / 2;
            }
        }

        self.len = end_offset;

        Ok(())
    }

    // truncate pieces and re-calculate merkle tree
    pub fn truncate<R: Read + Seek>(
        &mut self,
        at: usize,
        rdr: &mut R,
    ) -> Result<()> {
        assert!(at <= self.len);

        if at == self.len {
            return Ok(());
        }

        let leaf_cnt = align_piece_ceil_chunk(at);
        let node_cnt = tree_node_cnt(leaf_cnt);
        let leaves_begin = node_cnt - leaf_cnt;
        let mut new = MerkleTree {
            len: at,
            nodes: vec![Hash::new_empty(); node_cnt],
        };

        // copy leaf nodes
        let src = self.inner_cnt();
        new.nodes[leaves_begin..].clone_from_slice(
            &self.nodes[src..src + leaf_cnt],
        );

        // re-hash the last piece at cut position
        if align_piece_offset(at) != 0 || at == 0 {
            new.nodes[node_cnt - 1] = piece_hash(at, rdr)?;
        }

        // re-calculate inner nodes hash from bottom up
        let mut begin = leaves_begin;
        let mut end = node_cnt;
        let mut lvl_node_cnt = leaf_cnt;
        let mut old_begin = self.inner_cnt();
        let mut old_end = old_begin + leaf_cnt;
        let mut old_lvl_node_cnt = self.leaf_cnt();
        while begin >= 1 {
            // copy nodes from self
            let dst_begin = parent(begin, begin, lvl_node_cnt);
            let dst_end = parent(end - 1, begin, lvl_node_cnt) + 1;
            let src_begin = parent(old_begin, old_begin, old_lvl_node_cnt);
            let src_end = parent(old_end - 1, old_begin, old_lvl_node_cnt) + 1;
            assert_eq!(dst_end - dst_begin, src_end - src_begin);
            &new.nodes[dst_begin..dst_end].clone_from_slice(
                &self.nodes[src_begin..src_end],
            );

            // re-hash the last node
            if (end - begin) & 1 == 0 {
                new.hash_up(&[end - 2, end - 1], begin, lvl_node_cnt);
            } else {
                new.hash_up(&[end - 1], begin, lvl_node_cnt);
            }

            end = begin;
            begin = dst_begin;
            lvl_node_cnt = (lvl_node_cnt + 1) / 2;
            old_begin = src_begin;
            old_end = src_end;
            old_lvl_node_cnt = (old_lvl_node_cnt + 1) / 2;
        }

        *self = new;

        Ok(())
    }
}

// merkle tree builder
pub struct Writer {
    offset: usize,
    len: usize,
    state: HashState,
    hash_offset: usize,
    nodes: Vec<Hash>,
}

impl Writer {
    pub fn new(offset: usize) -> Self {
        Writer {
            offset,
            len: 0,
            state: Crypto::hash_init(),
            hash_offset: offset,
            nodes: Vec::new(),
        }
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.len
    }

    #[inline]
    pub fn nodes(&self) -> &[Hash] {
        &self.nodes
    }

    pub fn finish(&mut self) {
        if self.len == 0 || align_piece_offset(self.hash_offset) != 0 {
            self.nodes.push(Crypto::hash_final(&mut self.state));
        }
    }
}

impl Write for Writer {
    fn write(&mut self, data: &[u8]) -> IoResult<usize> {
        let mut data_pos = 0;
        let data_len = data.len();

        while data_pos < data.len() {
            let pos = align_piece_offset(self.hash_offset);
            let hash_len = min(PIECE_SIZE - pos, data_len - data_pos);

            Crypto::hash_update(
                &mut self.state,
                &data[data_pos..data_pos + hash_len],
            );

            // reached piece boundary, finish its hash and start a new round
            if align_piece_offset(self.hash_offset + hash_len) <= pos {
                let hash = Crypto::hash_final(&mut self.state);
                self.nodes.push(hash);
                self.state = Crypto::hash_init();
            }

            data_pos += hash_len;
            self.hash_offset += hash_len;
        }

        self.len += data_len;

        Ok(data_len)
    }

    fn flush(&mut self) -> IoResult<()> {
        Ok(())
    }
}

impl Debug for Writer {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("MerkleTreeBuilder")
            .field("offset", &self.offset)
            .field("len", &self.len)
            .field("nodes", &self.nodes)
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use std::io::Cursor;
    use base::init_env;
    use base::crypto::RandomSeed;
    use super::*;

    fn calculate_merkle_hash(buf: &[u8]) -> Hash {
        let mut parent = Vec::new();
        let mut children = Vec::new();

        if buf.is_empty() {
            return Crypto::hash(buf);
        }

        for chunk in buf.chunks(PIECE_SIZE) {
            children.push(Crypto::hash(chunk));
        }

        while children.len() > 1 {
            for pair in children.chunks(2) {
                let mut child: Vec<u8> = pair[0].to_vec();
                if pair.len() > 1 {
                    child.append(&mut pair[1].to_vec());
                    parent.push(Crypto::hash(&child));
                } else {
                    parent.push(pair[0].clone());
                }
            }
            children = parent.clone();
            parent.clear();
        }

        children[0].clone()
    }

    fn make_writer(offset: usize, buf: &[u8]) -> Writer {
        let mut wtr = Writer::new(offset);
        for chunk in buf.chunks(PIECE_SIZE) {
            wtr.write(&chunk[..]).unwrap();
        }
        wtr.finish();
        wtr
    }

    fn build_mtree(buf: &[u8]) -> MerkleTree {
        let wtr = make_writer(0, buf);
        MerkleTree::build(wtr.len(), wtr.nodes())
    }

    fn test_builder(len: usize) {
        let mut buf = vec![0u8; len];
        Crypto::random_buf_deterministic(&mut buf, &RandomSeed::default());
        let mtree = build_mtree(&buf);
        let ctl = calculate_merkle_hash(&buf);
        assert_eq!(mtree.root_hash(), &ctl);
    }

    #[test]
    fn build_merkle_tree() {
        init_env();

        for i in 0..35 {
            test_builder(PIECE_SIZE * i);
            test_builder(PIECE_SIZE * i + 3);
        }
    }

    fn test_merge(dst_len: usize, src_len: usize, offset: usize) {
        let mut src = vec![0u8; src_len];
        Crypto::random_buf_deterministic(&mut src, &RandomSeed::default());
        let mut dst = vec![0u8; dst_len];
        Crypto::random_buf_deterministic(&mut dst, &RandomSeed::default());

        let mut mtree = build_mtree(&dst[..]);
        let wtr = make_writer(offset, &src[..]);
        dst.resize(max(dst_len, offset + src_len), 0);
        &dst[offset..offset + src_len].copy_from_slice(&src[..]);

        let mut rdr = Cursor::new(&dst);
        mtree.merge(offset, src_len, wtr.nodes(), &mut rdr).unwrap();

        let ctl = calculate_merkle_hash(&dst);
        assert_eq!(mtree.len, dst.len());
        assert_eq!(mtree.root_hash(), &ctl);
    }

    #[test]
    fn merge_merkle_tree() {
        init_env();

        test_merge(3, 2, 0);
        test_merge(3, 2, 1);
        test_merge(3, 2, 3);
        test_merge(PIECE_SIZE, PIECE_SIZE, 1);
        test_merge(PIECE_SIZE * 2, PIECE_SIZE, 1);
        test_merge(PIECE_SIZE * 2, PIECE_SIZE * 2, 1);
        test_merge(PIECE_SIZE * 2, PIECE_SIZE * 2, PIECE_SIZE);
        test_merge(PIECE_SIZE * 2, PIECE_SIZE * 2, PIECE_SIZE + 1);
        test_merge(PIECE_SIZE * 2, PIECE_SIZE * 2, PIECE_SIZE * 2);
        test_merge(PIECE_SIZE * 3, 3, PIECE_SIZE * 2 + 1);
        test_merge(PIECE_SIZE * 3, PIECE_SIZE, PIECE_SIZE * 2 + 1);
        test_merge(PIECE_SIZE * 3, PIECE_SIZE * 2, PIECE_SIZE);
        test_merge(PIECE_SIZE * 3, PIECE_SIZE * 2, PIECE_SIZE * 2 + 1);
        test_merge(PIECE_SIZE * 3, PIECE_SIZE * 2, PIECE_SIZE * 3);
        test_merge(PIECE_SIZE * 4, PIECE_SIZE * 2, PIECE_SIZE * 2 - 2);
    }

    #[test]
    fn merge_merkle_tree_fuzz() {
        init_env();

        for i in 1..20 {
            let len = PIECE_SIZE * i + Crypto::random_u32(6u32) as usize - 3;
            let len2 = PIECE_SIZE *
                (Crypto::random_u32(i as u32) as usize + 1) +
                Crypto::random_u32(6u32) as usize - 3;
            let offset = Crypto::random_u32(len as u32) as usize;
            test_merge(len, len2, 0);
            test_merge(len, len2, offset);
            test_merge(len, len2, len);
        }
    }

    fn test_truncate(len: usize, at: usize) {
        let mut buf = vec![0u8; len];
        Crypto::random_buf_deterministic(&mut buf, &RandomSeed::default());
        let mut mtree = build_mtree(&buf[..]);

        let cutoff = &buf[..at];
        let mut rdr = Cursor::new(cutoff);
        mtree.truncate(at, &mut rdr).unwrap();

        let ctl = calculate_merkle_hash(cutoff);
        assert_eq!(mtree.len, cutoff.len());
        assert_eq!(mtree.root_hash(), &ctl);
    }

    #[test]
    fn truncate_merkle_tree() {
        init_env();

        test_truncate(2, 0);
        test_truncate(2, 1);
        test_truncate(2, 2);
        test_truncate(PIECE_SIZE, 0);
        test_truncate(PIECE_SIZE, 1);
        test_truncate(PIECE_SIZE, PIECE_SIZE);
        test_truncate(PIECE_SIZE * 2, 1);
        test_truncate(PIECE_SIZE * 2, PIECE_SIZE);
        test_truncate(PIECE_SIZE * 2, PIECE_SIZE + 1);
        test_truncate(PIECE_SIZE * 3, 0);
        test_truncate(PIECE_SIZE * 3, 1);
        test_truncate(PIECE_SIZE * 3, PIECE_SIZE);
        test_truncate(PIECE_SIZE * 3, PIECE_SIZE + 1);
        test_truncate(PIECE_SIZE * 3, PIECE_SIZE * 2);
        test_truncate(PIECE_SIZE * 3, PIECE_SIZE * 2 + 1);
        test_truncate(PIECE_SIZE * 3, PIECE_SIZE * 3);
        test_truncate(PIECE_SIZE * 4, 0);
        test_truncate(PIECE_SIZE * 4, 1);
        test_truncate(PIECE_SIZE * 4, PIECE_SIZE);
        test_truncate(PIECE_SIZE * 4, PIECE_SIZE + 1);
        test_truncate(PIECE_SIZE * 4, PIECE_SIZE * 2);
        test_truncate(PIECE_SIZE * 4, PIECE_SIZE * 2 + 1);
        test_truncate(PIECE_SIZE * 4, PIECE_SIZE * 3);
        test_truncate(PIECE_SIZE * 4, PIECE_SIZE * 3 + 1);
        test_truncate(PIECE_SIZE * 4, PIECE_SIZE * 4);
    }

    #[test]
    fn truncate_merkle_tree_fuzz() {
        init_env();

        for i in 1..20 {
            let len = PIECE_SIZE * i + Crypto::random_u32(6u32) as usize - 3;
            let at = Crypto::random_u32(len as u32) as usize;
            test_truncate(len, 0);
            test_truncate(len, at);
            test_truncate(len, len);
        }
    }

}
