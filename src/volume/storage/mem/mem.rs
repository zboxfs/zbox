use std::collections::HashMap;
use std::cmp::min;

use error::{Error, Result};
use base::IntoRef;
use trans::Eid;
use volume::storage::Storage;

/// Mem Storage
#[derive(Debug)]
pub struct MemStorage {
    map: HashMap<Eid, Vec<u8>>,
}

impl MemStorage {
    pub fn new() -> Self {
        MemStorage {
            map: HashMap::new(),
        }
    }
}

impl Storage for MemStorage {
    fn get(&mut self, buf: &mut [u8], id: &Eid, offset: u64) -> Result<usize> {
        let offset = offset as usize;
        let data = self.map.get(id).ok_or(Error::NotFound)?;
        let copy_len = min(buf.len(), data.len() - offset);
        buf[..copy_len].copy_from_slice(&data[offset..offset + copy_len]);
        Ok(copy_len)
    }

    fn put(&mut self, id: &Eid, buf: &[u8], offset: u64) -> Result<usize> {
        let data = self.map.entry(id.clone()).or_insert(Vec::new());
        if offset == 0 {
            data.clear();
        }
        assert_eq!(offset, data.len() as u64);
        data.extend_from_slice(buf);
        Ok(buf.len())
    }

    fn del(&mut self, id: &Eid) -> Result<()> {
        self.map.remove(id);
        Ok(())
    }
}

impl IntoRef for MemStorage {}

#[cfg(test)]
mod tests {
    use std::time::{Duration, Instant};

    use base::init_env;
    use base::crypto::{Crypto, RandomSeed, RANDOM_SEED_SIZE};
    use trans::Eid;
    use super::*;

    fn speed_str(duration: &Duration, data_len: usize) -> String {
        let secs = duration.as_secs() as f32
            + duration.subsec_nanos() as f32 / 1_000_000_000.0;
        let speed = data_len as f32 / (1024.0 * 1024.0) / secs;
        format!("{} MB/s", speed)
    }

    #[test]
    fn mem_storage_perf() {
        init_env();

        let mut storage = MemStorage::new();

        let id = Eid::new();
        const DATA_LEN: usize = 10 * 1024 * 1024;
        let mut buf = vec![0u8; DATA_LEN];
        let seed = RandomSeed::from(&[0u8; RANDOM_SEED_SIZE]);
        Crypto::random_buf_deterministic(&mut buf, &seed);

        // write
        let now = Instant::now();
        storage.put_all(&id, &buf).unwrap();
        let write_time = now.elapsed();

        // read
        let now = Instant::now();
        storage.get_all(&mut buf, &id).unwrap();
        let read_time = now.elapsed();

        println!(
            "Memory storage perf: read: {}, write: {}",
            speed_str(&read_time, DATA_LEN),
            speed_str(&write_time, DATA_LEN)
        );
    }
}
