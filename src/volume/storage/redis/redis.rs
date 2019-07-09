use std::fmt::{self, Debug};
use std::sync::Mutex;

use redis::{Client, Commands, Connection};

use base::crypto::{Crypto, Key};
use base::IntoRef;
use error::{Error, Result};
use trans::Eid;
use volume::address::Span;
use volume::storage::Storable;
use volume::BLK_SIZE;

// redis key for super block
#[inline]
fn super_blk_key(suffix: u64) -> String {
    format!("super_blk:{}", suffix)
}

// redis key for wal
#[inline]
fn wal_key(id: &Eid) -> String {
    format!("wal:{}", id.to_string())
}

// redis key for address
#[inline]
fn addr_key(id: &Eid) -> String {
    format!("address:{}", id.to_string())
}

// redis key for block
#[inline]
fn blk_key(blk_idx: usize) -> String {
    format!("block:{}", blk_idx)
}

/// Redis Storage
pub struct RedisStorage {
    client: Client,
    conn: Option<Mutex<Connection>>,
}

impl RedisStorage {
    pub fn new(path: &str) -> Result<Self> {
        // url format:
        // redis://[:<passwd>@]<hostname>[:port][/<db>]
        // redis+unix:///[:<passwd>@]<path>[?db=<db>]
        let url = if path.starts_with("+unix+") {
            format!("redis+unix:///{}", &path[6..])
        } else {
            format!("redis://{}", path)
        };
        let client = Client::open(url.as_str())?;

        Ok(RedisStorage { client, conn: None })
    }

    fn get_bytes(&self, key: &str) -> Result<Vec<u8>> {
        match self.conn {
            Some(ref conn) => {
                let conn = conn.lock().unwrap();
                if !conn.exists::<&str, bool>(key)? {
                    return Err(Error::NotFound);
                }
                let ret = conn.get(key)?;
                Ok(ret)
            }
            None => unreachable!(),
        }
    }

    fn set_bytes(&self, key: &str, val: &[u8]) -> Result<()> {
        match self.conn {
            Some(ref conn) => {
                let conn = conn.lock().unwrap();
                conn.set(key, val)?;
                Ok(())
            }
            None => unreachable!(),
        }
    }

    fn del(&self, key: &str) -> Result<()> {
        match self.conn {
            Some(ref conn) => {
                let conn = conn.lock().unwrap();
                conn.del(key)?;
                Ok(())
            }
            None => unreachable!(),
        }
    }
}

impl Storable for RedisStorage {
    fn exists(&self) -> Result<bool> {
        // check super block existence to determine if repo exists
        let conn = self.client.get_connection()?;
        let key = super_blk_key(0);
        conn.exists::<&str, bool>(&key).map_err(Error::from)
    }

    fn connect(&mut self) -> Result<()> {
        let conn = self.client.get_connection()?;
        self.conn = Some(Mutex::new(conn));
        Ok(())
    }

    #[inline]
    fn init(&mut self, _crypto: Crypto, _key: Key) -> Result<()> {
        Ok(())
    }

    #[inline]
    fn open(&mut self, _crypto: Crypto, _key: Key) -> Result<()> {
        Ok(())
    }

    fn get_super_block(&mut self, suffix: u64) -> Result<Vec<u8>> {
        let key = super_blk_key(suffix);
        self.get_bytes(&key)
    }

    fn put_super_block(&mut self, super_blk: &[u8], suffix: u64) -> Result<()> {
        let key = super_blk_key(suffix);
        self.set_bytes(&key, super_blk)
    }

    fn get_wal(&mut self, id: &Eid) -> Result<Vec<u8>> {
        let key = wal_key(id);
        self.get_bytes(&key)
    }

    fn put_wal(&mut self, id: &Eid, wal: &[u8]) -> Result<()> {
        let key = wal_key(id);
        self.set_bytes(&key, wal)
    }

    fn del_wal(&mut self, id: &Eid) -> Result<()> {
        let key = wal_key(id);
        self.del(&key)
    }

    fn get_address(&mut self, id: &Eid) -> Result<Vec<u8>> {
        let key = addr_key(id);
        self.get_bytes(&key)
    }

    fn put_address(&mut self, id: &Eid, addr: &[u8]) -> Result<()> {
        let key = addr_key(id);
        self.set_bytes(&key, addr)
    }

    fn del_address(&mut self, id: &Eid) -> Result<()> {
        let key = addr_key(id);
        self.del(&key)
    }

    fn get_blocks(&mut self, dst: &mut [u8], span: Span) -> Result<()> {
        let mut read = 0;
        for blk_idx in span {
            let key = blk_key(blk_idx);
            let blk = self.get_bytes(&key)?;
            assert_eq!(blk.len(), BLK_SIZE);
            dst[read..read + BLK_SIZE].copy_from_slice(&blk);
            read += BLK_SIZE;
        }

        Ok(())
    }

    fn put_blocks(&mut self, span: Span, mut blks: &[u8]) -> Result<()> {
        for blk_idx in span {
            let key = blk_key(blk_idx);
            self.set_bytes(&key, &blks[..BLK_SIZE])?;
            blks = &blks[BLK_SIZE..];
        }

        Ok(())
    }

    fn del_blocks(&mut self, span: Span) -> Result<()> {
        for blk_idx in span {
            let key = blk_key(blk_idx);
            self.del(&key)?;
        }
        Ok(())
    }

    #[inline]
    fn flush(&mut self) -> Result<()> {
        Ok(())
    }
}

impl Debug for RedisStorage {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("RedisStorage").finish()
    }
}

impl IntoRef for RedisStorage {}

#[cfg(test)]
mod tests {

    use super::*;
    use base::init_env;

    #[test]
    fn redis_storage() {
        init_env();
        let mut rs = RedisStorage::new("127.0.0.1").unwrap();
        rs.connect().unwrap();
        rs.init(Crypto::default(), Key::new_empty()).unwrap();

        let id = Eid::new();
        let buf = vec![1, 2, 3];
        let blks = vec![42u8; BLK_SIZE * 3];
        let mut dst = vec![0u8; BLK_SIZE * 3];

        // super block
        rs.put_super_block(&buf, 0).unwrap();
        let s = rs.get_super_block(0).unwrap();
        assert_eq!(&s[..], &buf[..]);

        // wal
        rs.put_wal(&id, &buf).unwrap();
        let s = rs.get_wal(&id).unwrap();
        assert_eq!(&s[..], &buf[..]);
        rs.del_wal(&id).unwrap();
        assert_eq!(rs.get_wal(&id).unwrap_err(), Error::NotFound);

        // address
        rs.put_address(&id, &buf).unwrap();
        let s = rs.get_address(&id).unwrap();
        assert_eq!(&s[..], &buf[..]);
        rs.del_address(&id).unwrap();
        assert_eq!(rs.get_address(&id).unwrap_err(), Error::NotFound);

        // block
        let span = Span::new(0, 3);
        rs.put_blocks(span, &blks).unwrap();
        rs.get_blocks(&mut dst, span).unwrap();
        assert_eq!(&dst[..], &blks[..]);
        rs.del_blocks(Span::new(1, 2)).unwrap();
        assert_eq!(
            rs.get_blocks(&mut dst, Span::new(0, 3)).unwrap_err(),
            Error::NotFound
        );
        assert_eq!(
            rs.get_blocks(&mut dst[..BLK_SIZE], Span::new(1, 1))
                .unwrap_err(),
            Error::NotFound
        );
        assert_eq!(
            rs.get_blocks(&mut dst[..BLK_SIZE], Span::new(2, 1))
                .unwrap_err(),
            Error::NotFound
        );

        // re-open
        drop(rs);
        let mut rs = RedisStorage::new("127.0.0.1").unwrap();
        rs.connect().unwrap();
        rs.open(Crypto::default(), Key::new_empty()).unwrap();

        rs.get_blocks(&mut dst[..BLK_SIZE], Span::new(0, 1))
            .unwrap();
        assert_eq!(&dst[..BLK_SIZE], &blks[..BLK_SIZE]);
        assert_eq!(
            rs.get_blocks(&mut dst[..BLK_SIZE], Span::new(1, 1))
                .unwrap_err(),
            Error::NotFound
        );
        assert_eq!(
            rs.get_blocks(&mut dst[..BLK_SIZE], Span::new(2, 1))
                .unwrap_err(),
            Error::NotFound
        );
    }
}
