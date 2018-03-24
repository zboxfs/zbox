use std::io::Result as IoResult;
use std::fmt::{self, Display};
use std::time::Duration;
use std::path::{Path, PathBuf};
use std::collections::HashSet;

use serde::Serialize;
use reqwest::{header, Client, Response, StatusCode};

use error::Result;
use base::crypto::{Crypto, Key};
use trans::{Eid, Txid};
use volume::storage::Storage;
use super::emap::Emap;
use super::estore::Estore;
use super::http_client::HttpClient;

// transaction session status
#[derive(Debug, PartialEq, Clone, Copy)]
enum SessionStatus {
    Init,      // initial status
    Started,   // transaction started
    Prepare,   // committing preparation started
    Recycle,   // recycling started
    Committed, // transaction committed
    Dispose,   // dispose a committed transaction
}

impl Default for SessionStatus {
    #[inline]
    fn default() -> Self {
        SessionStatus::Init
    }
}

impl Display for SessionStatus {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            SessionStatus::Init => write!(f, "init"),
            SessionStatus::Started => write!(f, "started"),
            SessionStatus::Prepare => write!(f, "prepare"),
            SessionStatus::Recycle => write!(f, "recycle"),
            SessionStatus::Committed => write!(f, "committed"),
            SessionStatus::Dispose => write!(f, "dispose"),
        }
    }
}

impl<'a> From<&'a str> for SessionStatus {
    fn from(val: &str) -> SessionStatus {
        match val {
            "started" => SessionStatus::Started,
            "prepare" => SessionStatus::Prepare,
            "recycle" => SessionStatus::Recycle,
            "committed" => SessionStatus::Committed,
            "dispose" => SessionStatus::Dispose,
            _ => unreachable!(),
        }
    }
}

// transaction session
#[derive(Debug)]
struct Session {
    seq: u64,
    txid: Txid,
    status: SessionStatus,
    wmark: u64,
    emap: Emap,
    deleted: HashSet<Eid>, // deleted entities
    recycle: Vec<Space>,
    base: PathBuf,
    skey: Key,
    crypto: Crypto,
}

impl Session {
    fn new(
        seq: u64,
        txid: Txid,
        base: &Path,
        skey: &Key,
        crypto: &Crypto,
    ) -> Self {
        let mut ret = Session {
            seq,
            txid,
            status: SessionStatus::Init,
            wmark: 0,
            emap: Emap::new(base, txid),
            deleted: HashSet::new(),
            recycle: Vec::new(),
            base: base.to_path_buf(),
            skey: skey.clone(),
            crypto: crypto.clone(),
        };
        ret.emap.set_crypto_key(crypto, skey);
        ret
    }
}

#[derive(Debug)]
struct Cache {
    client: HttpClient,
}

impl Cache {
    fn new(repo_id: &str, access_key: &str) -> Result<Self> {
        Ok(Cache {
            client: HttpClient::new(repo_id, access_key)?,
        })
    }

    #[inline]
    pub fn client(&self) -> &HttpClient {
        &self.client
    }

    #[inline]
    pub fn client_mut(&mut self) -> &mut HttpClient {
        &mut self.client
    }
}

/// Zbox Storage
#[derive(Debug)]
pub struct ZboxStorage {
    emap: Emap,
    cache: Cache,
}

impl ZboxStorage {
    pub fn new(repo_id: &str, access_key: &str) -> Result<Self> {
        Ok(ZboxStorage {
            emap: Emap::new(),
            cache: Cache::new(repo_id, access_key)?,
        })
    }
}

impl Storage for ZboxStorage {
    fn exists(&self, _location: &str) -> Result<bool> {
        let client = self.cache.client();
        let url = client.base_url().to_owned() + "super";
        let resp = client.head(&url)?;
        match resp.status() {
            StatusCode::Ok => return Ok(true),
            StatusCode::NotFound => return Ok(false),
            _ => {}
        }
        resp.error_for_status()?;
        Ok(false)
    }

    fn init(
        &mut self,
        volume_id: &Eid,
        crypto: &Crypto,
        skey: &Key,
    ) -> Result<()> {
        Ok(())
    }

    fn get_super_blk(&self) -> Result<Vec<u8>> {
        self.cache.client().get("super")
    }

    fn put_super_blk(&mut self, super_blk: &[u8]) -> Result<()> {
        self.cache.client_mut().put("super", super_blk)
    }

    fn open(
        &mut self,
        volume_id: &Eid,
        crypto: &Crypto,
        skey: &Key,
    ) -> Result<Txid> {
        Ok(Txid::new_empty())
    }

    fn read(
        &mut self,
        id: &Eid,
        offset: u64,
        buf: &mut [u8],
        txid: Txid,
    ) -> IoResult<usize> {
        Ok(0)
    }

    fn write(
        &mut self,
        id: &Eid,
        offset: u64,
        buf: &[u8],
        txid: Txid,
    ) -> IoResult<usize> {
        Ok(0)
    }

    fn del(&mut self, id: &Eid, txid: Txid) -> Result<Option<Eid>> {
        Ok(None)
    }

    fn begin_trans(&mut self, txid: Txid) -> Result<()> {
        let loc = format!("trans/{}", txid);
        self.cache.client_mut().put(&loc, &[])
    }

    fn abort_trans(&mut self, txid: Txid) -> Result<()> {
        Ok(())
    }

    fn commit_trans(&mut self, txid: Txid) -> Result<()> {
        let loc = format!("trans/{}", txid);
        self.cache.client_mut().delete(&loc)
    }
}

#[cfg(test)]
mod tests {
    use base::init_env;
    use volume::storage::Storage;
    use super::*;

    #[test]
    fn init_open() {
        init_env();
        let mut zbox = ZboxStorage::new("123", "456").unwrap();
        //zbox.put_super_blk(&[1, 2, 3]).unwrap();
        //assert!(zbox.exists("").unwrap());
        //let super_blk = zbox.get_super_blk().unwrap();
        //println!("{:?}", super_blk);

        let txid = Txid::from(222);
        zbox.begin_trans(txid).unwrap();
        zbox.commit_trans(txid).unwrap();
    }
}
