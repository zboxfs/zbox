use std::io::{Error as IoError, ErrorKind, Result as IoResult};

use reqwest;

use error::{Error, Result};
use base::crypto::{Crypto, Key};
use trans::{Eid, Txid};
use volume::storage::Storage;

/// Zbox Storage
#[derive(Debug)]
pub struct ZboxStorage {}

impl ZboxStorage {
    const API_URL: &'static str = "https://api.zbox.io/";

    pub fn new() -> Self {
        ZboxStorage {}
    }

    #[inline]
    fn make_url<S: AsRef<str>>(&self, path: S) -> String {
        ZboxStorage::API_URL.to_string() + path.as_ref()
    }

    #[inline]
    fn make_repo_url<S: AsRef<str>>(&self, path: S) -> String {
        ZboxStorage::API_URL.to_string() + "repos/" + path.as_ref()
    }
}

impl Storage for ZboxStorage {
    fn exists(&self, location: &str) -> bool {
        let url = self.make_repo_url(location);
        println!("url: {:#?}", url);
        //let text = reqwest::get(url).unwrap().text().unwrap();
        //println!("body = {:?}", text);
        false
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
        Ok(Vec::new())
    }

    fn put_super_blk(&mut self, super_blk: &[u8]) -> Result<()> {
        Ok(())
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
        Ok(())
    }

    fn abort_trans(&mut self, txid: Txid) -> Result<()> {
        Ok(())
    }

    fn commit_trans(&mut self, txid: Txid) -> Result<()> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn init_open() {
        let zbox = ZboxStorage::new();
        zbox.exists("123");
    }
}
