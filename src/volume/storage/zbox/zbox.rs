use std::io::Result as IoResult;
use std::time::Duration;

use reqwest::{header, Client, StatusCode};

use error::Result;
use base::crypto::{Crypto, Key};
use trans::{Eid, Txid};
use volume::storage::Storage;

/// Zbox Storage
#[derive(Debug)]
pub struct ZboxStorage {
    base_url: String,
    client: Client,
}

impl ZboxStorage {
    pub fn new(repo_id: &str, access_key: &str) -> Self {
        let mut headers = header::Headers::new();
        headers.set(header::Authorization("Basic ".to_owned() + access_key));

        let client = Client::builder()
            .timeout(Duration::from_secs(30))
            .default_headers(headers)
            .build()
            .unwrap();

        ZboxStorage {
            base_url: "https://data.zbox.io/repos/".to_owned() + repo_id + "/",
            client,
        }
    }
}

impl Storage for ZboxStorage {
    fn exists(&self, _location: &str) -> bool {
        let url = self.base_url.to_string() + "super";
        let resp = self.client.head(&url).send().unwrap();
        resp.status() == StatusCode::Ok
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
        let url = self.base_url.to_string() + "super";
        let mut resp = self.client.get(&url).send()?.error_for_status()?;
        let mut buf: Vec<u8> = Vec::new();
        resp.copy_to(&mut buf)?;
        Ok(buf)
    }

    fn put_super_blk(&mut self, super_blk: &[u8]) -> Result<()> {
        let url = self.base_url.to_string() + "super";
        let resp = self.client
            .put(&url)
            .body(super_blk.to_vec())
            .send()?
            .error_for_status()?;
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
        let mut zbox = ZboxStorage::new("123", "456");
        //zbox.exists("");
        zbox.put_super_blk(&[1, 2, 3]).unwrap();
        let super_blk = zbox.get_super_blk().unwrap();
        println!("{:?}", super_blk);
    }
}
