use std::error::Error as StdError;
use std::io::{Error as IoError, ErrorKind, Read, Result as IoResult, Write};
use std::fmt::{self, Display};
use std::time::Duration;
use std::path::{Path, PathBuf};
use std::collections::HashMap;
use std::sync::Arc;

use serde::Serialize;
use reqwest::{header, Client, Response, StatusCode};

use error::{Error, Result};
use base::IntoRef;
use base::crypto::{Crypto, Key};
use trans::{Eid, Txid};
use volume::storage::Storage;
use volume::storage::space::{LocId, Space};
use super::sector::SectorMgr;
use super::emap::Emap;
use super::estore::{Estore, EstoreRef};
use super::http_client::{HttpClient, HttpClientRef};
use super::session::Session;

/// Zbox Storage
#[derive(Debug)]
pub struct ZboxStorage {
    // tx sequence number
    seq: u64,

    emap: Emap,

    // transaction sessions
    sessions: HashMap<Txid, Session>,

    // sector manager
    secmgr: SectorMgr,

    estore: EstoreRef,

    client: HttpClientRef,

    skey: Key, // storage encryption key
    crypto: Crypto,
}

impl ZboxStorage {
    pub fn new(repo_id: &str, access_key: &str) -> Result<Self> {
        let client = HttpClient::new(repo_id, access_key)?.into_ref();
        let estore = Estore::new(&client).into_ref();

        Ok(ZboxStorage {
            seq: 0,
            emap: Emap::new(Txid::from(0)),
            sessions: HashMap::new(),
            secmgr: SectorMgr::new(&estore),
            estore,
            client,
            skey: Key::new_empty(),
            crypto: Crypto::default(),
        })
    }
}

impl Storage for ZboxStorage {
    fn exists(&self, _location: &str) -> Result<bool> {
        let client = self.client.read().unwrap();
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
        self.client.read().unwrap().get("super")
    }

    fn put_super_blk(&mut self, super_blk: &[u8]) -> Result<()> {
        self.client.write().unwrap().put("super", super_blk)
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
        if !txid.is_empty() {
            let session =
                map_io_err!(self.sessions.get(&txid).ok_or(Error::NoTrans))?;
            if let Some(space) = session.emap().get(id) {
                return self.secmgr.read(buf, space, offset);
            }
        }
        match self.emap.get(id) {
            Some(space) => self.secmgr.read(buf, space, offset),
            None => Err(IoError::new(
                ErrorKind::NotFound,
                Error::NoEntity.description(),
            )),
        }
    }

    fn write(
        &mut self,
        id: &Eid,
        offset: u64,
        buf: &[u8],
        txid: Txid,
    ) -> IoResult<usize> {
        let session =
            map_io_err!(self.sessions.get_mut(&txid).ok_or(Error::NoTrans))?;
        let buf_len = buf.len();
        let mut space;
        let curr = match session.get(id) {
            Some(s) => Some(s.clone()),
            None => self.emap.get(id).map(|s| s.clone()),
        };

        match curr {
            Some(curr_space) => {
                // TODO
                space = session.alloc(buf_len);
            }
            None => {
                // new entity
                assert_eq!(offset, 0);
                space = session.alloc(buf_len);
            }
        }

        // write data to sector
        self.secmgr.write(buf, &space, offset)?;

        // update emap
        *session.entry(id.clone()).or_insert(space) = space.clone();

        Ok(0)
    }

    fn del(&mut self, id: &Eid, txid: Txid) -> Result<Option<Eid>> {
        Ok(None)
    }

    fn begin_trans(&mut self, txid: Txid) -> Result<()> {
        if self.sessions.contains_key(&txid) {
            return Err(Error::InTrans);
        }

        // create new session and change status to started
        let mut session = Session::new(
            self.seq,
            txid,
            &self.client,
            &self.skey,
            &self.crypto,
        );
        session.status_started()?;

        // increase tx sequence and add session to session list
        self.seq += 1;
        self.sessions.insert(txid, session);
        debug!("begin tx#{}", txid);

        Ok(())
    }

    fn abort_trans(&mut self, txid: Txid) -> Result<()> {
        debug!("abort tx#{}", txid);
        let status = {
            let session = self.sessions.get(&txid).ok_or(Error::NoTrans)?;
            assert!(!session.is_committing());
            session.status()
        };
        //self.cleanup(txid, status)?;
        debug!("tx#{} is aborted", txid);
        Ok(())
    }

    fn commit_trans(&mut self, txid: Txid) -> Result<()> {
        // all other transactions must be completed
        if self.sessions.values().any(|s| s.is_committing()) {
            return Err(Error::Uncompleted);
        }

        Ok(())
        /*match self.commit(txid) {
            Ok(_) => Ok(()),
            Err(err) => {
                self.rollback(txid)?;
                Err(err)
            }
        }*/
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
