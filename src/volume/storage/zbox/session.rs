use std::fmt::{self, Display};
use std::path::{Path, PathBuf};
use std::collections::HashSet;
use std::collections::hash_map::Entry;

use error::Result;
use base::crypto::{Crypto, Key};
use base::utils::align_ceil;
use trans::{Eid, Txid};
use volume::storage::span::{Span, BLK_SIZE};
use volume::storage::space::Space;
use super::emap::Emap;
use super::http_client::HttpClientRef;

// transaction session status
#[derive(Debug, PartialEq, Clone, Copy)]
pub enum SessionStatus {
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
pub struct Session {
    seq: u64,
    txid: Txid,
    base_url: String,
    status: SessionStatus,
    wmark: u64,
    emap: Emap,
    deleted: HashSet<Eid>, // deleted entities
    recycle: Vec<Space>,
    client: HttpClientRef,
    skey: Key,
    crypto: Crypto,
}

impl Session {
    pub fn new(
        seq: u64,
        txid: Txid,
        client: &HttpClientRef,
        skey: &Key,
        crypto: &Crypto,
    ) -> Self {
        let base_url =
            format!("{}trans/{}", client.read().unwrap().base_url(), txid);
        let mut session = Session {
            seq,
            txid,
            base_url,
            status: SessionStatus::Init,
            wmark: 0,
            emap: Emap::new(txid),
            deleted: HashSet::new(),
            recycle: Vec::new(),
            client: client.clone(),
            skey: skey.clone(),
            crypto: crypto.clone(),
        };
        session.emap.set_crypto_key(crypto, skey);
        session
    }

    #[inline]
    pub fn emap(&self) -> &Emap {
        &self.emap
    }

    #[inline]
    pub fn get(&self, id: &Eid) -> Option<&Space> {
        self.emap.get(id)
    }

    #[inline]
    pub fn entry(&mut self, id: Eid) -> Entry<Eid, Space> {
        self.emap.entry(id)
    }

    // allocate space for entity
    pub fn alloc(&mut self, size: usize) -> Space {
        let blk_cnt = align_ceil(size, BLK_SIZE) / BLK_SIZE;
        let begin = self.wmark;
        self.wmark += blk_cnt as u64;
        let spans = Span::new(begin, self.wmark, 0).into_span_list(size);
        Space::new(self.txid, spans)
    }

    #[inline]
    pub fn is_committing(&self) -> bool {
        self.status == SessionStatus::Prepare
            || self.status == SessionStatus::Recycle
    }

    #[inline]
    pub fn status(&self) -> SessionStatus {
        self.status
    }

    // change session status
    fn switch_to_status(&mut self, to_status: SessionStatus) -> Result<()> {
        let mut client = self.client.write().unwrap();
        let url = format!("{}/{}", self.base_url, to_status);
        client.put(&url, &[])?;
        self.status = to_status;
        Ok(())
    }

    #[inline]
    pub fn status_started(&mut self) -> Result<()> {
        self.switch_to_status(SessionStatus::Started)
    }

    #[inline]
    pub fn status_prepare(&mut self) -> Result<()> {
        self.switch_to_status(SessionStatus::Prepare)
    }

    #[inline]
    pub fn status_recycle(&mut self) -> Result<()> {
        self.switch_to_status(SessionStatus::Recycle)
    }

    #[inline]
    pub fn status_committed(&mut self) -> Result<()> {
        self.switch_to_status(SessionStatus::Committed)
    }
}
