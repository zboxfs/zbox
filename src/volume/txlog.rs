use std::collections::{HashMap, HashSet, VecDeque};
use std::slice::Iter;
use std::fmt::{self, Debug};

use bytes::{BufMut, LittleEndian};

use error::{Error, Result};
use base::crypto::{Crypto, CryptoCtx, HashKey, Key};
use trans::{Eid, Id, Loc, Txid};
use trans::trans::Action;
use super::armor::{Arm, Armor, Seq};
use super::storage::StorageRef;

// committed txlog list size
const COMMITTED_QUEUE_SIZE: usize = 2;

// tx log entry
#[derive(Debug, Deserialize, Serialize)]
pub struct Entry {
    id: Eid,
    action: Action,
    pre_txid: Txid,  // previous txid
    ppre_txid: Txid, // pre-previous txid
}

impl Entry {
    #[inline]
    pub fn id(&self) -> &Eid {
        &self.id
    }

    #[inline]
    fn pre_loc(&self) -> Loc {
        Loc::new(&self.id, self.pre_txid)
    }

    #[inline]
    fn ppre_loc(&self) -> Loc {
        Loc::new(&self.id, self.ppre_txid)
    }
}

// tx log
#[derive(Debug, Deserialize, Serialize)]
pub struct TxLog {
    id: Eid,
    arm: Arm,
    txid: Txid,
    entries: Vec<Entry>,
}

impl TxLog {
    fn new(txid: Txid, hash_key: &HashKey) -> Self {
        TxLog {
            id: Self::make_id(txid, hash_key),
            arm: Arm::default(),
            txid,
            entries: Vec::new(),
        }
    }

    // make txlog id from txid
    fn make_id(txid: Txid, hash_key: &HashKey) -> Eid {
        let mut buf = Vec::new();
        buf.put_u64::<LittleEndian>(txid.val());
        let hash = Crypto::hash_with_key(&buf, hash_key);
        Eid::from_slice(&hash)
    }

    #[inline]
    pub fn iter(&self) -> Iter<Entry> {
        self.entries.iter()
    }

    fn add_entry(
        &mut self,
        id: &Eid,
        action: Action,
        pre_txid: Txid,
        ppre_txid: Txid,
    ) {
        assert!(!self.entries.iter().any(|i| i.id == *id));
        let entry = Entry {
            id: id.clone(),
            action,
            pre_txid,
            ppre_txid,
        };
        self.entries.push(entry);
    }

    // abort a txlog
    fn abort(&self, storage: &StorageRef) -> Result<()> {
        let mut storage = storage.write().unwrap();
        for ent in &self.entries {
            match ent.action {
                Action::New | Action::Update => {
                    let loc_id = Loc::new(&ent.id, self.txid).id();
                    storage.del(&loc_id)?;
                }
                Action::Delete => {} // do nothing
            }
        }
        Ok(())
    }

    // recylce a txlog, return a list of removed entity ids
    fn recyle(&self, storage: &StorageRef) -> Result<Vec<Eid>> {
        let mut ret = Vec::new();
        let mut storage = storage.write().unwrap();

        for ent in &self.entries {
            match ent.action {
                Action::New => {} // do nothing
                Action::Update => {
                    // remove the oldest entity snapshot
                    if !ent.ppre_txid.is_empty() {
                        let loc_id = ent.ppre_loc().id();
                        storage.del(&loc_id)?;
                    }
                }
                Action::Delete => {
                    // remove all entity snapshots
                    if !ent.ppre_txid.is_empty() {
                        let loc_id = ent.ppre_loc().id();
                        storage.del(&loc_id)?;
                    }
                    if !ent.pre_txid.is_empty() {
                        let loc_id = ent.pre_loc().id();
                        storage.del(&loc_id)?;
                    }
                    ret.push(ent.id.clone());
                }
            }
        }

        Ok(ret)
    }
}

impl Id for TxLog {
    #[inline]
    fn id(&self) -> &Eid {
        &self.id
    }

    #[inline]
    fn id_mut(&mut self) -> &mut Eid {
        &mut self.id
    }
}

impl Seq for TxLog {
    #[inline]
    fn seq(&self) -> u64 {
        self.txid.val()
    }
}

impl<'de> Armor<'de> for TxLog {
    #[inline]
    fn arm(&self) -> Arm {
        self.arm
    }

    #[inline]
    fn arm_mut(&mut self) -> &mut Arm {
        &mut self.arm
    }
}

// tx log group
#[derive(Debug, Deserialize, Serialize)]
struct TxLogGrp {
    id: Eid,
    seq: u64,
    arm: Arm,
    done: VecDeque<TxLog>,
    doing: HashSet<Txid>,
}

impl TxLogGrp {
    fn new() -> Self {
        TxLogGrp {
            id: Eid::new_empty(),
            seq: 0,
            arm: Arm::default(),
            done: VecDeque::new(),
            doing: HashSet::new(),
        }
    }

    #[inline]
    fn save(
        &mut self,
        storage: &StorageRef,
        crypto_ctx: &CryptoCtx,
    ) -> Result<()> {
        <Self as Armor>::save(self, storage, crypto_ctx)?;
        self.seq += 1;
        Ok(())
    }
}

impl Id for TxLogGrp {
    #[inline]
    fn id(&self) -> &Eid {
        &self.id
    }

    #[inline]
    fn id_mut(&mut self) -> &mut Eid {
        &mut self.id
    }
}

impl Seq for TxLogGrp {
    #[inline]
    fn seq(&self) -> u64 {
        self.seq
    }
}

impl<'de> Armor<'de> for TxLogGrp {
    #[inline]
    fn arm(&self) -> Arm {
        self.arm
    }

    #[inline]
    fn arm_mut(&mut self) -> &mut Arm {
        &mut self.arm
    }
}

/// Tx log manager
pub struct TxLogMgr {
    logs: TxLogGrp,
    active: HashMap<Txid, TxLog>,
    inactive: HashMap<Txid, TxLog>,
    storage: StorageRef,
    crypto_ctx: CryptoCtx,
}

impl TxLogMgr {
    pub fn new(storage: StorageRef) -> Self {
        TxLogMgr {
            logs: TxLogGrp::new(),
            active: HashMap::new(),
            inactive: HashMap::new(),
            storage,
            crypto_ctx: CryptoCtx::default(),
        }
    }

    pub fn set_crypto_ctx(
        &mut self,
        crypto: &Crypto,
        key: &Key,
        subkey_id: u64,
    ) {
        let subkey = Crypto::derive_from_key(key, subkey_id).unwrap();
        self.crypto_ctx.set_with(crypto, &subkey);
    }

    #[inline]
    pub fn set_storage(&mut self, storage: &StorageRef) {
        self.storage = storage.clone();
    }

    #[inline]
    pub fn active_logs(&self) -> &HashMap<Txid, TxLog> {
        &self.active
    }

    #[inline]
    pub fn inactive_logs(&self) -> &HashMap<Txid, TxLog> {
        &self.inactive
    }

    fn txgrp_id(&self) -> Eid {
        let subkey =
            Crypto::derive_from_key(&self.crypto_ctx.hash_key, 0).unwrap();
        Eid::from_slice(&subkey.as_slice())
    }

    #[inline]
    pub fn init(&mut self) {
        *self.logs.id_mut() = self.txgrp_id();
    }

    // open txlog group, return the txid watermark
    pub fn open(&mut self) -> Result<Txid> {
        // load txlog group
        self.logs =
            TxLogGrp::load(&self.txgrp_id(), &self.storage, &self.crypto_ctx)?;

        // load all uncompleted txlog and add them to inactive list
        let uncompleted: Vec<Txid> =
            self.logs.doing.iter().map(|t| *t).collect();
        for txid in &uncompleted {
            // make txlog id
            let id = TxLog::make_id(*txid, &self.crypto_ctx.hash_key);

            // load uncompleted txlog
            match TxLog::load(&id, &self.storage, &self.crypto_ctx) {
                Ok(txlog) => {
                    // insert to inactive list to be aborted again
                    self.inactive.insert(*txid, txlog);
                }
                Err(ref err) if *err == Error::NotFound => {
                    // if the txlog doesn't exist, that means it has already
                    // been aborted, no need to do it again
                    self.logs.doing.remove(txid);
                }
                Err(err) => return Err(err),
            }
        }

        // get txid watermark
        let txid_wm = self.logs
            .done
            .iter()
            .map(|t| t.txid)
            .chain(self.logs.doing.iter().map(|t| *t))
            .max()
            .unwrap();

        Ok(txid_wm)
    }

    // start a new txlog
    pub fn start_log(&mut self, txid: Txid) -> Result<()> {
        assert!(!self.active.contains_key(&txid));
        let txlog = TxLog::new(txid, &self.crypto_ctx.hash_key);
        self.active.insert(txid, txlog);
        self.logs.doing.insert(txid);
        self.logs.save(&self.storage, &self.crypto_ctx)
    }

    // add an entry to specified txlog
    fn add_entry(
        &mut self,
        loc: &Loc,
        action: Action,
        pre_txid: Txid,
        ppre_txid: Txid,
    ) -> Result<()> {
        let txlog = self.active.get_mut(&loc.txid).unwrap();
        txlog.add_entry(&loc.eid, action, pre_txid, ppre_txid);
        txlog.save(&self.storage, &self.crypto_ctx)
    }

    #[inline]
    pub fn add_new_entry(&mut self, loc: &Loc) -> Result<()> {
        self.add_entry(loc, Action::New, Txid::new_empty(), Txid::new_empty())
    }

    #[inline]
    pub fn add_update_entry(
        &mut self,
        loc: &Loc,
        pre_txid: Txid,
        ppre_txid: Txid,
    ) -> Result<()> {
        self.add_entry(loc, Action::Update, pre_txid, ppre_txid)
    }

    #[inline]
    pub fn add_delete_entry(
        &mut self,
        loc: &Loc,
        pre_txid: Txid,
        ppre_txid: Txid,
    ) -> Result<()> {
        self.add_entry(loc, Action::Delete, pre_txid, ppre_txid)
    }

    // recycle trans and return a list of removed entity ids
    pub fn recycle(&mut self) -> Result<Vec<Eid>> {
        let mut ret = Vec::new();

        while self.logs.done.len() >= COMMITTED_QUEUE_SIZE {
            // recycle the oldest trans and remove the retired txlog
            {
                let retiree = self.logs.done.front().unwrap();
                ret = retiree.recyle(&self.storage)?;
                TxLog::remove_no_order(
                    retiree.id(),
                    &self.storage,
                    &self.crypto_ctx,
                )?;
            }

            self.logs.done.pop_front();
        }

        Ok(ret)
    }

    pub fn commit(&mut self, txid: Txid) -> Result<()> {
        let txlog = self.active.remove(&txid).unwrap();
        self.logs.done.push_back(txlog);
        self.logs.doing.remove(&txid);
        self.logs.save(&self.storage, &self.crypto_ctx)
    }

    pub fn deactivate(&mut self, txid: Txid) {
        if let Some(txlog) = self.active.remove(&txid) {
            self.inactive.insert(txid, txlog);
        }
        assert!(self.inactive.contains_key(&txid));
    }

    pub fn abort(&mut self, txid: Txid) -> Result<()> {
        {
            let txlog = self.inactive.get(&txid).unwrap();
            txlog.abort(&self.storage)?;
            TxLog::remove(txlog.id(), &self.storage, &self.crypto_ctx)?;
        }
        self.logs.doing.remove(&txid);
        self.logs.save(&self.storage, &self.crypto_ctx)?;
        self.inactive.remove(&txid);
        Ok(())
    }
}

impl Debug for TxLogMgr {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("TxLogMgr")
            .field("logs", &self.logs)
            .field("active", &self.active)
            .field("inactive", &self.inactive)
            .finish()
    }
}
