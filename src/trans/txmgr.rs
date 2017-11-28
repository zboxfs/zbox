use std::sync::{Arc, RwLock};
use std::collections::HashMap;
use std::fmt::{self, Debug};

use linked_hash_map::LinkedHashMap;

use error::{Error, Result};
use base::IntoRef;
use volume::VolumeRef;
use super::{Eid, Id, Txid};
use super::trans::{Trans, TransRef, Action, TransableRef};

/// Tranaction manager
#[derive(Default)]
pub struct TxMgr {
    id: Eid,
    txid_src: Txid,

    // transaction list
    txs: LinkedHashMap<Txid, TransRef>,

    // entity map
    ents: HashMap<Eid, Txid>,

    vol: VolumeRef,
}

impl TxMgr {
    pub fn new(txid_src: Txid, vol: &VolumeRef) -> Self {
        TxMgr {
            id: Eid::new(),
            txid_src,
            txs: LinkedHashMap::new(),
            ents: HashMap::new(),
            vol: vol.clone(),
        }
    }

    /// Begin a transaction
    pub fn begin_trans(txmgr: &TxMgrRef) -> Result<TxHandle> {
        // check if current thread is already in transaction
        if Txid::is_in_trans() {
            return Err(Error::InTrans);
        }

        // create new txid and tx
        let mut tm = txmgr.write().unwrap();
        let txid = tm.txid_src.next();
        let tx = Trans::new(txid).into_ref();
        tm.txs.insert(txid, tx);

        // begin volume transaction
        let mut vol = tm.vol.write().unwrap();
        vol.begin_trans(txid)?;

        Ok(TxHandle {
            txid,
            txmgr: txmgr.clone(),
        })
    }

    /// Add entity to transaction
    pub fn add_to_trans(
        &mut self,
        id: &Eid,
        txid: Txid,
        entity: TransableRef,
        action: Action,
    ) -> Result<()> {
        let cur_txid = self.ents.entry(id.clone()).or_insert(txid);
        if *cur_txid != txid {
            // entity is already in other transaction
            return Err(Error::InTrans);
        }

        // get tx and add entity to tx
        let txref = self.txs.get(&txid).ok_or(Error::NoTrans)?;
        let mut tx = txref.write().unwrap();
        tx.add_entity(id, entity, action);

        Ok(())
    }

    fn remove_trans(&mut self, txid: Txid) {
        self.txs.remove(&txid);
        self.ents.retain(|_, &mut v| v != txid);
    }
}

impl Id for TxMgr {
    fn id(&self) -> &Eid {
        &self.id
    }

    fn id_mut(&mut self) -> &mut Eid {
        &mut self.id
    }
}

impl Debug for TxMgr {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("TxMgr")
            .field("id", &self.id)
            .field("txid_src", &self.txid_src)
            .field("txs", &self.txs)
            .field("ents", &self.ents)
            .finish()
    }
}

impl IntoRef for TxMgr {}

/// TxMgr reference type
pub type TxMgrRef = Arc<RwLock<TxMgr>>;

// Transaction handle
#[derive(Debug, Default, Clone)]
pub struct TxHandle {
    pub txid: Txid,
    pub txmgr: TxMgrRef,
}

impl TxHandle {
    /// Run operations in transaction and continue
    pub fn run<F>(&self, oper: F) -> Result<()>
    where
        F: FnOnce() -> Result<()>,
    {
        match oper() {
            Ok(_) => Ok(()),
            Err(err) => self.abort(err),
        }
    }

    /// Run operations in transaction and commit
    pub fn run_all<F>(&self, oper: F) -> Result<()>
    where
        F: FnOnce() -> Result<()>,
    {
        match oper() {
            Ok(_) => self.commit(),
            Err(err) => self.abort(err),
        }
    }

    fn abort(&self, err: Error) -> Result<()> {
        let mut tm = self.txmgr.write().unwrap();

        let ret = {
            let tx_ref = tm.txs.get(&self.txid).unwrap();
            let mut tx = tx_ref.write().unwrap();
            self.abort_trans(&mut tx, &tm.vol).and_then(|_| Err(err))
        };

        // remove tx from tx manager regardless abort result
        tm.remove_trans(self.txid);

        ret
    }

    /// Commit transaction
    pub fn commit(&self) -> Result<()> {
        let ret = self.commit_trans();

        // remove tx from tx manager regardless commit result
        let mut tm = self.txmgr.write().unwrap();
        tm.remove_trans(self.txid);

        ret
    }

    fn commit_trans(&self) -> Result<()> {
        let tm = self.txmgr.read().unwrap();
        let tx_ref = tm.txs.get(&self.txid).ok_or(Error::NoTrans)?;
        let mut tx = tx_ref.write().unwrap();

        match tx.commit(&tm.vol) {
            Ok(_) => {
                // volume commit
                let mut vol = tm.vol.write().unwrap();
                vol.commit_trans(self.txid)?;

                // notify tx is completed
                tx.complete_commit();

                Ok(())
            }
            Err(err) => {
                self.abort_trans(&mut tx, &tm.vol)?;
                Err(err)
            }
        }
    }

    fn abort_trans(&self, tx: &mut Trans, vol: &VolumeRef) -> Result<()> {
        tx.abort();
        let mut vol = vol.write().unwrap();
        vol.abort_trans(self.txid)
    }
}
