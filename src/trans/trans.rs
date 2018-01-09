use std::fmt::{self, Debug};
use std::sync::{Arc, RwLock};
use std::collections::HashMap;

use error::{Error, Result};
use base::IntoRef;
use volume::VolumeRef;
use super::{Eid, Id, Txid};

/// Cohort action in transaction
#[derive(Debug, Clone, Copy, PartialEq, Deserialize, Serialize)]
pub enum Action {
    New,
    Update,
    Delete,
}

/// Transable trait, be able to do transaction
pub trait Transable: Debug + Id {
    fn commit(&mut self, action: Action, vol: &VolumeRef) -> Result<()>;
    fn complete_commit(&mut self, action: Action);
    fn abort(&mut self, action: Action);
}

pub type TransableRef = Arc<RwLock<Transable + Send + Sync>>;

#[derive(Debug)]
struct Cohort {
    entity: TransableRef,
    action: Action,
}

/// Transaction
pub struct Trans {
    txid: Txid,
    cohorts: HashMap<Eid, Cohort>,
}

impl Trans {
    pub fn new(txid: Txid) -> Self {
        Trans {
            txid,
            cohorts: HashMap::new(),
        }
    }

    // add entity to transaction
    pub fn add_entity(
        &mut self,
        id: &Eid,
        entity: TransableRef,
        action: Action,
    ) {
        let cohort = self.cohorts.entry(id.clone()).or_insert(Cohort {
            entity: entity,
            action,
        });
        match action {
            Action::Update => {}
            _ => cohort.action = action,
        }
    }

    fn finish(&mut self) {
        self.cohorts.clear();
        Txid::reset_current();
    }

    // commit transaction
    pub fn commit(&mut self, vol: &VolumeRef) -> Result<()> {
        debug!(
            "commit tx#{}: entity_cnt: {}",
            self.txid,
            self.cohorts.len()
        );
        //debug!("trans.commit: cohorts: {:#?}", self.cohorts);

        // commit each entity
        for cohort in self.cohorts.values() {
            match cohort.action {
                Action::Delete => {
                    let using = Arc::strong_count(&cohort.entity);
                    if using > 1 {
                        error!(
                            "Cannot delete entity in use (using: ({}), \
                            cohort: {:?})",
                            using,
                            cohort
                        );
                        return Err(Error::InUse);
                    }
                }
                _ => {}
            }

            let mut ent = cohort.entity.write().unwrap();
            ent.commit(cohort.action, &vol)?;
        }

        Ok(())
    }

    // complete commit
    pub fn complete_commit(&mut self) {
        for cohort in self.cohorts.values() {
            let mut ent = cohort.entity.write().unwrap();
            ent.complete_commit(cohort.action);
        }
        self.finish();
    }

    // abort transaction
    pub fn abort(&mut self) {
        debug!("abort tx#{}", self.txid);

        // notify all cohorts in this transaction to abort
        for cohort in self.cohorts.values() {
            let mut ent = cohort.entity.write().unwrap();
            ent.abort(cohort.action);
        }

        self.finish();
    }
}

impl IntoRef for Trans {}

impl Debug for Trans {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("Trans")
            .field("txid", &self.txid)
            .field("cohorts", &self.cohorts)
            .finish()
    }
}

/// Transaction reference type
pub type TransRef = Arc<RwLock<Trans>>;
