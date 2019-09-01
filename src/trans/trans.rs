use std::fmt::{self, Debug};
use std::sync::{Arc, RwLock};

use linked_hash_map::LinkedHashMap;

use super::wal::Wal;
use super::{Eid, EntityType, Id, Txid};
use base::IntoRef;
use error::{Error, Result};
use volume::{Arm, Armor, VolumeRef, VolumeWalArmor};

/// Cohort action in transaction
#[derive(Debug, Clone, Copy, Eq, PartialEq, Deserialize, Serialize)]
pub enum Action {
    New,
    Update,
    Delete,
}

/// Transable trait, be able to be added in transaction
pub trait Transable: Debug + Id + Send + Sync {
    fn action(&self) -> Action;
    fn commit(&mut self, vol: &VolumeRef) -> Result<()>;
    fn complete_commit(&mut self);
    fn abort(&mut self);
}

pub type TransableRef = Arc<RwLock<dyn Transable>>;

/// Transaction
pub struct Trans {
    txid: Txid,
    cohorts: LinkedHashMap<Eid, TransableRef>,
    wal: Wal,
    wal_armor: VolumeWalArmor<Wal>,
    wal_saved: bool,
}

impl Trans {
    pub fn new(txid: Txid, vol: &VolumeRef) -> Self {
        Trans {
            txid,
            cohorts: LinkedHashMap::new(),
            wal: Wal::new(txid),
            wal_armor: VolumeWalArmor::new(vol),
            wal_saved: false,
        }
    }

    #[inline]
    pub fn get_wal(&self) -> Wal {
        self.wal.clone()
    }

    #[inline]
    pub fn begin_trans(&mut self) -> Result<()> {
        self.wal_armor.save_item(&mut self.wal)
    }

    // add an entity to this transaction
    pub fn add_entity(
        &mut self,
        id: &Eid,
        entity: TransableRef,
        action: Action,
        ent_type: EntityType,
        arm: Arm,
    ) -> Result<()> {
        // add a wal entry
        self.wal.add_entry(id, action, ent_type, arm);
        self.wal_saved = false;

        // If the entity is a direct entity, such as Segment Data, we need to
        // save the wal now before writing to that entity. For the other types
        // of entities, the wal save is delayed before commit.
        if ent_type == EntityType::Direct {
            self.wal_armor.save_item(&mut self.wal).or_else(|err| {
                self.wal.remove_entry(id);
                Err(err)
            })?;
            self.wal_saved = true;
        }

        // add entity to cohorts
        self.cohorts.entry(id.clone()).or_insert(entity);

        Ok(())
    }

    /// Commit transaction
    pub fn commit(&mut self, vol: &VolumeRef) -> Result<Wal> {
        debug!("commit tx#{}, cohorts: {}", self.txid, self.cohorts.len());

        //dbg!(&self.cohorts);

        // save wal if it is not saved yet
        if !self.wal_saved {
            self.wal_armor.save_item(&mut self.wal)?;
            self.wal_saved = true;
        }

        let mut ent_in_use = Vec::new();

        // commit each entity
        for entity in self.cohorts.values() {
            let mut ent = entity.write().unwrap();

            // make sure deleted entity is not in use
            if ent.action() == Action::Delete {
                let using_cnt = Arc::strong_count(&entity);
                if using_cnt > 1 {
                    ent_in_use.push(ent.id().clone());
                }
            }

            // commit entity
            ent.commit(&vol)?;
        }

        // make sure all deleted entities are not used
        for id in ent_in_use {
            let entity = self.cohorts.get(&id).unwrap();
            let ent = entity.read().unwrap();
            let using_cnt = Arc::strong_count(&entity);
            if using_cnt > 1 {
                error!(
                    "deleted entity({:?}) still in use (using: {})",
                    ent.id(), using_cnt,
                );
                return Err(Error::InUse);
            }
        }

        Ok(self.wal.clone())
    }

    // complete commit
    pub fn complete_commit(&mut self) {
        for entity in self.cohorts.values() {
            let mut ent = entity.write().unwrap();
            ent.complete_commit();
        }
        self.cohorts.clear();
    }

    // abort transaction
    pub fn abort(&mut self, vol: &VolumeRef) -> Result<()> {
        // abort each entity
        for entity in self.cohorts.values() {
            let mut ent = entity.write().unwrap();
            ent.abort();
        }

        self.cohorts.clear();

        // clean aborted entities
        self.wal.clean_aborted(vol)?;

        // remove wal
        self.wal_armor.remove_all_arms(self.wal.id())
    }
}

impl IntoRef for Trans {}

impl Debug for Trans {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("Trans")
            .field("txid", &self.txid)
            .field("cohorts", &self.cohorts)
            .field("wal", &self.wal)
            .field("wal_saved", &self.wal_saved)
            .finish()
    }
}

/// Transaction reference type
pub type TransRef = Arc<RwLock<Trans>>;
