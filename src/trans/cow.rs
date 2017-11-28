use std::fmt::{self, Debug};
use std::sync::{Arc, RwLock, Weak};
use std::clone::Clone;
use std::default::Default;
use std::ops::Deref;

use serde::{Deserialize, Serialize};

use error::{Error, Result};
use base::IntoRef;
use base::lru::{Lru, CountMeter, Pinnable};
use volume::{VolumeRef, Persistable};
use super::{Eid, Id, CloneNew, Txid, TxMgrRef};
use super::trans::{Action, Transable};

/// Cow switch
#[derive(Debug, Clone, Copy, Deserialize, Serialize)]
enum Switch {
    Left,
    Right,
}

impl Switch {
    fn other(&self) -> Switch {
        match *self {
            Switch::Left => Switch::Right,
            Switch::Right => Switch::Left,
        }
    }

    #[inline]
    fn toggle(&mut self) {
        *self = self.other();
    }
}

impl Default for Switch {
    fn default() -> Self {
        Switch::Left
    }
}

/// Cow slot
#[derive(Debug, Deserialize, Serialize)]
struct Slot<T: Id> {
    id: Eid,
    txid: Option<Txid>,

    #[serde(skip_serializing, skip_deserializing, default)]
    inner: Option<T>,
}

impl<T: Id> Slot<T> {
    fn new(inner: T) -> Self {
        Slot {
            id: inner.id().clone(),
            txid: None,
            inner: Some(inner),
        }
    }

    fn inner_ref(&self) -> &T {
        match self.inner {
            Some(ref inner) => inner,
            None => panic!("Cow slot not loaded"),
        }
    }

    fn inner_ref_mut(&mut self) -> &mut T {
        match self.inner {
            Some(ref mut inner) => inner,
            None => panic!("Cow slot not loaded"),
        }
    }
}

/// Copy-on-write wrapper
#[derive(Default, Deserialize, Serialize)]
pub struct Cow<T>
where
    T: Debug + Default + Send + Sync + CloneNew,
{
    id: Eid,
    switch: Switch,
    left: Option<Slot<T>>,
    right: Option<Slot<T>>,

    #[serde(skip_serializing, skip_deserializing, default)]
    txid: Option<Txid>,

    #[serde(skip_serializing, skip_deserializing, default)]
    self_ref: CowWeakRef<T>,

    #[serde(skip_serializing, skip_deserializing, default)]
    txmgr: TxMgrRef,
}

impl<'d, T> Cow<T>
where
    T: Debug
        + Default
        + Send
        + Sync
        + CloneNew
        + Persistable<'d>
        + 'static,
{
    fn new(inner: T, txmgr: &TxMgrRef) -> Self {
        Cow {
            id: Eid::new(),
            switch: Switch::default(),
            left: Some(Slot::new(inner)),
            right: None,
            txid: None,
            self_ref: Weak::default(),
            txmgr: txmgr.clone(),
        }
    }

    /// Add self to transaction
    fn add_to_trans(&mut self, action: Action) -> Result<()> {
        let curr_txid = Txid::current()?;

        if let Some(txid) = self.txid {
            if txid != curr_txid {
                return Err(Error::InUse);
            }
        }

        // add self to transaction
        {
            let mut txmgr = self.txmgr.write().unwrap();
            let self_ref = self.self_ref.upgrade().unwrap();
            txmgr.add_to_trans(&self.id, curr_txid, self_ref, action)?;
        }

        Ok(match self.txid {
            Some(txid) => assert_eq!(txid, curr_txid),
            None => {
                self.txid = Some(curr_txid);
                if action == Action::New {
                    self.slot_mut().txid = self.txid;
                }
            }
        })
    }

    /// Get mutable reference for inner object by cloning it
    pub fn make_mut(&mut self) -> Result<&mut T> {
        self.add_to_trans(Action::Update)?;

        if self.slot().txid == self.txid {
            let switch = self.switch;
            return Ok(self.index_mut_by(switch));
        }

        if !self.has_other() {
            let new_val = T::clone_new(self);
            let mut slot = Slot::new(new_val);
            slot.txid = self.txid;
            *self.other_mut() = Some(slot);
        }

        assert_eq!(self.other_slot().txid, self.txid);
        let other = self.switch.other();
        Ok(self.index_mut_by(other))
    }

    /// Get mutable reference of inner object
    /// without adding to transaction
    pub fn make_mut_naive(&mut self) -> Result<&mut T> {
        if self.txid.is_some() {
            return Err(Error::InTrans);
        }
        let curr_switch = self.switch;
        Ok(self.index_mut_by(curr_switch))
    }

    /// Mark cow as to be deleted
    pub fn make_del(&mut self) -> Result<()> {
        self.add_to_trans(Action::Delete)
    }

    // mutable index by switch
    fn index_mut_by(&mut self, switch: Switch) -> &mut T {
        match switch {
            Switch::Left => {
                match self.left {
                    Some(ref mut slot) => return slot.inner_ref_mut(),
                    None => {}
                }
            }
            Switch::Right => {
                match self.right {
                    Some(ref mut slot) => return slot.inner_ref_mut(),
                    None => {}
                }
            }
        }
        panic!("Cow slot is empty");
    }

    fn has_other(&self) -> bool {
        match self.switch {
            Switch::Left => self.right.is_some(),
            Switch::Right => self.left.is_some(),
        }
    }

    fn other_mut(&mut self) -> &mut Option<Slot<T>> {
        match self.switch {
            Switch::Left => &mut self.right,
            Switch::Right => &mut self.left,
        }
    }

    fn slot_by(&self, switch: Switch) -> &Slot<T> {
        match switch {
            Switch::Left => {
                match self.left {
                    Some(ref slot) => return slot,
                    None => {}
                }
            }
            Switch::Right => {
                match self.right {
                    Some(ref slot) => return slot,
                    None => {}
                }
            }
        }
        panic!("Cow slot is empty");
    }

    fn slot(&self) -> &Slot<T> {
        self.slot_by(self.switch)
    }

    fn other_slot(&self) -> &Slot<T> {
        self.slot_by(self.switch.other())
    }

    fn slot_mut_by(&mut self, switch: Switch) -> &mut Slot<T> {
        match switch {
            Switch::Left => {
                match self.left {
                    Some(ref mut slot) => return slot,
                    None => {}
                }
            }
            Switch::Right => {
                match self.right {
                    Some(ref mut slot) => return slot,
                    None => {}
                }
            }
        }
        panic!("Cow slot is empty");
    }

    fn slot_mut(&mut self) -> &mut Slot<T> {
        let switch = self.switch;
        self.slot_mut_by(switch)
    }

    pub fn load(
        id: &Eid,
        txmgr: &TxMgrRef,
        vol: &VolumeRef,
    ) -> Result<CowRef<T>> {
        let txid = Txid::current_or_empty();

        // load cow
        let mut cow = <Cow<T> as Persistable>::load(id, txid, vol)?;
        assert!(!cow.has_other());
        {
            // load inner value
            let slot = cow.slot_mut();
            let inner = T::load(&slot.id, txid, vol)?;
            slot.inner = Some(inner);
        }

        let cow_ref = cow.into_ref();
        {
            let mut c = cow_ref.write().unwrap();
            c.self_ref = Arc::downgrade(&cow_ref);
            c.txmgr = txmgr.clone();
        }

        Ok(cow_ref)
    }

    pub fn save(&self, txid: Txid, vol: &VolumeRef) -> Result<()> {
        // save inner value
        T::save(self, txid, vol)?;

        // save cow
        <Cow<T> as Persistable>::save(self, txid, vol)
    }
}

impl<'d, T> Deref for Cow<T>
where
    T: Debug
        + Default
        + Send
        + Sync
        + Deserialize<'d>
        + CloneNew
        + Serialize,
{
    type Target = T;

    fn deref(&self) -> &T {
        match self.switch {
            Switch::Left => {
                match self.left {
                    Some(ref slot) => return slot.inner_ref(),
                    None => unreachable!(),
                }
            }
            Switch::Right => {
                match self.right {
                    Some(ref slot) => return slot.inner_ref(),
                    None => unreachable!(),
                }
            }
        }
    }
}

impl<T> Debug for Cow<T>
where
    T: Debug + Default + Send + Sync + CloneNew,
{
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("Cow")
            .field("id", &self.id)
            .field("txid", &self.txid)
            .field("switch", &self.switch)
            .field("left", &self.left)
            .field("right", &self.right)
            .finish()
    }
}

impl<T> Id for Cow<T>
where
    T: Debug + Default + Send + Sync + CloneNew,
{
    fn id(&self) -> &Eid {
        &self.id
    }

    fn id_mut(&mut self) -> &mut Eid {
        &mut self.id
    }
}

impl<T> IntoRef for Cow<T>
where
    T: Debug + Default + Send + Sync + CloneNew,
{
}

impl<'d, T> Persistable<'d> for Cow<T>
where
    T: Debug
        + Default
        + Send
        + Sync
        + CloneNew
        + Persistable<'d>
        + 'static,
{
}

impl<'d, T> Transable for Cow<T>
where
    T: Debug
        + Default
        + Send
        + Sync
        + CloneNew
        + Persistable<'d>
        + 'static,
{
    fn commit(&mut self, action: Action, vol: &VolumeRef) -> Result<()> {
        let txid = self.txid.unwrap();

        match action {
            Action::New => self.save(txid, vol),
            Action::Update => {
                // toggle switch and then save the new inner value
                self.switch.toggle();
                T::save(self, txid, vol)?;

                // remove old inner value
                T::remove(&self.other_slot().id, txid, vol)?;

                // save cow
                let other_bk = self.other_mut().take();
                let result = <Cow<T> as Persistable>::save(self, txid, vol);
                *self.other_mut() = other_bk;
                result
            }
            Action::Delete => {
                if self.has_other() {
                    T::remove(&self.other_slot().id, txid, vol)?;
                }
                T::remove(&self.slot().id, txid, vol)?;
                Cow::<T>::remove(&self.id, txid, vol)?;
                Ok(())
            }
        }
    }

    fn complete_commit(&mut self, action: Action) {
        match action {
            Action::New | Action::Delete => {}
            Action::Update => {
                self.other_mut().take();
            }
        }
        self.txid = None;
    }

    fn abort(&mut self, action: Action) {
        match action {
            Action::New => {}
            Action::Update => {
                if self.slot().txid == self.txid {
                    // toggle switch back to old inner value
                    self.switch.toggle();
                }
                self.other_mut().take();
            }
            Action::Delete => {
                if self.has_other() {
                    self.other_mut().take();
                }
            }
        }
        self.txid = None;
    }
}

/// Cow reference type
pub type CowRef<T> = Arc<RwLock<Cow<T>>>;

/// Cow weak reference type
pub type CowWeakRef<T> = Weak<RwLock<Cow<T>>>;

/// Wrap value into Cow reference
pub trait IntoCow<'de>
where
    Self: Debug
        + Default
        + Send
        + Sync
        + CloneNew
        + Persistable<'de>
        + 'static,
{
    fn into_cow(self, txmgr: &TxMgrRef) -> Result<CowRef<Self>> {
        let cow_ref = Cow::new(self, txmgr).into_ref();
        {
            let mut cow = cow_ref.write().unwrap();
            cow.self_ref = Arc::downgrade(&cow_ref);
            cow.add_to_trans(Action::New)?;
        }
        Ok(cow_ref)
    }
}

/// Cow cache pin checker
#[derive(Debug, Clone, Default)]
pub struct CowPinChecker {}

impl<T> Pinnable<CowRef<T>> for CowPinChecker
where
    T: Debug
        + Default
        + Send
        + Sync
        + CloneNew,
{
    fn is_pinned(&self, item: &CowRef<T>) -> bool {
        // if cannot read the inner cow entity, we assume it is pinned
        match item.try_read() {
            Ok(cow) => cow.txid.is_some(),
            Err(_) => true,
        }
    }
}

/// Cow LRU cache
#[derive(Debug, Clone, Default)]
pub struct CowCache<T: Debug + Default + Send + Sync + CloneNew> {
    lru: Arc<RwLock<Lru<Eid, CowRef<T>, CountMeter<CowRef<T>>, CowPinChecker>>>,
    txmgr: TxMgrRef,
}

impl<'d, T> CowCache<T>
where
    T: Debug
        + Default
        + Send
        + Sync
        + CloneNew
        + Persistable<'d>
        + 'static,
{
    pub fn new(capacity: usize, txmgr: &TxMgrRef) -> Self {
        CowCache {
            lru: Arc::new(RwLock::new(Lru::new(capacity))),
            txmgr: txmgr.clone(),
        }
    }

    pub fn get(&self, id: &Eid, vol: &VolumeRef) -> Result<CowRef<T>> {
        let mut lru = self.lru.write().unwrap();

        // get from cache first
        if let Some(val) = lru.get_refresh(id) {
            return Ok(val.clone());
        }

        // if not in cache, load it from volume
        // then insert into cache
        let cow_ref = Cow::<T>::load(id, &self.txmgr, vol)?;
        lru.insert(id.clone(), cow_ref.clone());
        Ok(cow_ref)
    }

    pub fn remove(&self, id: &Eid) -> Option<CowRef<T>> {
        let mut lru = self.lru.write().unwrap();
        lru.remove(id)
    }
}
