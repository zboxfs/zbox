use std::fmt::{self, Debug};
use std::sync::{Arc, RwLock, Weak};
use std::clone::Clone;
use std::default::Default;
use std::ops::Deref;

use error::{Error, Result};
use base::IntoRef;
use base::lru::{CountMeter, Lru, Pinnable};
use volume::{Persistable, VolumeRef};
use super::{CloneNew, Eid, Id, TxMgrRef, Txid};
use super::trans::{Action, Transable};

/// Cow switch
#[derive(Debug, Clone, Copy, Deserialize, Serialize)]
enum Switch {
    Left,
    Right,
}

impl Switch {
    #[inline]
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
    #[inline]
    fn default() -> Self {
        Switch::Left
    }
}

/// Cow slot
#[derive(Debug, Deserialize, Serialize)]
struct Slot<T: Id> {
    id: Eid,
    txid: Option<Txid>,

    #[serde(skip_serializing, skip_deserializing, default)] inner: Option<T>,
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

    #[serde(skip_serializing, skip_deserializing, default)] txid: Option<Txid>,

    #[serde(skip_serializing, skip_deserializing, default)]
    self_ref: CowWeakRef<T>,

    #[serde(skip_serializing, skip_deserializing, default)] txmgr: TxMgrRef,
}

impl<'d, T> Cow<T>
where
    T: Debug + Default + Send + Sync + CloneNew + Persistable<'d> + 'static,
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
    pub fn make_mut_naive(&mut self) -> &mut T {
        let curr_switch = self.switch;
        self.index_mut_by(curr_switch)
    }

    /// Mark cow as to be deleted
    pub fn make_del(&mut self) -> Result<()> {
        self.add_to_trans(Action::Delete)
    }

    // mutable index by switch
    fn index_mut_by(&mut self, switch: Switch) -> &mut T {
        match switch {
            Switch::Left => match self.left {
                Some(ref mut slot) => return slot.inner_ref_mut(),
                None => {}
            },
            Switch::Right => match self.right {
                Some(ref mut slot) => return slot.inner_ref_mut(),
                None => {}
            },
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
            Switch::Left => match self.left {
                Some(ref slot) => return slot,
                None => {}
            },
            Switch::Right => match self.right {
                Some(ref slot) => return slot,
                None => {}
            },
        }
        panic!("Cow slot is empty");
    }

    #[inline]
    fn slot(&self) -> &Slot<T> {
        self.slot_by(self.switch)
    }

    #[inline]
    fn other_slot(&self) -> &Slot<T> {
        self.slot_by(self.switch.other())
    }

    fn slot_mut_by(&mut self, switch: Switch) -> &mut Slot<T> {
        match switch {
            Switch::Left => match self.left {
                Some(ref mut slot) => return slot,
                None => {}
            },
            Switch::Right => match self.right {
                Some(ref mut slot) => return slot,
                None => {}
            },
        }
        panic!("Cow slot is empty");
    }

    fn slot_mut(&mut self) -> &mut Slot<T> {
        let switch = self.switch;
        self.slot_mut_by(switch)
    }

    pub fn load_cow(
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

    pub fn save_cow(&self, txid: Txid, vol: &VolumeRef) -> Result<()> {
        // save inner value
        T::save(self, txid, vol)?;

        // save cow
        <Cow<T> as Persistable>::save(self, txid, vol)
    }
}

impl<'d, T> Deref for Cow<T>
where
    T: Debug + Default + Send + Sync + CloneNew + Persistable<'d> + 'static,
{
    type Target = T;

    fn deref(&self) -> &T {
        let curr_txid = Txid::current_or_empty();
        if self.slot().txid == Some(curr_txid) || !self.has_other() {
            self.slot().inner_ref()
        } else {
            self.other_slot().inner_ref()
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
    #[inline]
    fn id(&self) -> &Eid {
        &self.id
    }

    #[inline]
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
    T: Debug + Default + Send + Sync + CloneNew + Persistable<'d> + 'static,
{
}

impl<'d, T> Transable for Cow<T>
where
    T: Debug + Default + Send + Sync + CloneNew + Persistable<'d> + 'static,
{
    fn commit(&mut self, action: Action, vol: &VolumeRef) -> Result<()> {
        let txid = self.txid.unwrap();

        match action {
            Action::New => self.save_cow(txid, vol),
            Action::Update => {
                // toggle switch and then save the new inner value
                self.switch.toggle();
                T::save(self, txid, vol)?;

                // remove old inner value
                T::remove(&self.other_slot().id, txid, vol)?;

                // save cow itself
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
    Self: Debug + Default + Send + Sync + CloneNew + Persistable<'de> + 'static,
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
    T: Debug + Default + Send + Sync + CloneNew,
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
    T: Debug + Default + Send + Sync + CloneNew + Persistable<'d> + 'static,
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
        let cow_ref = Cow::<T>::load_cow(id, &self.txmgr, vol)?;
        lru.insert(id.clone(), cow_ref.clone());
        Ok(cow_ref)
    }

    pub fn remove(&self, id: &Eid) -> Option<CowRef<T>> {
        let mut lru = self.lru.write().unwrap();
        lru.remove(id)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::{thread, time};
    use base::init_env;
    use base::crypto::{Cipher, Cost};
    use trans::{CloneNew, Eid, TxMgr, Txid};
    use volume::Volume;

    fn setup_vol() -> VolumeRef {
        init_env();
        let uri = "mem://test".to_string();
        let mut vol = Volume::new(&uri).unwrap();
        vol.init(Cost::default(), Cipher::Xchacha).unwrap();
        vol.into_ref()
    }

    #[derive(Debug, Default, Clone, Deserialize, Serialize)]
    struct Obj {
        id: Eid,
        val: u8,
    }

    impl Obj {
        fn new(val: u8) -> Self {
            Obj {
                id: Eid::new(),
                val,
            }
        }

        fn val(&self) -> u8 {
            self.val
        }
    }

    impl Id for Obj {
        fn id(&self) -> &Eid {
            &self.id
        }

        fn id_mut(&mut self) -> &mut Eid {
            &mut self.id
        }
    }

    impl CloneNew for Obj {}
    impl<'de> Persistable<'de> for Obj {}

    #[test]
    fn inner_obj_ref() {
        let vol = setup_vol();
        let txid = Txid::from(0);
        let txmgr = TxMgr::new(txid, &vol).into_ref();
        let val = 42;
        let obj = Obj::new(val);
        let obj2 = Obj::new(val);
        let children_cnt = 4;
        let cow_ref = Cow::new(obj, &txmgr).into_ref();
        {
            let mut c = cow_ref.write().unwrap();
            c.self_ref = Arc::downgrade(&cow_ref);
            c.slot_mut().txid = Some(txid);
        }
        let cow_ref2 = Cow::new(obj2, &txmgr).into_ref();

        let mut children = vec![];
        for i in 0..children_cnt {
            let txmgr = txmgr.clone();
            let cow_ref = cow_ref.clone();
            let cow_ref2 = cow_ref2.clone();
            children.push(thread::spawn(move || {
                if i == 0 {
                    // writer thread to update value
                    TxMgr::begin_trans(&txmgr).unwrap();
                    let mut cow = cow_ref.write().unwrap();
                    assert_eq!(cow.val(), val);
                    assert!(!cow.has_other());
                    {
                        let c = cow.make_mut().unwrap();
                        c.val += 1;
                    }
                    assert!(cow.has_other());
                    assert_eq!(cow.val(), val + 1);
                } else {
                    thread::sleep(time::Duration::from_millis(100));

                    // reader thread should still read old value
                    let cow = cow_ref.read().unwrap();
                    assert_eq!(cow.val(), val);

                    // read unchanged value
                    let cow2 = cow_ref2.read().unwrap();
                    assert_eq!(cow2.val(), val);
                }
            }));
        }
        for child in children {
            let _ = child.join();
        }
    }
}
