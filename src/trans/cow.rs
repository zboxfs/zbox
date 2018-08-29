use std::clone::Clone;
use std::default::Default;
use std::fmt::{self, Debug};
use std::ops::Deref;
use std::sync::{Arc, RwLock, Weak};

use serde::{Deserialize, Serialize};

use super::trans::{Action, Transable};
use super::{Eid, EntityType, Id, TxMgrRef, Txid};
use base::lru::{CountMeter, Lru, Pinnable};
use base::IntoRef;
use error::{Error, Result};
use volume::{Arm, ArmAccess, Armor, Seq, VolumeArmor, VolumeRef};

/// Trait for entity can be wrapped in cow
pub trait Cowable: Debug + Default + Clone + Send + Sync {}

/// Copy-on-write wrapper
#[derive(Default, Deserialize, Serialize)]
pub struct Cow<T: Cowable> {
    id: Eid,
    seq: u64,
    arm: Arm,
    left: Option<T>,
    right: Option<T>,

    #[serde(skip_serializing, skip_deserializing, default)]
    txid: Option<Txid>,
    #[serde(skip_serializing, skip_deserializing, default)]
    action: Option<Action>,

    #[serde(skip_serializing, skip_deserializing, default)]
    self_ref: CowWeakRef<T>,

    #[serde(skip_serializing, skip_deserializing, default)]
    txmgr: TxMgrRef,
}

impl<'de, T> Cow<T>
where
    T: Cowable + Deserialize<'de> + Serialize + 'static,
{
    fn new(id: &Eid, inner: T, txmgr: &TxMgrRef) -> Self {
        let arm = Arm::default().other();
        let mut left = None;
        let mut right = None;
        match arm {
            Arm::Left => left = Some(inner),
            Arm::Right => right = Some(inner),
        }

        Cow {
            id: id.clone(),
            seq: 0,
            arm,
            left,
            right,
            txid: None,
            action: None,
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

            // deal with action ordering
            if let Some(curr_action) = self.action {
                match curr_action {
                    Action::New => match action {
                        // if the action is new first and then update,
                        // we still treat it as new
                        Action::New | Action::Update => return Ok(()),
                        _ => {}
                    },
                    Action::Update => match action {
                        Action::New => unreachable!(), // wrong action order
                        Action::Update => return Ok(()),
                        _ => {}
                    },
                    Action::Delete => match action {
                        Action::Delete => return Ok(()),
                        _ => unreachable!(), // wrong action order
                    },
                }
            }
        }

        // add cow to transaction
        {
            let mut txmgr = self.txmgr.write().unwrap();
            let self_ref = self.self_ref.upgrade().unwrap();
            let arm = if action == Action::New {
                self.arm
            } else {
                self.arm.other()
            };
            txmgr.add_to_trans(
                &self.id,
                curr_txid,
                self_ref,
                action,
                EntityType::Cow,
                arm,
            )?;
        }

        // set txid and action for this cow
        self.txid = Some(curr_txid);
        self.action = Some(action);

        Ok(())
    }

    /// Get mutable reference for inner object by cloning it
    pub fn make_mut(&mut self) -> Result<&mut T> {
        // if cow is a newly created, use it directly
        if self.action == Some(Action::New) {
            return Ok(self.inner_mut());
        }

        // copy inner if it is not copied yet
        if !self.has_other() {
            let new_inner = T::clone(self.inner());
            *self.other_mut() = Some(new_inner);
        }

        self.add_to_trans(Action::Update)?;

        Ok(self.other_inner_mut())
    }

    /// Get mutable reference of inner object without adding the cow to
    /// transaction
    #[inline]
    pub fn make_mut_naive(&mut self) -> &mut T {
        self.inner_mut()
    }

    /// Mark cow as deleted
    #[inline]
    pub fn make_del(&mut self) -> Result<()> {
        self.add_to_trans(Action::Delete)
    }

    #[inline]
    fn has_other(&self) -> bool {
        match self.arm {
            Arm::Left => self.right.is_some(),
            Arm::Right => self.left.is_some(),
        }
    }

    #[inline]
    fn curr_mut(&mut self) -> &mut Option<T> {
        match self.arm {
            Arm::Left => &mut self.left,
            Arm::Right => &mut self.right,
        }
    }

    #[inline]
    fn other_mut(&mut self) -> &mut Option<T> {
        match self.arm {
            Arm::Left => &mut self.right,
            Arm::Right => &mut self.left,
        }
    }

    fn inner_by(&self, arm: Arm) -> &T {
        match arm {
            Arm::Left => match self.left {
                Some(ref inner) => return inner,
                None => {}
            },
            Arm::Right => match self.right {
                Some(ref inner) => return inner,
                None => {}
            },
        }
        panic!("Cow is empty");
    }

    fn inner_mut_by(&mut self, arm: Arm) -> &mut T {
        match arm {
            Arm::Left => match self.left {
                Some(ref mut inner) => return inner,
                None => {}
            },
            Arm::Right => match self.right {
                Some(ref mut inner) => return inner,
                None => {}
            },
        }
        panic!("Cow is empty");
    }

    #[inline]
    fn inner(&self) -> &T {
        self.inner_by(self.arm)
    }

    #[inline]
    fn inner_mut(&mut self) -> &mut T {
        let arm = self.arm;
        self.inner_mut_by(arm)
    }

    #[inline]
    fn other_inner(&self) -> &T {
        self.inner_by(self.arm.other())
    }

    #[inline]
    fn other_inner_mut(&mut self) -> &mut T {
        let arm = self.arm.other();
        self.inner_mut_by(arm)
    }

    // load cow from volume
    pub fn load(
        id: &Eid,
        txmgr: &TxMgrRef,
        vol: &VolumeRef,
    ) -> Result<CowRef<T>> {
        let vol_armor = VolumeArmor::<Cow<T>>::new(vol);
        let cow = vol_armor.load_item(id)?;
        let cow_ref = cow.into_ref();
        {
            let mut c = cow_ref.write().unwrap();
            c.self_ref = Arc::downgrade(&cow_ref);
            c.txmgr = txmgr.clone();
        }
        Ok(cow_ref)
    }

    // save cow to volume
    #[inline]
    pub fn save(&mut self, vol: &VolumeRef) -> Result<()> {
        let vol_armor = VolumeArmor::<Cow<T>>::new(vol);
        vol_armor.save_item(self)
    }
}

impl<'de, T> Deref for Cow<T>
where
    T: Cowable + Deserialize<'de> + Serialize + 'static,
{
    type Target = T;

    fn deref(&self) -> &T {
        let curr_txid = Txid::current_or_empty();
        if self.txid.is_none()
            || self.txid != Some(curr_txid)
            || self.action == Some(Action::New)
        {
            self.inner()
        } else {
            self.other_inner()
        }
    }
}

impl<T> Debug for Cow<T>
where
    T: Cowable,
{
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("Cow")
            .field("id", &self.id)
            .field("seq", &self.seq)
            .field("arm", &self.arm)
            .field("txid", &self.txid)
            .field("action", &self.action)
            .field("left", &self.left)
            .field("right", &self.right)
            .finish()
    }
}

impl<T> Id for Cow<T>
where
    T: Cowable,
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

impl<T> Seq for Cow<T>
where
    T: Cowable,
{
    #[inline]
    fn seq(&self) -> u64 {
        self.seq
    }

    #[inline]
    fn inc_seq(&mut self) {
        self.seq += 1
    }
}

impl<'de, T> ArmAccess<'de> for Cow<T>
where
    T: Cowable + Deserialize<'de> + Serialize,
{
    #[inline]
    fn arm(&self) -> Arm {
        self.arm
    }

    #[inline]
    fn arm_mut(&mut self) -> &mut Arm {
        &mut self.arm
    }
}

impl<T> IntoRef for Cow<T> where T: Cowable {}

impl<'de, T> Transable for Cow<T>
where
    T: Cowable + Deserialize<'de> + Serialize + 'static,
{
    #[inline]
    fn action(&self) -> Action {
        self.action.clone().unwrap()
    }

    fn commit(&mut self, vol: &VolumeRef) -> Result<()> {
        match self.action {
            Some(action) => match action {
                Action::New => {
                    // toggle arm temporarily because save() will toggle it
                    self.arm.toggle();

                    self.save(vol).or_else(|err| {
                        // if saving cow failed, arm will not be switched,
                        // so we need to switch it here
                        self.arm.toggle();
                        Err(err)
                    })
                }
                Action::Update => {
                    // save old inner object first
                    let old = self.curr_mut().take();

                    // save cow and restore the old inner object
                    let result = self.save(vol).and_then(|_| {
                        // toggle the arm back because save() has
                        // already toggled it
                        self.arm.toggle();
                        Ok(())
                    });

                    // restore the old inner object
                    *self.curr_mut() = old;

                    result
                }
                Action::Delete => {
                    // do nothing here, actual deletion will be delayed
                    // after 2 txs
                    Ok(())
                }
            },
            None => unreachable!(),
        }
    }

    fn complete_commit(&mut self) {
        match self.action {
            Some(action) => match action {
                Action::Update => {
                    // toggle arm and discard the old inner object
                    self.arm.toggle();
                    self.other_mut().take();
                }
                _ => {}
            },
            None => unreachable!(),
        }
        self.txid = None;
        self.action = None;
    }

    fn abort(&mut self) {
        match self.action {
            Some(action) => match action {
                Action::Update => {
                    // discard the new inner object
                    self.other_mut().take();
                }
                _ => {}
            },
            None => unreachable!(),
        }
        self.txid = None;
        self.action = None;
    }
}

/// Cow reference type
pub type CowRef<T> = Arc<RwLock<Cow<T>>>;

/// Cow weak reference type
pub type CowWeakRef<T> = Weak<RwLock<Cow<T>>>;

/// Wrap value into Cow reference
pub trait IntoCow<'de>
where
    Self: Cowable + Deserialize<'de> + Serialize + 'static,
{
    fn into_cow_with_id(
        self,
        id: &Eid,
        txmgr: &TxMgrRef,
    ) -> Result<CowRef<Self>> {
        let cow_ref = Cow::new(id, self, txmgr).into_ref();
        {
            let mut cow = cow_ref.write().unwrap();
            cow.self_ref = Arc::downgrade(&cow_ref);
            cow.add_to_trans(Action::New)?;
        }
        Ok(cow_ref)
    }

    #[inline]
    fn into_cow(self, txmgr: &TxMgrRef) -> Result<CowRef<Self>> {
        let id = Eid::new();
        Self::into_cow_with_id(self, &id, txmgr)
    }
}

/// Cow cache pin checker
#[derive(Debug, Clone, Default)]
pub struct CowPinChecker {}

impl<T> Pinnable<CowRef<T>> for CowPinChecker
where
    T: Cowable,
{
    fn is_pinned(&self, item: &CowRef<T>) -> bool {
        // cow in transaction must be kept in cache,
        // if cannot read the inner cow entity, we assume it is pinned
        match item.try_read() {
            Ok(cow) => cow.txid.is_some(),
            Err(_) => true,
        }
    }
}

/// Cow LRU cache
#[derive(Debug, Clone, Default)]
pub struct CowCache<T: Cowable> {
    lru: Arc<RwLock<Lru<Eid, CowRef<T>, CountMeter<CowRef<T>>, CowPinChecker>>>,
    txmgr: TxMgrRef,
}

impl<'de, T> CowCache<T>
where
    T: Cowable + Deserialize<'de> + Serialize + 'static,
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

    pub fn insert(&self, cow: &CowRef<T>) {
        let mut lru = self.lru.write().unwrap();
        let id = {
            let cow = cow.read().unwrap();
            cow.id.clone()
        };
        lru.insert(id, cow.clone());
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
    use fs::Config;
    use trans::{Eid, TxMgr};
    use volume::Volume;

    fn setup_vol() -> VolumeRef {
        init_env();
        let uri = "mem://foo".to_string();
        let mut vol = Volume::new(&uri).unwrap();
        vol.init("pwd", &Config::default(), &Vec::new()).unwrap();
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
    }

    impl Cowable for Obj {}

    #[test]
    fn inner_obj_ref() {
        let vol = setup_vol();
        let txmgr = TxMgr::new(&Eid::new(), &vol).into_ref();
        let val = 42;
        let obj = Obj::new(val);
        let obj2 = Obj::new(val);
        let threads_cnt = 4;
        let cow_ref = Cow::new(&Eid::new(), obj, &txmgr).into_ref();
        {
            let mut c = cow_ref.write().unwrap();
            c.self_ref = Arc::downgrade(&cow_ref);
        }
        let cow_ref2 = Cow::new(&Eid::new(), obj2, &txmgr).into_ref();

        let mut threads = vec![];
        for i in 0..threads_cnt {
            let txmgr = txmgr.clone();
            let cow_ref = cow_ref.clone();
            let cow_ref2 = cow_ref2.clone();
            threads.push(thread::spawn(move || {
                if i == 0 {
                    // writer thread to update value
                    let _txhandle = TxMgr::begin_trans(&txmgr).unwrap();
                    let mut cow = cow_ref.write().unwrap();
                    assert_eq!(cow.val, val);
                    assert!(!cow.has_other());
                    {
                        let c = cow.make_mut().unwrap();
                        c.val += 1;
                    }
                    assert!(cow.has_other());
                    assert_eq!(cow.val, val + 1);
                } else {
                    thread::sleep(time::Duration::from_millis(100));

                    // reader thread should still read old value
                    let cow = cow_ref.read().unwrap();
                    assert_eq!(cow.val, val);

                    // read unchanged value
                    let cow2 = cow_ref2.read().unwrap();
                    assert_eq!(cow2.val, val);
                }
            }));
        }

        for t in threads {
            let _ = t.join();
        }
    }
}
