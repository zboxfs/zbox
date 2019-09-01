use std::borrow::Borrow;
use std::fmt::{self, Debug};
use std::hash::Hash;
use std::marker::PhantomData;

use linked_hash_map::{Entries, LinkedHashMap};

pub trait Meter<T> {
    fn measure(&self, item: &T) -> isize;
}

pub trait Pinnable<T> {
    #[inline]
    fn is_pinned(&self, _: &T) -> bool {
        false
    }
}

/// LRU
#[derive(Clone, Default)]
pub struct Lru<K, V, M, P>
where
    K: Eq + Hash,
    M: Meter<V> + Default,
    P: Pinnable<V> + Default + Clone,
{
    capacity: usize,
    used: usize,
    map: LinkedHashMap<K, V>,
    meter: M,
    pin_ckr: P,
}

impl<K, V, M, P> Lru<K, V, M, P>
where
    K: Eq + Hash,
    M: Meter<V> + Default,
    P: Pinnable<V> + Default + Clone,
{
    pub fn new(capacity: usize) -> Self {
        Lru {
            capacity,
            used: 0,
            map: LinkedHashMap::new(),
            meter: M::default(),
            pin_ckr: P::default(),
        }
    }

    pub fn insert(&mut self, k: K, v: V) -> Option<V> {
        debug_assert!(self.capacity > 0);

        let mut delta: isize = self.meter.measure(&v);
        let mut ret: Option<V> = None;

        if let Some(old_val) = self.map.insert(k, v) {
            delta -= self.meter.measure(&old_val);
            ret = Some(old_val);
        }

        self.used = (self.used as isize + delta) as usize;
        if self.used > self.capacity {
            self.remove_lru();
        }

        ret
    }

    #[inline]
    pub fn contains_key<Q: ?Sized>(&self, k: &Q) -> bool
    where
        K: Borrow<Q>,
        Q: Eq + Hash,
    {
        self.map.contains_key(k)
    }

    #[inline]
    pub fn get_refresh<Q: ?Sized>(&mut self, k: &Q) -> Option<&mut V>
    where
        K: Borrow<Q>,
        Q: Eq + Hash,
    {
        self.map.get_refresh(k)
    }

    #[inline]
    pub fn entries(&mut self) -> Entries<K, V> {
        self.map.entries()
    }

    pub fn remove<Q: ?Sized>(&mut self, k: &Q) -> Option<V>
    where
        K: Borrow<Q>,
        Q: Eq + Hash,
    {
        self.map.remove(k).and_then(|v| {
            self.used = (self.used as isize - self.meter.measure(&v)) as usize;
            Some(v)
        })
    }

    fn remove_lru(&mut self) -> Option<V> {
        let pin_ckr = self.pin_ckr.clone();
        let ret = self
            .map
            .entries()
            .enumerate()
            .find(|&(_, ref ent)| !pin_ckr.is_pinned(ent.get()))
            .and_then(|(_, ent)| Some(ent.remove()));
        if let Some(ref v) = ret {
            self.used = (self.used as isize - self.meter.measure(v)) as usize;
        }
        ret
    }
}

impl<K, V, M, P> Debug for Lru<K, V, M, P>
where
    K: Debug + Eq + Hash,
    V: Debug,
    M: Meter<V> + Default,
    P: Pinnable<V> + Default + Clone,
{
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("Lru")
            .field("capacity", &self.capacity)
            .field("used", &self.used)
            .field("map", &self.map)
            .finish()
    }
}

/// Count meter, measured by object count
#[derive(Debug, Clone)]
pub struct CountMeter<T> {
    _marker: PhantomData<T>,
}

impl<T> Meter<T> for CountMeter<T> {
    #[inline]
    fn measure(&self, _: &T) -> isize {
        1
    }
}

impl<T> Default for CountMeter<T> {
    fn default() -> Self {
        CountMeter {
            _marker: PhantomData::<T>,
        }
    }
}

/// Default pin checker
#[derive(Debug, Clone)]
pub struct PinChecker<T> {
    _marker: PhantomData<T>,
}

impl<T> Pinnable<T> for PinChecker<T> {}

impl<T> Default for PinChecker<T> {
    fn default() -> Self {
        PinChecker {
            _marker: PhantomData::<T>,
        }
    }
}
