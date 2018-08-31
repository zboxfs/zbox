use std::cell::RefCell;
use std::fmt::{self, Debug, Display, Formatter};
use std::result::Result as StdResult;
use std::u64;

use bytes::BufMut;
use serde::de::{self, Deserializer};
use serde::ser::Serializer;
use serde::{Deserialize, Serialize};

use super::Eid;
use base::crypto::{Crypto, HashKey};
use error::{Error, Result};

// per-thread txid
thread_local!{
    // per-thread tranaction ID, with initial value 0
    static TXID: RefCell<u64> = RefCell::new(0);
}

/// Transaction ID, one per thread
#[derive(Hash, Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord)]
pub struct Txid(u64);

impl Txid {
    #[inline]
    pub fn new_empty() -> Self {
        Self::default()
    }

    #[inline]
    pub fn val(&self) -> u64 {
        self.0
    }

    /// Check if thread is already in transaction
    pub fn is_in_trans() -> bool {
        let cur = TXID.with(|t| *t.borrow());
        cur != 0
    }

    /// Get current thread transaction ID
    pub fn current() -> Result<Self> {
        let cur = TXID.with(|t| *t.borrow());
        // zero is not treated as a valid transaction id
        if cur == 0 {
            return Err(Error::NotInTrans);
        }
        Ok(Txid(cur))
    }

    #[inline]
    pub fn current_or_empty() -> Self {
        Txid::current().unwrap_or(Txid::new_empty())
    }

    pub fn reset_current() {
        TXID.with(|t| *t.borrow_mut() = 0);
    }

    /// Get next txid by increase one
    pub fn next(&mut self) -> Txid {
        self.0 = self.0.checked_add(1).unwrap();
        TXID.with(|t| *t.borrow_mut() = self.0);
        Txid(self.0)
    }

    /// Derive an eid from txid
    pub fn derive_id(&self, hash_key: &HashKey) -> Eid {
        let mut buf = Vec::new();
        buf.put_u64_le(self.0);
        let hash = Crypto::hash_with_key(&buf, hash_key);
        Eid::from_slice(&hash)
    }
}

impl Debug for Txid {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Txid({})", self.0)
    }
}

impl Display for Txid {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl From<u64> for Txid {
    fn from(val: u64) -> Txid {
        Txid(val)
    }
}

impl Serialize for Txid {
    fn serialize<S>(&self, serializer: S) -> StdResult<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_u64(self.0)
    }
}

struct TxidVisitor;

impl<'de> de::Visitor<'de> for TxidVisitor {
    type Value = Txid;

    fn expecting(&self, formatter: &mut Formatter) -> fmt::Result {
        write!(formatter, "u64 integer")
    }

    fn visit_u64<E>(self, value: u64) -> StdResult<Self::Value, E>
    where
        E: de::Error,
    {
        Ok(Txid(value))
    }
}

impl<'de> Deserialize<'de> for Txid {
    fn deserialize<D>(deserializer: D) -> StdResult<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_u64(TxidVisitor)
    }
}
