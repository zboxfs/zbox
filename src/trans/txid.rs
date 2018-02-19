use std::result::Result as StdResult;
use std::fmt::{self, Debug, Display, Formatter};
use std::cell::RefCell;

use serde::{Deserialize, Serialize};
use serde::ser::Serializer;
use serde::de::{self, Deserializer};

use error::{Error, Result};

thread_local!{
    // per-thread tranaction ID, with initial value 0
    static TXID: RefCell<u64> = RefCell::new(0);
}

/// Transaction ID, one per thread
#[derive(Hash, Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord)]
pub struct Txid(u64);

impl Txid {
    pub fn new_empty() -> Self {
        Self::default()
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.0 == 0
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
        // zero is not a valid transaction id
        if cur == 0 {
            return Err(Error::NotInTrans);
        }
        Ok(Txid(cur))
    }

    pub fn current_or_empty() -> Self {
        Txid::current().unwrap_or(Txid::new_empty())
    }

    pub fn reset_current() {
        TXID.with(|t| *t.borrow_mut() = 0);
    }

    pub fn next(&mut self) -> Txid {
        let (mut next, is_overflowed) = self.0.overflowing_add(1);
        if is_overflowed {
            next = 1;
        }
        self.0 = next;
        TXID.with(|t| *t.borrow_mut() = next);
        Txid(next)
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

struct TxidVisitor {}

impl TxidVisitor {
    fn new() -> Self {
        TxidVisitor {}
    }
}

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
        let visitor = TxidVisitor::new();
        deserializer.deserialize_u64(visitor)
    }
}
