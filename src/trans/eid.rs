use std::fmt::{self, Debug};
use std::ops::Index;

use bytes::{BufMut, LittleEndian};

use base::crypto::Crypto;
use super::txid::Txid;

/// Unique entity ID.
///
/// This represents a 32-byte randomly generated unique ID.
#[repr(C)]
#[derive(PartialEq, Eq, Hash, Default, Clone, Deserialize, Serialize)]
pub struct Eid([u8; Eid::EID_SIZE]);

impl Eid {
    /// Entity ID size
    pub(crate) const EID_SIZE: usize = 32;

    /// Create an empty entity ID
    #[inline]
    pub(crate) fn new_empty() -> Self {
        Eid::default()
    }

    /// Create a new random entity ID
    pub(crate) fn new() -> Self {
        let mut eid = Eid::new_empty();
        Crypto::random_buf(&mut eid.0);
        eid
    }

    pub(crate) fn from_slice(buf: &[u8]) -> Self {
        assert_eq!(buf.len(), Eid::EID_SIZE);
        let mut ret = Eid::new_empty();
        ret.0.copy_from_slice(buf);
        ret
    }
}

impl AsRef<[u8]> for Eid {
    #[inline]
    fn as_ref(&self) -> &[u8] {
        &self.0
    }
}

impl Index<usize> for Eid {
    type Output = u8;

    #[inline]
    fn index(&self, idx: usize) -> &u8 {
        &self.0[idx]
    }
}

impl Debug for Eid {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Eid({})", &self.to_string()[..6])
    }
}

impl ToString for Eid {
    fn to_string(&self) -> String {
        let strs: Vec<String> =
            self.0.iter().map(|b| format!("{:x}", b)).collect();
        strs.join("")
    }
}

/// Entity locator
#[derive(Clone, Deserialize, Serialize)]
pub struct Loc {
    pub eid: Eid,
    pub txid: Txid,
}

impl Loc {
    #[inline]
    pub fn new(id: &Eid, txid: Txid) -> Self {
        Loc {
            eid: id.clone(),
            txid,
        }
    }

    // derive locator id from entity id and txid
    pub fn id(&self) -> Eid {
        let mut buf = Vec::with_capacity(Eid::EID_SIZE + 8);
        buf.put(self.eid.as_ref());
        buf.put_u64::<LittleEndian>(self.txid.val());
        let hash = Crypto::hash(&buf);
        Eid::from_slice(&hash)
    }
}

impl Debug for Loc {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("TxLogs")
            .field("eid", &self.eid)
            .field("txid", &self.txid)
            .field("id", &self.id())
            .finish()
    }
}

/// Entity Id trait
pub trait Id {
    fn id(&self) -> &Eid;
    fn id_mut(&mut self) -> &mut Eid;
}

/// Clone to entity with new id
pub trait CloneNew: Clone + Id {
    fn clone_new(&self) -> Self {
        let mut ret = self.clone();
        *ret.id_mut() = Eid::new();
        ret
    }
}
