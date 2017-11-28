use std::fmt::{self, Debug};
use std::path::PathBuf;
use std::ops::Index;

use base::crypto::Crypto;
use super::txid::Txid;

/// Entity ID
#[derive(PartialEq, Eq, Hash, Default, Clone, Deserialize, Serialize)]
pub struct Eid([u8; Eid::EID_SIZE]);

impl Eid {
    /// Entity ID size
    pub const EID_SIZE: usize = 32;

    /// Create an empty entity ID
    pub fn new_empty() -> Self {
        Eid::default()
    }

    /// Create a new random entity ID
    pub fn new() -> Self {
        let mut eid = Eid::new_empty();
        Crypto::random_buf(&mut eid.0);
        if let Ok(txid) = Txid::current() {
            eid.0[0] = txid.val() as u8;
        }
        eid
    }

    pub fn from_slice(buf: &[u8]) -> Self {
        assert_eq!(buf.len(), Eid::EID_SIZE);
        let mut ret = Eid::new_empty();
        ret.0.copy_from_slice(buf);
        ret
    }

    pub fn to_path_string(&self) -> PathBuf {
        let s = self.to_string();
        PathBuf::new().join(&s[0..2]).join(&s[2..4]).join(&s)
    }

    pub fn to_short_string(&self) -> String {
        (&self.to_string()[..8]).to_string()
    }
}

impl AsRef<[u8]> for Eid {
    fn as_ref(&self) -> &[u8] {
        &self.0
    }
}

impl Index<usize> for Eid {
    type Output = u8;

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
