use std::fmt::{self, Debug};
use std::ops::Index;
use std::path::{Path, PathBuf};

use base::crypto::Crypto;

/// Unique entity ID.
///
/// This represents a 32-byte randomly generated unique ID.
#[repr(C)]
#[derive(PartialEq, PartialOrd, Ord, Eq, Hash, Default, Clone, Deserialize,
         Serialize)]
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

    #[inline]
    pub(crate) fn is_empty(&self) -> bool {
        *self == Eid::default()
    }

    pub(crate) fn from_slice(buf: &[u8]) -> Self {
        assert_eq!(buf.len(), Eid::EID_SIZE);
        let mut ret = Eid::new_empty();
        ret.0.copy_from_slice(buf);
        ret
    }

    pub(crate) fn to_path_buf(&self, base: &Path) -> PathBuf {
        let s = self.to_string();
        base.join(&s[0..2]).join(&s[2..4]).join(&s)
    }

    #[allow(dead_code)]
    pub(crate) fn to_rel_path(&self) -> PathBuf {
        let base = Path::new("");
        self.to_path_buf(&base)
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
            self.0.iter().map(|b| format!("{:02x}", b)).collect();
        strs.join("")
    }
}

/// Entity Id trait
pub trait Id {
    fn id(&self) -> &Eid;
    fn id_mut(&mut self) -> &mut Eid;
}
