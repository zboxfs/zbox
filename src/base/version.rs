use std::fmt::{self, Debug};

use version;

/// Semantic version
#[derive(Default, Clone, Deserialize, Serialize)]
pub struct Version {
    major: u8,
    minor: u8,
    patch: u8,
}

impl Version {
    pub fn current() -> Self {
        Version {
            major: version::MAJOR_VERSION,
            minor: version::MINOR_VERSION,
            patch: version::PATCH_VERSION,
        }
    }

    pub fn match_current_minor(&self) -> bool {
        let curr = Version::current();
        self.major == curr.major && self.minor == curr.minor
    }

    pub fn to_string(&self) -> String {
        format!("{}.{}.{}", self.major, self.minor, self.patch)
    }
}

impl Debug for Version {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Version({})", self.to_string())
    }
}
