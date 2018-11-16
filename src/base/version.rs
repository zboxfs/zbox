use std::fmt::{self, Debug, Display};

use version;

/// Semantic version
#[derive(Default, Clone, Deserialize, Serialize)]
pub struct Version {
    major: u8,
    minor: u8,
    patch: u8,
}

impl Version {
    #[inline]
    pub fn current_repo_version() -> Self {
        Version {
            major: version::REPO_MAJOR_VERSION,
            minor: version::REPO_MINOR_VERSION,
            patch: version::REPO_PATCH_VERSION,
        }
    }

    #[allow(dead_code)]
    #[inline]
    pub fn current_lib_version() -> Self {
        Version {
            major: version::LIB_MAJOR_VERSION,
            minor: version::LIB_MINOR_VERSION,
            patch: version::LIB_PATCH_VERSION,
        }
    }

    pub fn match_repo_version(&self) -> bool {
        let curr = Version::current_repo_version();
        self.major == curr.major && self.minor == curr.minor
    }
}

impl Debug for Version {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Version({})", self.to_string())
    }
}

impl Display for Version {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}.{}.{}", self.major, self.minor, self.patch)
    }
}
