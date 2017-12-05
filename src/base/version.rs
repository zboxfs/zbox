use version;

/// Semantic version
#[derive(Debug, Default, Clone)]
pub struct Version {
    major: u8,
    minor: u8,
    patch: u8,
}

impl Version {
    pub const BYTES_LEN: usize = 3;

    pub fn new(major: u8, minor: u8, patch: u8) -> Self {
        Version {
            major,
            minor,
            patch,
        }
    }

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

    #[inline]
    pub fn serialize(&self) -> [u8; 3] {
        [self.major, self.minor, self.patch]
    }

    pub fn deserialize(buf: &[u8]) -> Self {
        assert_eq!(buf.len(), Version::BYTES_LEN);
        Version::new(buf[0], buf[1], buf[2])
    }

    pub fn to_string(&self) -> String {
        format!("{}.{}.{}", self.major, self.minor, self.patch)
    }
}
