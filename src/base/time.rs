use std::fmt::{self, Debug};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

#[derive(Copy, Clone, Default, Deserialize, Serialize)]
pub struct Time(Duration);

impl Time {
    pub const BYTES_LEN: usize = 8; // seconds, u64

    pub fn now() -> Self {
        let now = SystemTime::now();
        let duration = now.duration_since(UNIX_EPOCH).unwrap();
        Time(duration)
    }

    #[inline]
    pub fn from_secs(secs: u64) -> Self {
        Time(Duration::from_secs(secs))
    }

    #[inline]
    pub fn as_secs(&self) -> u64 {
        self.0.as_secs()
    }

    #[inline]
    pub fn to_system_time(&self) -> SystemTime {
        UNIX_EPOCH + self.0
    }
}

impl Debug for Time {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Time({})", &self.0.as_secs())
    }
}
