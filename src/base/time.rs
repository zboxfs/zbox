use std::fmt::{self, Debug};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};

#[cfg(target_arch = "wasm32")]
use js_sys;

#[derive(Copy, Clone, Default, Deserialize, Serialize)]
pub struct Time(Duration);

impl Time {
    pub fn now() -> Self {
        let now = {
            #[cfg(target_arch = "wasm32")]
            {
                let js_date = js_sys::Date::now() as u64;
                UNIX_EPOCH + Duration::from_millis(js_date)
            }
            #[cfg(not(target_arch = "wasm32"))]
            {
                SystemTime::now()
            }
        };
        let duration = now.duration_since(UNIX_EPOCH).unwrap();
        Time(duration)
    }

    #[inline]
    pub fn to_system_time(self) -> SystemTime {
        UNIX_EPOCH + self.0
    }
}

impl Debug for Time {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Time({})", &self.0.as_secs())
    }
}
