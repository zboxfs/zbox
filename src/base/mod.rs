//! base module document
//!

pub mod crypto;
pub mod lru;
mod refcnt;
mod time;
pub mod utils;
mod version;

pub use self::refcnt::RefCnt;
pub use self::time::Time;
pub use self::version::Version;

use std::sync::{Arc, Once, RwLock, ONCE_INIT};

#[cfg(target_os = "android")]
use android_log;

#[cfg(not(target_os = "android"))]
use env_logger;

static INIT: Once = ONCE_INIT;

/// Initialise ZboxFS environment.
///
/// This function should be called before any other functions provided by ZboxFS.
/// This function can be called more than one time.
pub fn init_env() {
    // only call the initialisation code once globally
    INIT.call_once(|| {
        #[cfg(target_os = "android")]
        {
            android_log::init("ZboxFS").unwrap();
        }
        #[cfg(not(target_os = "android"))]
        {
            env_logger::try_init().ok();
        }
        crypto::Crypto::init().expect("Initialise crypto failed");
    });
}

/// Wrap type into reference type Arc<RwLock<T>>
pub trait IntoRef: Sized {
    fn into_ref(self) -> Arc<RwLock<Self>> {
        Arc::new(RwLock::new(self))
    }
}
