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

use std::sync::{Arc, RwLock, Once, ONCE_INIT};

use env_logger;

static INIT: Once = ONCE_INIT;

/// Global initilisation
pub fn global_init() {
    // only call the initialisation code once globally
    INIT.call_once(|| {
        env_logger::init().expect("Initialise logger failed");
        crypto::Crypto::init().expect("Initialise crypto failed");
    });
}

/// Wrap type into reference type Arc<RwLock<T>>
pub trait IntoRef: Sized {
    fn into_ref(self) -> Arc<RwLock<Self>> {
        Arc::new(RwLock::new(self))
    }
}
