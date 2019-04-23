//! base module document
//!

pub(crate) mod crypto;
pub(crate) mod lru;
pub(crate) mod lz4;
mod refcnt;
mod time;
pub(crate) mod utils;
pub(crate) mod version;
pub(crate) mod vio;

pub use self::refcnt::RefCnt;
pub use self::time::Time;
pub use self::version::Version;

use std::sync::{Arc, Once, RwLock, ONCE_INIT};

#[cfg(target_os = "android")]
use log::Level;

#[cfg(target_os = "android")]
use android_logger::{self, Filter};

#[cfg(target_arch = "wasm32")]
use log::Level;

#[cfg(target_arch = "wasm32")]
use wasm_logger;

#[cfg(all(not(target_os = "android"), not(target_arch = "wasm32")))]
use env_logger;

static INIT: Once = ONCE_INIT;

cfg_if! {
    if #[cfg(target_os = "android")] {
        pub fn init_env() {
            // only call the initialisation code once globally
            INIT.call_once(|| {
                android_logger::init_once(
                    Filter::default()
                        .with_min_level(Level::Trace)
                        .with_allowed_module_path("zbox::fs::fs")
                        .with_allowed_module_path("zbox::trans::txmgr"),
                    Some("zboxfs"),
                );
                crypto::Crypto::init().expect("Initialise crypto failed");
            });
        }
    } else if #[cfg(target_arch = "wasm32")] {
        pub fn init_env() {
            INIT.call_once(|| {
                wasm_logger::init(wasm_logger::Config::new(Level::Trace));
                crypto::Crypto::init().expect("Initialise crypto failed");
            });
        }
        pub fn init_env_no_logging() {
            INIT.call_once(|| {
                crypto::Crypto::init().expect("Initialise crypto failed");
            });
        }
    } else {
        /// Initialise ZboxFS environment.
        ///
        /// This function should be called before any other functions provided
        /// by ZboxFS.
        /// This function can be called more than one time.
        pub fn init_env() {
            INIT.call_once(|| {
                env_logger::try_init().ok();
                crypto::Crypto::init().expect("Initialise crypto failed");
            });
        }
    }
}

/// Wrap type into reference type Arc<RwLock<T>>
pub trait IntoRef: Sized {
    fn into_ref(self) -> Arc<RwLock<Self>> {
        Arc::new(RwLock::new(self))
    }
}
