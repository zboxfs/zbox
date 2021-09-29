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

use std::sync::{Arc, Once, RwLock};

use cfg_if::cfg_if;
use log::info;

#[cfg(target_os = "android")]
use std::ptr::NonNull;

#[cfg(target_os = "android")]
use std::sync::Mutex;

#[cfg(target_os = "android")]
use jni::{JNIEnv, JavaVM};

/// Get ZboxFS library version string.
///
/// This method return ZboxFS library version as a string, e.g. "ZboxFS v0.9.2".
#[inline]
pub fn zbox_version() -> String {
    format!("ZboxFS v{}", Version::lib_version())
}

static INIT: Once = Once::new();

#[cfg(target_os = "android")]
lazy_static! {
    // global JVM pointer
    pub static ref JVM: Mutex<JavaVM> = unsafe {
        let p = NonNull::dangling();
        Mutex::new(JavaVM::from_raw(p.as_ptr()).unwrap())
    };
}

cfg_if! {
    if #[cfg(target_os = "android")] {
        pub fn init_env(env: JNIEnv) {
            // only call the initialisation code once globally
            INIT.call_once(|| {
                crypto::Crypto::init().expect("Initialise crypto failed");

                // save global JVM pointer
                let jvm = env.get_java_vm().unwrap();
                let mut jvm_ptr = JVM.lock().unwrap();
                *jvm_ptr = jvm;

                info!(
                    "{} - Zero-details, privacy-focused in-app file system",
                    zbox_version()
                );
            });
        }
    } else {
        /// Initialise ZboxFS environment.
        ///
        /// This method should be called before any other methods provided
        /// by ZboxFS.
        /// This method can be called more than one time.
        pub fn init_env() {
            INIT.call_once(|| {
                #[cfg(not(target_arch = "wasm32"))]
                {
                    env_logger::try_init().ok();
                }
                crypto::Crypto::init().expect("Initialise crypto failed");
                info!(
                    "{} - Zero-details, privacy-focused in-app file system",
                    zbox_version()
                );
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
