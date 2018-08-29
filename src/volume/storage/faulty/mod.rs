mod faulty;

pub use self::faulty::FaultyStorage;

#[cfg(feature = "storage-faulty")]
pub use self::faulty::Controller;
