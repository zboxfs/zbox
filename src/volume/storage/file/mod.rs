mod emap;
mod file;
mod sector;
mod vio;

pub use self::file::FileStorage;

use std::io::{ErrorKind, Read, Result as IoResult, Write};
use std::path::Path;

use serde::{Deserialize, Serialize};
use rmp_serde::{Deserializer, Serializer};

use error::Result;
use base::crypto::{Crypto, Key};
use self::vio::imp as vio_imp;

// remove file if it exists
fn remove_file<P: AsRef<Path>>(path: P) -> IoResult<bool> {
    match vio_imp::remove_file(path.as_ref()) {
        Ok(_) => Ok(true),
        Err(ref e) if e.kind() == ErrorKind::NotFound => Ok(false),
        Err(e) => Err(e),
    }
}

// remove dir and its content if it exists
fn remove_dir_all<P: AsRef<Path>>(path: P) -> IoResult<()> {
    match vio_imp::remove_dir_all(path.as_ref()) {
        Ok(_) => Ok(()),
        Err(ref e) if e.kind() == ErrorKind::NotFound => Ok(()),
        Err(e) => Err(e),
    }
}

// serilise, encrypt and save object to disk
fn save_obj<S, P>(obj: &S, path: P, skey: &Key, crypto: &Crypto) -> Result<()>
where
    S: Serialize,
    P: AsRef<Path>,
{
    let mut buf = Vec::new();
    obj.serialize(&mut Serializer::new(&mut buf))?;
    let enc = crypto.encrypt(&buf, skey)?;
    let mut file = vio_imp::OpenOptions::new()
        .write(true)
        .create_new(true)
        .truncate(true)
        .open(path.as_ref())?;
    file.write_all(&enc)?;
    Ok(())
}

// deserilise, decrypt and load object from disk
fn load_obj<'de, D, P>(path: P, skey: &Key, crypto: &Crypto) -> Result<D>
where
    D: Deserialize<'de>,
    P: AsRef<Path>,
{
    let mut buf = Vec::new();
    let mut rd = vio_imp::File::open(path.as_ref())?;
    rd.read_to_end(&mut buf)?;
    let dec = crypto.decrypt(&buf, skey)?;
    let mut de = Deserializer::new(&dec[..]);
    Ok(Deserialize::deserialize(&mut de)?)
}
