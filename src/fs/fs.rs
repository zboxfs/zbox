use std::sync::{Arc, RwLock};
use std::path::Path;
use std::io;

use bytes::BufMut;

use error::{Error, Result};
use base::IntoRef;
use base::crypto::{Cipher, Cost};
use content::{Store, StoreRef};
use trans::{Id, Eid, Txid, TxMgr, TxMgrRef};
use trans::cow::IntoCow;
use volume::{Volume, VolumeRef, Meta as VolumeMeta};
use super::Handle;
use super::fnode::{Fnode, FnodeRef, FileType, Version, Metadata, DirEntry,
                   Cache as FnodeCache, Reader as FnodeReader,
                   Writer as FnodeWriter};

// default cache size
const FNODE_CACHE_SIZE: usize = 16;

/// File system metadata
#[derive(Debug)]
pub struct Meta {
    pub version_limit: u8,
    pub read_only: bool,
    pub vol_meta: VolumeMeta,
}

/// File system
#[derive(Debug)]
pub struct Fs {
    root: FnodeRef,
    fcache: FnodeCache,
    store: StoreRef,
    txmgr: TxMgrRef,
    vol: VolumeRef,
    version_limit: u8,
    read_only: bool,
}

impl Fs {
    /// Check if fs exists
    #[inline]
    pub fn exists(uri: &str) -> Result<bool> {
        Volume::exists(uri)
    }

    /// Create new fs
    pub fn create(
        uri: &str,
        pwd: &str,
        cost: Cost,
        cipher: Cipher,
        version_limit: u8,
    ) -> Result<Fs> {
        // create and initialise volume
        let mut vol = Volume::new(uri)?;
        vol.init(cost, cipher)?;
        let vol = vol.into_ref();

        // create fs components
        let txmgr = TxMgr::new(Txid::from(0), &vol).into_ref();
        let fcache = FnodeCache::new(FNODE_CACHE_SIZE, &txmgr);

        // the initial transaction, it must be successful
        let mut store_ref: Option<StoreRef> = None;
        let mut root_ref: Option<FnodeRef> = None;
        TxMgr::begin_trans(&txmgr)?.run_all(|| {
            let store_cow = Store::new(&txmgr, &vol).into_cow(&txmgr)?;
            let root_cow =
                Fnode::new(FileType::Dir, 0, &store_cow).into_cow(&txmgr)?;
            root_ref = Some(root_cow);
            store_ref = Some(store_cow);
            Ok(())
        })?;

        // write volume super block with payload
        // payload: store_id + root_id + version_limit
        let store = store_ref.unwrap();
        let root = root_ref.unwrap();
        {
            let mut payload = Vec::with_capacity(2 * Eid::EID_SIZE + 1);
            let store = store.read().unwrap();
            let root = root.read().unwrap();
            payload.put(store.id().as_ref());
            payload.put(root.id().as_ref());
            payload.put(version_limit);
            let mut vol = vol.write().unwrap();
            vol.write_payload(pwd, &payload)?;
        }

        Ok(Fs {
            root,
            fcache,
            store,
            txmgr,
            vol,
            version_limit,
            read_only: false,
        })
    }

    /// Open fs
    pub fn open(uri: &str, pwd: &str, read_only: bool) -> Result<Fs> {
        let mut vol = Volume::new(uri)?;

        // open volume
        let (last_txid, payload) = vol.open(pwd)?;
        let mut iter = payload.chunks(Eid::EID_SIZE);
        let store_id = Eid::from_slice(iter.next().unwrap());
        let root_id = Eid::from_slice(iter.next().unwrap());
        let version_limit = iter.next().unwrap()[0];
        let vol = vol.into_ref();

        // create file sytem components
        let txmgr = TxMgr::new(last_txid, &vol).into_ref();
        let store = Store::load_store(&store_id, &txmgr, &vol)?;
        let root = Fnode::load_root(&root_id, &txmgr, &store, &vol)?;
        let fcache = FnodeCache::new(FNODE_CACHE_SIZE, &txmgr);

        Ok(Fs {
            root,
            fcache,
            store,
            txmgr,
            vol,
            version_limit,
            read_only,
        })
    }

    #[inline]
    pub fn is_read_only(&self) -> bool {
        self.read_only
    }

    /// Get file system metadata
    pub fn meta(&self) -> Meta {
        let vol = self.vol.read().unwrap();
        Meta {
            version_limit: self.version_limit,
            read_only: self.read_only,
            vol_meta: vol.meta(),
        }
    }

    /// Reset volume password
    pub fn reset_password(
        &mut self,
        old_pwd: &str,
        new_pwd: &str,
        cost: Cost,
    ) -> Result<()> {
        if self.read_only {
            return Err(Error::ReadOnly);
        }

        let mut vol = self.vol.write().unwrap();
        vol.reset_password(old_pwd, new_pwd, cost)
    }

    /// Resolve path
    pub fn resolve(&self, path: &Path) -> Result<FnodeRef> {
        // only resolve absolute path
        if !path.has_root() {
            return Err(Error::InvalidPath);
        }

        let mut fnode = self.root.clone();

        // loop through path component and skip root
        for name in path.iter().skip(1) {
            let name = name.to_str().unwrap();
            let fc = fnode.clone();
            fnode = Fnode::child(fc, name, &self.fcache, &self.vol)?;
        }
        Ok(fnode)
    }

    // resolve path to parent fnode and child file name
    fn resolve_parent(&self, path: &Path) -> Result<(FnodeRef, String)> {
        let parent_path = path.parent().ok_or(Error::IsRoot)?;
        let file_name = path.file_name().and_then(|s| s.to_str()).ok_or(
            Error::InvalidPath,
        )?;
        let parent = self.resolve(parent_path)?;
        Ok((parent, file_name.to_string()))
    }

    /// Open fnode
    pub fn open_fnode(&mut self, path: &Path) -> Result<Handle> {
        let fnode = self.resolve(path)?;
        Ok(Handle {
            fnode,
            store: self.store.clone(),
            txmgr: self.txmgr.clone(),
            vol: self.vol.clone(),
        })
    }

    /// Create fnode
    pub fn create_fnode(
        &mut self,
        path: &Path,
        ftype: FileType,
        version_limit: Option<u8>,
    ) -> Result<FnodeRef> {
        if self.read_only {
            return Err(Error::ReadOnly);
        }

        let (parent, name) = self.resolve_parent(path)?;

        {
            let parent = parent.read().unwrap();
            if !parent.is_dir() {
                return Err(Error::NotDir);
            }
            if parent.has_child(&name) {
                return Err(Error::AlreadyExists);
            }
        }

        let mut fnode = FnodeRef::default();
        let tx_handle = TxMgr::begin_trans(&self.txmgr)?;
        tx_handle.run_all(|| {
            fnode = Fnode::new_under(
                &parent,
                &name,
                ftype,
                version_limit.unwrap_or(self.version_limit),
                &self.txmgr,
            )?;
            Ok(())
        })?;

        Ok(fnode)
    }

    /// Recursively create directories along the path
    pub fn create_dir_all(&mut self, path: &Path) -> Result<()> {
        match self.create_fnode(path, FileType::Dir, None) {
            Ok(_) => return Ok(()),
            Err(ref e) if *e == Error::NotFound => {}
            Err(err) => return Err(err),
        }
        match path.parent() {
            Some(p) => self.create_dir_all(p)?,
            None => return Err(Error::IsRoot),
        }
        self.create_fnode(path, FileType::Dir, None)?;
        Ok(())
    }

    /// Read directory entries
    pub fn read_dir(&self, path: &Path) -> Result<Vec<DirEntry>> {
        let parent = self.resolve(path)?;
        Fnode::read_dir(parent, path, &self.fcache, &self.vol)
    }

    /// Get metadata of specified path
    pub fn metadata(&self, path: &Path) -> Result<Metadata> {
        let fnode_ref = self.resolve(path)?;
        let fnode = fnode_ref.read().unwrap();
        Ok(fnode.metadata())
    }

    /// Get file version list of specified path
    pub fn history(&self, path: &Path) -> Result<Vec<Version>> {
        let fnode_ref = self.resolve(path)?;
        let fnode = fnode_ref.read().unwrap();
        if fnode.is_dir() {
            return Err(Error::IsDir);
        }
        Ok(fnode.history())
    }

    /// Copy a regular file to another
    pub fn copy(&mut self, from: &Path, to: &Path) -> Result<()> {
        if self.read_only {
            return Err(Error::ReadOnly);
        }

        let src = self.open_fnode(from)?;
        {
            let fnode = src.fnode.read().unwrap();
            if !fnode.is_file() {
                return Err(Error::NotFile);
            }
        }

        let tgt = {
            match self.open_fnode(to) {
                Ok(tgt) => {
                    // if target and source is same fnode, do nothing
                    if Arc::ptr_eq(&tgt.fnode, &src.fnode) {
                        return Ok(());
                    }

                    {
                        let fnode = tgt.fnode.read().unwrap();
                        if !fnode.is_file() {
                            return Err(Error::NotFile);
                        }
                    }
                    tgt
                }
                Err(ref err) if *err == Error::NotFound => {
                    self.create_fnode(to, FileType::File, None)?;
                    self.open_fnode(to)?
                }
                Err(err) => return Err(err),
            }
        };

        // tx #1: truncate target file
        let tx_handle = TxMgr::begin_trans(&self.txmgr)?;
        tx_handle.run_all(
            || Fnode::set_len(tgt.clone(), 0, tx_handle.txid),
        )?;

        // tx #2: copy data from source to target
        let tx_handle = TxMgr::begin_trans(&self.txmgr)?;
        tx_handle.run_all(|| {
            let mut rdr = FnodeReader::new_current(src.fnode.clone())?;
            let mut wtr = FnodeWriter::new(tgt.clone(), tx_handle.txid)?;
            io::copy(&mut rdr, &mut wtr)?;
            wtr.finish()
        })?;

        Ok(())
    }

    /// Remove a regular file
    pub fn remove_file(&mut self, path: &Path) -> Result<()> {
        if self.read_only {
            return Err(Error::ReadOnly);
        }

        let fnode_ref = self.resolve(path)?;
        {
            let fnode = fnode_ref.read().unwrap();
            if !fnode.is_file() {
                return Err(Error::NotFile);
            }
        }

        // begin and run transaction
        let tx_handle = TxMgr::begin_trans(&self.txmgr)?;
        tx_handle.run_all(move || {
            Fnode::unlink(&fnode_ref)?;
            let mut fnode = fnode_ref.write().unwrap();
            fnode.make_mut()?.clear_vers()?;
            fnode.make_del()?;
            self.fcache.remove(fnode.id());
            Ok(())
        })?;

        Ok(())
    }

    /// Remove an existing empty directory
    pub fn remove_dir(&mut self, path: &Path) -> Result<()> {
        if self.read_only {
            return Err(Error::ReadOnly);
        }

        let fnode_ref = self.resolve(path)?;
        {
            let fnode = fnode_ref.read().unwrap();
            if !fnode.is_dir() {
                return Err(Error::NotDir);
            }
            if fnode.is_root() {
                return Err(Error::IsRoot);
            }
            if fnode.children_cnt() > 0 {
                return Err(Error::NotEmpty);
            }
        }

        // begin and run transaction
        let tx_handle = TxMgr::begin_trans(&self.txmgr)?;
        tx_handle.run_all(move || {
            Fnode::unlink(&fnode_ref)?;
            let mut fnode = fnode_ref.write().unwrap();
            fnode.make_del()?;
            self.fcache.remove(fnode.id());
            Ok(())
        })?;

        Ok(())
    }

    /// Remove an existing directory recursively
    pub fn remove_dir_all(&mut self, path: &Path) -> Result<()> {
        for child in self.read_dir(path)? {
            let child_path = child.path();
            match child.file_type() {
                FileType::File => self.remove_file(&child_path)?,
                FileType::Dir => self.remove_dir_all(&child_path)?,
            }
        }
        match self.remove_dir(path) {
            Ok(_) => Ok(()),
            Err(ref err) if *err == Error::IsRoot => Ok(()),
            Err(err) => Err(err),
        }
    }

    /// Rename a file or directory to new name
    pub fn rename(&mut self, from: &Path, to: &Path) -> Result<()> {
        if self.read_only {
            return Err(Error::ReadOnly);
        }

        if to.starts_with(from) {
            return Err(Error::InvalidArgument);
        }

        let src = self.open_fnode(from)?;
        {
            let fnode = src.fnode.read().unwrap();
            if fnode.is_root() {
                return Err(Error::IsRoot);
            }
        }

        let (tgt_parent, name) = self.resolve_parent(to)?;
        {
            let parent = tgt_parent.read().unwrap();
            if parent.has_child(&name) {
                return Err(Error::AlreadyExists);
            }
        }

        // begin and run transaction
        TxMgr::begin_trans(&self.txmgr)?.run_all(|| {
            // remove from source
            Fnode::unlink(&src.fnode)?;

            // and then add to target
            Fnode::add_child(&tgt_parent, &src.fnode, &name)
        })
    }
}

impl IntoRef for Fs {}

/// Fs reference type
pub type FsRef = Arc<RwLock<Fs>>;
