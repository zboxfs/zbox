use std::sync::Arc;
use std::collections::VecDeque;
use std::io::{Read, Result as IoResult, Seek, SeekFrom, Write};
use std::fmt::{self, Debug};
use std::time::SystemTime;
use std::path::{Path, PathBuf};
use std::cmp::min;

use error::{Error, Result};
use base::Time;
use base::lru::{CountMeter, Lru, PinChecker};
use trans::{CloneNew, Eid, Id, TxMgrRef, Txid};
use trans::cow::{Cow, CowCache, CowRef, CowWeakRef, IntoCow};
use volume::{Persistable, VolumeRef};
use content::{ChunkMap, Content, ContentReader, ContentRef, StoreRef,
              Writer as StoreWriter};
use super::Handle;

// maximum sub nodes for a fnode
const SUB_NODES_CNT: usize = 8;

/// A structure representing a type of file with accessors for each file type.
#[derive(Debug, Copy, Clone, PartialEq, Deserialize, Serialize)]
pub enum FileType {
    File,
    Dir,
}

impl FileType {
    /// Test whether this file type represents a regular file.
    pub fn is_file(&self) -> bool {
        *self == FileType::File
    }

    /// Test whether this file type represents a directory.
    pub fn is_dir(&self) -> bool {
        *self == FileType::Dir
    }
}

impl Default for FileType {
    fn default() -> Self {
        FileType::File
    }
}

// fnode child entry
#[derive(Debug, Clone, Deserialize, Serialize)]
struct ChildEntry {
    id: Eid,
    ftype: FileType,
    name: String,
}

impl ChildEntry {
    fn new(id: &Eid, ftype: FileType, name: &str) -> Self {
        ChildEntry {
            id: id.clone(),
            ftype,
            name: name.to_string(),
        }
    }
}

/// A representation of a permanent file content.
#[derive(Debug, Default, Clone, Deserialize, Serialize)]
pub struct Version {
    num: usize,      // version number
    content_id: Eid, // content id
    len: usize,
    ctime: Time,
}

impl Version {
    fn new(num: usize, content_id: &Eid, len: usize) -> Self {
        Version {
            num,
            content_id: content_id.clone(),
            len,
            ctime: Time::now(),
        }
    }

    /// Returns the version number of this content.
    ///
    /// The version number starts from 1 and continuously increases by 1.
    pub fn num(&self) -> usize {
        self.num
    }

    /// Returns the byte length of this version of content.
    pub fn len(&self) -> usize {
        self.len
    }

    /// Returns the creation time of this version of content.
    pub fn created(&self) -> SystemTime {
        self.ctime.to_system_time()
    }
}

/// Metadata information about a file or a directory.
///
/// This structure is returned from the [`File::metadata`] and
/// [`Repo::metadata`] represents known metadata about a file such as its type,
/// size, modification times, etc.
///
/// [`File::metadata`]: struct.File.html#method.metadata
/// [`Repo::metadata`]: struct.Repo.html#method.metadata
#[derive(Debug, Copy, Clone)]
pub struct Metadata {
    ftype: FileType,
    len: usize,
    curr_version: usize,
    ctime: Time,
    mtime: Time,
}

impl Metadata {
    /// Returns the file type for this metadata.
    pub fn file_type(&self) -> FileType {
        self.ftype
    }

    /// Returns whether this metadata is for a directory.
    pub fn is_dir(&self) -> bool {
        self.ftype == FileType::Dir
    }

    /// Returns whether this metadata is for a regular file.
    pub fn is_file(&self) -> bool {
        self.ftype == FileType::File
    }

    /// Returns the size of the current version of file, in bytes, this
    /// metadata is for.
    pub fn len(&self) -> usize {
        self.len
    }

    /// Returns current version number of file listed in this metadata.
    pub fn curr_version(&self) -> usize {
        self.curr_version
    }

    /// Returns the creation time listed in this metadata.
    pub fn created(&self) -> SystemTime {
        self.ctime.to_system_time()
    }

    /// Returns the last modification time listed in this metadata.
    pub fn modified(&self) -> SystemTime {
        self.mtime.to_system_time()
    }
}

/// Entries returned by the [`read_dir`] function.
///
/// An instance of `DirEntry` represents an entry inside of a directory in the
/// repository. Each entry can be inspected via methods to learn about the
/// full path or other metadata.
///
/// [`read_dir`]: struct.Repo.html#method.read_dir
#[derive(Debug)]
pub struct DirEntry {
    path: PathBuf,
    metadata: Metadata,
    name: String,
}

impl DirEntry {
    /// Returns the full path to the file that this entry represents.
    pub fn path(&self) -> &Path {
        self.path.as_path()
    }

    /// Return the metadata for the file that this entry points at.
    pub fn metadata(&self) -> Metadata {
        self.metadata
    }

    /// Return the file type for the file that this entry points at.
    pub fn file_type(&self) -> FileType {
        self.metadata.file_type()
    }

    /// Returns the bare file name of this directory entry without any other
    /// leading path component.
    pub fn file_name(&self) -> &str {
        &self.name
    }
}

type SubNodes = Lru<
    String,
    FnodeWeakRef,
    CountMeter<FnodeWeakRef>,
    PinChecker<FnodeWeakRef>,
>;

/// File node
#[derive(Default, Clone, Deserialize, Serialize)]
pub struct Fnode {
    id: Eid,
    ftype: FileType,
    version_limit: u8,
    ctime: Time,
    mtime: Time,
    kids: Vec<ChildEntry>,
    vers: VecDeque<Version>,
    chk_map: ChunkMap,

    // parent fnode
    #[serde(skip_serializing, skip_deserializing, default)]
    parent: Option<FnodeRef>,

    #[serde(skip_serializing, skip_deserializing,
            default = "Fnode::default_sub_nodes")]
    sub_nodes: SubNodes,

    #[serde(skip_serializing, skip_deserializing, default)] store: StoreRef,
}

impl Fnode {
    /// Default versoin limit
    pub const DEFAULT_VERSION_LIMIT: u8 = 10;

    pub fn new(ftype: FileType, version_limit: u8, store: &StoreRef) -> Self {
        let version_limit = match ftype {
            FileType::File => version_limit,
            FileType::Dir => 0,
        };
        Fnode {
            id: Eid::new(),
            ftype,
            version_limit,
            ctime: Time::now(),
            mtime: Time::now(),
            kids: Vec::new(),
            vers: VecDeque::new(),
            chk_map: ChunkMap::new(),
            parent: None,
            sub_nodes: Self::default_sub_nodes(),
            store: store.clone(),
        }
    }

    /// Create new fnode under parent
    pub fn new_under(
        parent: &FnodeRef,
        name: &str,
        ftype: FileType,
        version_limit: u8,
        txmgr: &TxMgrRef,
    ) -> Result<FnodeRef> {
        let kid = {
            let mut pfnode_cow = parent.write().unwrap();
            let pfnode = pfnode_cow.make_mut()?;
            if !pfnode.is_dir() {
                return Err(Error::NotDir);
            }

            // create child fnode and add initial version
            let mut kid = Fnode::new(ftype, version_limit, &pfnode.store);
            if ftype == FileType::File {
                kid.add_ver(Content::new().into_cow(&txmgr)?)?;
            }

            kid.into_cow(txmgr)?
        };

        // add child to parent
        Fnode::add_child(parent, &kid, name)?;

        Ok(kid)
    }

    fn default_sub_nodes() -> SubNodes {
        Lru::new(SUB_NODES_CNT)
    }

    /// Check if fnode is regular file
    pub fn is_file(&self) -> bool {
        self.ftype == FileType::File
    }

    /// Check if fnode is directory
    pub fn is_dir(&self) -> bool {
        self.ftype == FileType::Dir
    }

    /// Check if fnode is root
    pub fn is_root(&self) -> bool {
        self.parent.is_none()
    }

    /// Get fnode metadata
    pub fn metadata(&self) -> Metadata {
        Metadata {
            ftype: self.ftype,
            len: match self.ftype {
                FileType::File => self.curr_ver().len,
                FileType::Dir => 0,
            },
            curr_version: self.curr_ver_num(),
            ctime: self.ctime,
            mtime: self.mtime,
        }
    }

    /// Get size of fnode current version
    pub fn curr_len(&self) -> usize {
        match self.ftype {
            FileType::File => self.curr_ver().len,
            FileType::Dir => 0,
        }
    }

    /// Get fnode version list
    pub fn history(&self) -> Vec<Version> {
        Vec::from(self.vers.clone())
    }

    /// Load root fnode
    pub fn load_root(
        root_id: &Eid,
        txmgr: &TxMgrRef,
        store: &StoreRef,
        vol: &VolumeRef,
    ) -> Result<FnodeRef> {
        let root = Cow::<Fnode>::load_cow(root_id, txmgr, vol)?;
        {
            let mut root_cow = root.write().unwrap();
            let root = root_cow.make_mut_naive()?;
            root.store = store.clone();
        }
        Ok(root)
    }

    // load one child fnode
    fn load_child(
        &mut self,
        name: &str,
        self_ref: FnodeRef,
        cache: &Cache,
        vol: &VolumeRef,
    ) -> Result<FnodeRef> {
        // get child fnode from sub node list first
        if let Some(fnode) = self.sub_nodes
            .get_refresh(name)
            .and_then(|sub| sub.upgrade())
        {
            return Ok(fnode);
        }

        // if child is not in sub node list, load it from fnode cache
        self.kids
            .iter()
            .find(|ref c| c.name == name)
            .ok_or(Error::NotFound)
            .and_then(|child| {
                cache.get(&child.id, vol).map_err(|e| Error::from(e))
            })
            .and_then(|child| {
                // set parent, store and volume for the child
                {
                    let mut child_cow = child.write().unwrap();
                    let c = child_cow.make_mut_naive()?;
                    c.parent = Some(self_ref);
                    c.store = self.store.clone();
                }

                // add to parent's sub node list
                self.sub_nodes
                    .insert(name.to_string(), Arc::downgrade(&child));
                Ok(child)
            })
    }

    pub fn has_child(&self, name: &str) -> bool {
        self.kids.iter().position(|ref c| c.name == name).is_some()
    }

    pub fn children_cnt(&self) -> usize {
        self.kids.len()
    }

    /// Get single child fnode
    pub fn child(
        parent: FnodeRef,
        name: &str,
        cache: &Cache,
        vol: &VolumeRef,
    ) -> Result<FnodeRef> {
        let mut par = parent.write().unwrap();
        par.make_mut_naive()?
            .load_child(name, parent.clone(), cache, vol)
    }

    fn children_names(&self) -> Vec<String> {
        self.kids.iter().map(|ref k| k.name.clone()).collect()
    }

    /// Get children dir entry list
    pub fn read_dir(
        parent: FnodeRef,
        path: &Path,
        cache: &Cache,
        vol: &VolumeRef,
    ) -> Result<Vec<DirEntry>> {
        let mut par = parent.write().unwrap();
        let par = par.make_mut_naive()?;
        if !par.is_dir() {
            return Err(Error::NotDir);
        }

        let parent_path = {
            #[cfg(windows)]
            {
                let mut path_str = path.to_str().unwrap().to_string();
                if !path_str.ends_with("/") {
                    path_str.push_str("/");
                }
                PathBuf::from(path_str)
            }
            #[cfg(unix)]
            {
                path
            }
        };

        let mut ret = Vec::new();
        let child_names = par.children_names();

        for name in child_names.iter() {
            let child_ref = par.load_child(&name, parent.clone(), cache, vol)?;
            let child = child_ref.read().unwrap();
            ret.push(DirEntry {
                path: parent_path.join(name),
                metadata: child.metadata(),
                name: name.clone(),
            });
        }

        Ok(ret)
    }

    /// Add child to parent fnode
    pub fn add_child(
        parent: &FnodeRef,
        child: &FnodeRef,
        name: &str,
    ) -> Result<()> {
        let mut parent_cow = parent.write().unwrap();
        let par = parent_cow.make_mut()?;

        // add to child to parent's children list
        let mut kid = child.write().unwrap();
        par.kids.push(ChildEntry::new(kid.id(), kid.ftype, name));

        // update child's parent
        kid.make_mut()?.parent = Some(parent.clone());

        // add to parent's sub node list and update modified time
        par.sub_nodes
            .insert(name.to_string(), Arc::downgrade(child));
        par.mtime = Time::now();

        Ok(())
    }

    /// Remove child fnode from parent
    pub fn unlink(fnode: &FnodeRef) -> Result<()> {
        let child = fnode.read().unwrap();
        match child.parent {
            Some(ref parent) => {
                let mut par = parent.write().unwrap();
                let par = par.make_mut()?;
                let child_idx = par.kids
                    .iter()
                    .position(|ref c| c.id == *child.id())
                    .ok_or(Error::NotFound)?;
                {
                    let name = &par.kids[child_idx].name;
                    par.sub_nodes.remove(name);
                }
                par.kids.remove(child_idx);
                Ok(())
            }
            None => return Err(Error::IsRoot),
        }
    }

    // get specified version
    fn ver(&self, ver_num: usize) -> Option<&Version> {
        self.vers.iter().find(|v| v.num == ver_num)
    }

    // get current version
    fn curr_ver(&self) -> &Version {
        self.vers.back().unwrap()
    }

    /// Get current version number
    pub fn curr_ver_num(&self) -> usize {
        if self.vers.is_empty() {
            return 0;
        }
        self.curr_ver().num
    }

    // remove a specified version and its associated content
    fn remove_ver(&mut self, ver_num: usize) -> Result<()> {
        let idx = self.vers
            .iter()
            .position(|v| v.num == ver_num)
            .ok_or(Error::NoVersion)?;
        let ver = self.vers.remove(idx).unwrap();

        if let Some(ctn) = {
            let mut store = self.store.write().unwrap();
            store.make_mut()?.deref_content(&ver.content_id)?
        } {
            Content::unlink(&ctn, &mut self.chk_map, &self.store)?;
        }

        Ok(())
    }

    pub fn clear_vers(&mut self) -> Result<()> {
        let ver_nums: Vec<usize> = self.vers.iter().map(|v| v.num).collect();
        for ver_num in ver_nums {
            self.remove_ver(ver_num)?;
        }
        Ok(())
    }

    // add a new version
    // return content if it is duplicated, none if not
    fn add_ver(&mut self, content: ContentRef) -> Result<Option<ContentRef>> {
        // dedup content and add the new version
        let is_deduped = {
            let ctn = content.read().unwrap();
            let mut store_cow = self.store.write().unwrap();
            let store = store_cow.make_mut()?;
            let deduped_id = store.dedup_content(ctn.id(), ctn.hash())?;
            let ver =
                Version::new(self.curr_ver_num() + 1, &deduped_id, ctn.len());
            self.mtime = ver.ctime;
            self.vers.push_back(ver);
            deduped_id != *ctn.id()
        };

        // remove the oldest version, note that version limit is zero based
        if self.vers.len() > self.version_limit as usize {
            let retire = self.vers.front().unwrap().num;
            self.remove_ver(retire)?;
        }

        if is_deduped {
            Ok(Some(content.clone()))
        } else {
            Ok(None)
        }
    }

    /// Get reader for sepcified version number
    pub fn version_reader(&self, ver_num: usize) -> Result<ContentReader> {
        let ver = self.ver(ver_num).ok_or(Error::NoVersion)?;
        let content = {
            let st = self.store.read().unwrap();
            st.get_content(&ver.content_id)?
        };
        Ok(ContentReader::new(&content, &self.store))
    }

    // clone a new current content
    fn clone_current_content(&self, txmgr: &TxMgrRef) -> Result<ContentRef> {
        let curr_ctn = {
            let store = self.store.read().unwrap();
            store.get_content(&self.curr_ver().content_id)?
        };
        let new_ctn = curr_ctn.read().unwrap();
        new_ctn.clone_new().into_cow(txmgr)
    }

    /// Set file to specified length
    ///
    /// if new length is equal to old length, do nothing
    pub fn set_len(handle: Handle, len: usize, txid: Txid) -> Result<()> {
        let curr_len = {
            let fnode = handle.fnode.read().unwrap();
            fnode.curr_len()
        };

        if curr_len < len {
            // append
            let mut size = len - curr_len;
            let buf = vec![0u8; min(size, 16 * 1024)];
            let mut wtr = Writer::new(handle.clone(), txid)?;
            wtr.seek(SeekFrom::Start(curr_len as u64))?;

            while size > 0 {
                let write_len = min(size, buf.len());
                let written = wtr.write(&buf[..write_len])?;
                size -= written;
            }
            wtr.finish()?;
        } else if curr_len > len {
            // truncate
            let mut fnode_cow = handle.fnode.write().unwrap();
            let ctn = {
                let new_ctn = fnode_cow.clone_current_content(&handle.txmgr)?;
                Content::truncate(&new_ctn, len, &handle.store)?;
                new_ctn
            };

            // link the new content first
            Content::link(ctn.clone(), &handle.store)?;

            // then dedup content and add deduped content as a new version
            // if content is duplicated, then unlink the content
            let fnode = fnode_cow.make_mut()?;
            if let Some(ctn) = fnode.add_ver(ctn)? {
                Content::unlink(&ctn, &mut fnode.chk_map, &handle.store)?;
            }
        }

        Ok(())
    }
}

impl Debug for Fnode {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("Fnode")
            .field("id", &self.id)
            .field("ftype", &self.ftype)
            .field("version_limit", &self.version_limit)
            .field("ctime", &self.ctime)
            .field("mtime", &self.mtime)
            .field("kids", &self.kids)
            .field("vers", &self.vers)
            .field("sub_nodes", &self.sub_nodes)
            .finish()
    }
}

impl Id for Fnode {
    #[inline]
    fn id(&self) -> &Eid {
        &self.id
    }

    #[inline]
    fn id_mut(&mut self) -> &mut Eid {
        &mut self.id
    }
}

impl CloneNew for Fnode {}

impl<'de> IntoCow<'de> for Fnode {}

impl<'de> Persistable<'de> for Fnode {}

/// Fnode reference type
pub type FnodeRef = CowRef<Fnode>;

/// Fnode weak reference type
pub type FnodeWeakRef = CowWeakRef<Fnode>;

/// Fnode Reader
#[derive(Debug)]
pub struct Reader {
    rdr: ContentReader,
}

impl Reader {
    /// Create a reader for specified version
    pub fn new(fnode: FnodeRef, ver: usize) -> Result<Self> {
        let fnode = fnode.read().unwrap();
        let rdr = fnode.version_reader(ver)?;
        Ok(Reader { rdr })
    }

    /// Create a reader for current version
    pub fn new_current(fnode: FnodeRef) -> Result<Self> {
        let fnode = fnode.read().unwrap();
        let rdr = fnode.version_reader(fnode.curr_ver_num())?;
        Ok(Reader { rdr })
    }
}

impl Read for Reader {
    fn read(&mut self, buf: &mut [u8]) -> IoResult<usize> {
        self.rdr.read(buf)
    }
}

impl Seek for Reader {
    fn seek(&mut self, pos: SeekFrom) -> IoResult<u64> {
        self.rdr.seek(pos)
    }
}

/// Fnode Writer
#[derive(Debug)]
pub struct Writer {
    inner: StoreWriter,
    handle: Handle,
}

impl Writer {
    pub fn new(handle: Handle, txid: Txid) -> Result<Self> {
        let chk_map = {
            let f = handle.fnode.read().unwrap();
            f.chk_map.clone()
        };
        let inner =
            StoreWriter::new(chk_map, &handle.txmgr, &handle.store, txid)?;
        Ok(Writer { inner, handle })
    }

    pub fn finish(self) -> Result<()> {
        let stg_ctn = self.inner.finish()?;
        let handle = &self.handle;

        let mut fnode_cow = handle.fnode.write().unwrap();

        // merge stage content to current content
        let ctn = {
            let new_ctn = fnode_cow.clone_current_content(&handle.txmgr)?;
            Content::replace(&new_ctn, &stg_ctn, &handle.store)?;
            new_ctn
        };

        // dedup content and add deduped content as a new version
        let fnode = fnode_cow.make_mut()?;
        if let Some(ctn) = fnode.add_ver(ctn)? {
            // content is duplicated
            Content::unlink(&ctn, &mut fnode.chk_map, &handle.store)?;
        }

        Ok(())
    }
}

impl Write for Writer {
    fn write(&mut self, buf: &[u8]) -> IoResult<usize> {
        self.inner.write(buf)
    }

    fn flush(&mut self) -> IoResult<()> {
        self.inner.flush()
    }
}

impl Seek for Writer {
    fn seek(&mut self, pos: SeekFrom) -> IoResult<u64> {
        self.inner.seek(pos)
    }
}

/// Fnode cache
pub type Cache = CowCache<Fnode>;
