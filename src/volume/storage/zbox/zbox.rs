use std::fmt::{self, Debug};
use std::path::{Path, PathBuf};

use super::index_accessor::IndexAccessor;
use super::local_cache::{CacheType, LocalCache, LocalCacheRef};
use super::sector::SectorMgr;
use base::crypto::{Crypto, Key};
use base::IntoRef;
use error::{Error, Result};
use trans::Eid;
use volume::address::Span;
use volume::storage::index_mgr::{IndexMgr, Lsmt, MemTab, Tab};
use volume::storage::Storable;

// parse uri
// example: access_key@repo_id?cache_type=mem&cache_size=2mb[&base=path]
// return: (
//   access_key: &str,
//   repo_id: &str,
//   cache_type: CacheType,
//   cache_size: usize,
//   base: PathBuf
// )
fn parse_uri(mut uri: &str) -> Result<(&str, &str, CacheType, usize, PathBuf)> {
    if !uri.is_ascii() {
        return Err(Error::InvalidUri);
    }

    // parse access key, required
    let mut idx = uri.find('@').ok_or(Error::InvalidUri)?;
    let access_key = &uri[..idx];
    uri = &uri[idx + 1..];
    if uri.is_empty() {
        return Err(Error::InvalidUri);
    }

    // parse repo id, required
    let repo_id;
    if let Some(idx) = uri.find('?') {
        repo_id = &uri[..idx];
        uri = &uri[idx + 1..];
    } else {
        repo_id = &uri[..uri.len()];
        uri = &uri[repo_id.len()..];
    };

    // set default value for parameters
    let mut cache_type: Option<CacheType> = Some(CacheType::Mem);
    let mut cache_size: Option<usize> = Some(1);
    let mut base: Option<PathBuf> = None;

    // parse parameters
    if !uri.is_empty() {
        for param in uri.split('&') {
            idx = param.find('=').ok_or(Error::InvalidUri)?;
            let key = &param[..idx];
            let value = &param[idx + 1..];

            match key {
                "cache_type" => {
                    let ctype = value.parse::<CacheType>()?;
                    cache_type = Some(ctype);
                }
                "cache_size" => {
                    let value = value.to_lowercase();
                    let idx = value.find("mb").ok_or(Error::InvalidUri)?;
                    let value = &value[..idx];
                    let size = value
                        .parse::<usize>()
                        .map_err(|_| Error::InvalidUri)?;
                    if size < 1 {
                        // cache size must >= 1MB
                        return Err(Error::InvalidUri);
                    }
                    cache_size = Some(size);
                }
                "base" => {
                    base = Some(PathBuf::from(value));
                }
                _ => return Err(Error::InvalidUri),
            }
        }
    }

    // verify parameters
    if cache_type == Some(CacheType::File) && base.is_none() {
        return Err(Error::InvalidUri);
    }

    Ok((
        access_key,
        repo_id,
        cache_type.unwrap(),
        cache_size.unwrap(),
        base.unwrap_or_else(|| PathBuf::from("")),
    ))
}

/// Zbox Storage
pub struct ZboxStorage {
    wal_base: PathBuf,
    local_cache: LocalCacheRef,
    sec_mgr: SectorMgr,
    idx_mgr: IndexMgr,
}

impl ZboxStorage {
    // subkey ids
    const SUBKEY_ID_LOCAL_CACHE: u64 = 42;
    const SUBKEY_ID_SEC_MGR: u64 = 43;
    const SUBKEY_ID_IDX_MGR: u64 = 44;

    // super block file name stem
    const SUPER_BLK_STEM: &'static str = "super_blk";

    // wal file directory
    const WAL_DIR: &'static str = "wal";

    // create zbox storage
    pub fn new(uri: &str) -> Result<Self> {
        // parse uri string
        let (access_key, repo_id, cache_type, cache_size, base) =
            parse_uri(uri)?;

        // create local cache
        let local_cache = LocalCache::new(
            cache_type, cache_size, &base, repo_id, access_key,
        )?
        .into_ref();

        // create sector manager and index manager
        let sec_mgr = SectorMgr::new(&local_cache);
        let idx_mgr = IndexMgr::new(
            Box::new(IndexAccessor::<Lsmt>::new(&local_cache)),
            Box::new(IndexAccessor::<MemTab>::new(&local_cache)),
            Box::new(IndexAccessor::<Tab>::new(&local_cache)),
        );

        Ok(ZboxStorage {
            wal_base: PathBuf::from(Self::WAL_DIR),
            local_cache,
            sec_mgr,
            idx_mgr,
        })
    }

    // set crypto context for componenets
    fn set_crypto_ctx(&mut self, crypto: Crypto, key: Key) {
        {
            let subkey = key.derive(Self::SUBKEY_ID_LOCAL_CACHE);
            let mut local_cache = self.local_cache.write().unwrap();
            local_cache.set_crypto_ctx(crypto.clone(), subkey);
        }

        let subkey = key.derive(Self::SUBKEY_ID_SEC_MGR);
        self.sec_mgr.set_crypto_ctx(crypto.clone(), subkey);

        let subkey = key.derive(Self::SUBKEY_ID_IDX_MGR);
        self.idx_mgr.set_crypto_ctx(crypto.clone(), subkey);
    }
}

impl Storable for ZboxStorage {
    #[inline]
    fn exists(&self) -> Result<bool> {
        let local_cache = self.local_cache.read().unwrap();
        local_cache.repo_exists()
    }

    #[inline]
    fn connect(&mut self, force: bool) -> Result<()> {
        let mut local_cache = self.local_cache.write().unwrap();
        local_cache.connect(force)
    }

    fn init(&mut self, crypto: Crypto, key: Key) -> Result<()> {
        self.set_crypto_ctx(crypto, key);
        {
            let mut local_cache = self.local_cache.write().unwrap();
            local_cache.init()?;
        }
        self.sec_mgr.init()?;
        self.idx_mgr.init()?;
        Ok(())
    }

    fn open(&mut self, crypto: Crypto, key: Key, _force: bool) -> Result<()> {
        self.set_crypto_ctx(crypto, key);
        {
            let mut local_cache = self.local_cache.write().unwrap();
            local_cache.open()?;
        }
        self.sec_mgr.open()?;
        self.idx_mgr.open()?;
        Ok(())
    }

    fn get_super_block(&mut self, suffix: u64) -> Result<Vec<u8>> {
        let rel_path =
            Path::new(Self::SUPER_BLK_STEM).with_extension(suffix.to_string());
        let mut local_cache = self.local_cache.write().unwrap();
        local_cache.get(&rel_path)
    }

    fn put_super_block(&mut self, super_blk: &[u8], suffix: u64) -> Result<()> {
        let rel_path =
            Path::new(Self::SUPER_BLK_STEM).with_extension(&suffix.to_string());
        let mut local_cache = self.local_cache.write().unwrap();
        local_cache.put_pinned(&rel_path, super_blk)?;
        local_cache.flush()
    }

    #[inline]
    fn get_wal(&mut self, id: &Eid) -> Result<Vec<u8>> {
        let rel_path = id.to_path_buf(&self.wal_base);
        let mut local_cache = self.local_cache.write().unwrap();
        local_cache.get(&rel_path)
    }

    #[inline]
    fn put_wal(&mut self, id: &Eid, wal: &[u8]) -> Result<()> {
        let rel_path = id.to_path_buf(&self.wal_base);
        let mut local_cache = self.local_cache.write().unwrap();
        local_cache.put_pinned(&rel_path, wal)
    }

    #[inline]
    fn del_wal(&mut self, id: &Eid) -> Result<()> {
        let rel_path = id.to_path_buf(&self.wal_base);
        let mut local_cache = self.local_cache.write().unwrap();
        local_cache.del(&rel_path)
    }

    #[inline]
    fn get_address(&mut self, id: &Eid) -> Result<Vec<u8>> {
        self.idx_mgr.get(id)
    }

    #[inline]
    fn put_address(&mut self, id: &Eid, addr: &[u8]) -> Result<()> {
        self.idx_mgr.insert(id, addr)
    }

    #[inline]
    fn del_address(&mut self, id: &Eid) -> Result<()> {
        self.idx_mgr.delete(id)
    }

    #[inline]
    fn get_blocks(&mut self, dst: &mut [u8], span: Span) -> Result<()> {
        assert_eq!(dst.len(), span.bytes_len());
        self.sec_mgr.get_blocks(dst, span)
    }

    #[inline]
    fn put_blocks(&mut self, span: Span, blks: &[u8]) -> Result<()> {
        assert_eq!(blks.len(), span.bytes_len());
        self.sec_mgr.put_blocks(span, blks)
    }

    #[inline]
    fn del_blocks(&mut self, span: Span) -> Result<()> {
        self.sec_mgr.del_blocks(span)
    }

    #[inline]
    fn flush(&mut self) -> Result<()> {
        self.sec_mgr.flush()?;
        self.idx_mgr.flush()?;
        let mut local_cache = self.local_cache.write().unwrap();
        local_cache.flush()
    }

    #[inline]
    fn destroy(&mut self) -> Result<()> {
        let mut local_cache = self.local_cache.write().unwrap();
        local_cache.destroy_repo()
    }
}

impl Debug for ZboxStorage {
    #[inline]
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("ZboxStorage").finish()
    }
}

impl IntoRef for ZboxStorage {}

#[cfg(test)]
mod tests {
    extern crate tempdir;

    use self::tempdir::TempDir;

    use super::*;
    use base::init_env;
    use volume::BLK_SIZE;

    #[test]
    fn zbox_parse_uri() {
        assert_eq!(parse_uri("").unwrap_err(), Error::InvalidUri);
        assert_eq!(parse_uri("abcd").unwrap_err(), Error::InvalidUri);
        assert_eq!(parse_uri("中文").unwrap_err(), Error::InvalidUri);
        assert_eq!(parse_uri("//").unwrap_err(), Error::InvalidUri);
        assert_eq!(parse_uri("zbox://").unwrap_err(), Error::InvalidUri);
        assert_eq!(parse_uri("zbox://foo").unwrap_err(), Error::InvalidUri);
        assert_eq!(parse_uri("zbox://foo@").unwrap_err(), Error::InvalidUri);
        assert!(parse_uri("zbox://foo@bar").is_ok());
        assert!(parse_uri("zbox://foo@bar?").is_ok());
    }

    fn do_test(uri: &str) {
        init_env();
        let mut zs = ZboxStorage::new(uri).unwrap();
        zs.connect().unwrap();
        zs.init(Crypto::default(), Key::new_empty()).unwrap();

        let id = Eid::new();
        let buf = vec![1, 2, 3];
        let blks = vec![42u8; BLK_SIZE * 3];
        let mut dst = vec![0u8; BLK_SIZE * 3];

        // super block
        zs.put_super_block(&buf, 0).unwrap();
        let s = zs.get_super_block(0).unwrap();
        assert_eq!(&s[..], &buf[..]);

        // wal
        zs.put_wal(&id, &buf).unwrap();
        let s = zs.get_wal(&id).unwrap();
        assert_eq!(&s[..], &buf[..]);
        zs.del_wal(&id).unwrap();
        assert_eq!(zs.get_wal(&id).unwrap_err(), Error::NotFound);

        // address
        zs.put_address(&id, &buf).unwrap();
        let s = zs.get_address(&id).unwrap();
        assert_eq!(&s[..], &buf[..]);
        zs.del_address(&id).unwrap();
        assert_eq!(zs.get_address(&id).unwrap_err(), Error::NotFound);

        // block
        let span = Span::new(0, 3);
        zs.put_blocks(span, &blks).unwrap();
        zs.get_blocks(&mut dst, span).unwrap();
        assert_eq!(&dst[..], &blks[..]);
        zs.del_blocks(Span::new(1, 2)).unwrap();
        assert_eq!(zs.get_blocks(&mut dst, span).unwrap_err(), Error::NotFound);
        assert_eq!(
            zs.get_blocks(&mut dst[..BLK_SIZE], Span::new(1, 1))
                .unwrap_err(),
            Error::NotFound
        );
        assert_eq!(
            zs.get_blocks(&mut dst[..BLK_SIZE], Span::new(2, 1))
                .unwrap_err(),
            Error::NotFound
        );
        zs.flush().unwrap();

        // re-open
        drop(zs);
        let mut zs = ZboxStorage::new(uri).unwrap();
        zs.connect().unwrap();
        zs.open(Crypto::default(), Key::new_empty()).unwrap();

        zs.get_blocks(&mut dst[..BLK_SIZE], Span::new(0, 1))
            .unwrap();
        assert_eq!(&dst[..BLK_SIZE], &blks[..BLK_SIZE]);
        assert_eq!(
            zs.get_blocks(&mut dst[..BLK_SIZE], Span::new(1, 1))
                .unwrap_err(),
            Error::NotFound
        );
        assert_eq!(
            zs.get_blocks(&mut dst[..BLK_SIZE], Span::new(2, 1))
                .unwrap_err(),
            Error::NotFound
        );
    }

    #[test]
    fn zbox_storage_mem() {
        do_test("accessKey456@repo456?cache_type=mem&cache_size=1mb");
    }

    #[test]
    fn zbox_storage_file() {
        let tmpdir = TempDir::new("zbox_test").expect("Create temp dir failed");
        let base = tmpdir.path().to_path_buf();
        /*if base.exists() {*/
        /*std::fs::remove_dir_all(&base).unwrap();*/
        /*}*/
        let uri = format!(
            "accessKey456@repo456?cache_type=file&cache_size=1mb&base={}",
            base.display()
        );
        do_test(&uri);
    }
}
