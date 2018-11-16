use std::path::{Path, PathBuf};

use super::local_cache::{CacheType, LocalCache};
use super::sector::SectorMgr;
use base::crypto::{Crypto, Key};
use base::IntoRef;
use error::{Error, Result};
use trans::Eid;
use volume::address::Span;
use volume::storage::Storable;

// parse uri
// example: access_key@repo_id?cache_type=mem&cache_size=2[&base=path]
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

    // parse repo id, required
    idx = uri.find('?').ok_or(Error::InvalidUri)?;
    let repo_id = &uri[..idx];
    uri = &uri[idx + 1..];

    // set default value for parameters
    let mut cache_type: Option<CacheType> = Some(CacheType::Mem);
    let mut cache_size: Option<usize> = Some(1);
    let mut base: Option<PathBuf> = None;

    // parse parameters
    for param in uri.split('&') {
        idx = param.find('=').ok_or(Error::InvalidUri)?;
        let key = &param[..idx];
        let value = &param[idx + 1..];

        if key == "cache_type" {
            let ctype = value.parse::<CacheType>()?;
            cache_type = Some(ctype);
        } else if key == "cache_size" {
            let size = value.parse::<usize>().map_err(|_| Error::InvalidUri)?;
            if size < 1 {
                // cache size must >= 1MB
                return Err(Error::InvalidUri);
            }
            cache_size = Some(size);
        } else if key == "base" {
            base = Some(PathBuf::from(value));
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
        base.unwrap_or(PathBuf::from("")),
    ))
}

/// Zbox Storage
#[derive(Debug)]
pub struct ZboxStorage {
    local_cache: LocalCache,
    sec_mgr: SectorMgr,
}

impl ZboxStorage {
    // subkey ids
    const SUBKEY_ID_LOCAL_CACHE: u64 = 42;
    const SUBKEY_ID_SEC_MGR: u64 = 43;

    // super block file name stem
    const SUPER_BLK_STEM: &'static str = "super_blk";

    // create zbox storage
    pub fn new(uri: &str) -> Result<Self> {
        // parse uri string
        let (access_key, repo_id, cache_type, cache_size, base) =
            parse_uri(uri)?;

        // create local cache
        let local_cache = LocalCache::new(
            cache_type, cache_size, &base, repo_id, access_key,
        )?;

        // create sector manager
        let sec_mgr = SectorMgr::new();

        Ok(ZboxStorage {
            local_cache,
            sec_mgr,
        })
    }

    // set crypto context for componenets
    fn set_crypto_ctx(&mut self, crypto: Crypto, key: Key) {
        let subkey = key.derive(Self::SUBKEY_ID_LOCAL_CACHE);
        self.local_cache.set_crypto_ctx(crypto.clone(), subkey);

        let subkey = key.derive(Self::SUBKEY_ID_SEC_MGR);
        self.sec_mgr.set_crypto_ctx(crypto.clone(), subkey);
    }
}

impl Storable for ZboxStorage {
    #[inline]
    fn exists(&self) -> Result<bool> {
        self.local_cache.repo_exists()
    }

    fn init(&mut self, crypto: Crypto, key: Key) -> Result<()> {
        self.set_crypto_ctx(crypto, key);
        self.local_cache.init()?;
        Ok(())
    }

    fn open(&mut self, crypto: Crypto, key: Key) -> Result<()> {
        self.set_crypto_ctx(crypto, key);
        self.local_cache.open()?;
        self.sec_mgr.open(&mut self.local_cache)
    }

    fn get_super_block(&mut self, suffix: u64) -> Result<Vec<u8>> {
        let rel_path =
            Path::new(Self::SUPER_BLK_STEM).with_extension(suffix.to_string());
        self.local_cache.get_pinned(&rel_path)
    }

    fn put_super_block(&mut self, super_blk: &[u8], suffix: u64) -> Result<()> {
        let rel_path =
            Path::new(Self::SUPER_BLK_STEM).with_extension(&suffix.to_string());
        self.local_cache.put_pinned(&rel_path, super_blk)
    }

    #[inline]
    fn get_address(&mut self, id: &Eid) -> Result<Vec<u8>> {
        self.local_cache.get_address(id)
    }

    #[inline]
    fn put_address(&mut self, id: &Eid, addr: &[u8]) -> Result<()> {
        self.local_cache.put_address(id, addr)
    }

    #[inline]
    fn del_address(&mut self, id: &Eid) -> Result<()> {
        self.local_cache.del_address(id)
    }

    #[inline]
    fn get_blocks(&mut self, dst: &mut [u8], span: Span) -> Result<()> {
        assert_eq!(dst.len(), span.bytes_len());
        self.sec_mgr.get_blocks(dst, span, &mut self.local_cache)
    }

    #[inline]
    fn put_blocks(&mut self, span: Span, blks: &[u8]) -> Result<()> {
        assert_eq!(blks.len(), span.bytes_len());
        self.sec_mgr.put_blocks(span, blks, &mut self.local_cache)
    }

    #[inline]
    fn del_blocks(&mut self, span: Span) -> Result<()> {
        self.sec_mgr.del_blocks(span, &mut self.local_cache)
    }

    fn flush(&mut self) -> Result<()> {
        self.sec_mgr.flush(&mut self.local_cache)?;
        self.local_cache.flush()
    }
}

impl IntoRef for ZboxStorage {}

#[cfg(test)]
mod tests {

    use super::*;
    use base::init_env;
    use volume::BLK_SIZE;

    fn do_test(uri: &str) {
        init_env();
        let mut zs = ZboxStorage::new(uri).unwrap();
        zs.init(Crypto::default(), Key::new_empty()).unwrap();

        let id = Eid::new();
        let buf = vec![1, 2, 3];
        let blks = vec![42u8; BLK_SIZE * 3];
        let mut dst = vec![0u8; BLK_SIZE * 3];

        // super block
        zs.put_super_block(&buf, 0).unwrap();
        let s = zs.get_super_block(0).unwrap();
        assert_eq!(&s[..], &buf[..]);

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
        do_test("accessKey456@repo456?cache_type=mem&cache_size=1");
    }

    #[test]
    fn zbox_storage_file() {
        do_test("accessKey456@repo456?cache_type=file&cache_size=1&base=./tt");
    }
}
