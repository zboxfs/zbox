use std::collections::HashMap;
use std::convert::AsRef;
use std::fmt::{self, Debug, Display};
use std::path::{Path, PathBuf};

use http::header::{self, HeaderMap, HeaderName, HeaderValue};
use http::{Error as HttpError, HttpTryFrom, StatusCode, Uri};

use super::transport::{DummyTransport, Response, Transport};
use base::Version;
use error::{Error, Result};

// remote object cache control
#[derive(Clone, Copy)]
pub enum CacheControl {
    Long, // cache for 1 year
    NoCache,
}

impl CacheControl {
    // cache object max age, pushed to S3 object's Cache-Control header
    // 1 year, 31536000 seconds
    const MAX_AGE: u64 = 31_536_000;

    #[inline]
    fn to_header_value(self) -> HeaderValue {
        HeaderValue::from_str(&self.to_string()).unwrap()
    }
}

impl From<bool> for CacheControl {
    #[inline]
    fn from(is_pinned: bool) -> Self {
        if is_pinned {
            CacheControl::NoCache
        } else {
            CacheControl::Long
        }
    }
}

impl Display for CacheControl {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            CacheControl::Long => write!(f, "max-age={}", Self::MAX_AGE),
            CacheControl::NoCache => write!(f, "no-cache"),
        }
    }
}

// check repo existsresponse
#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct RepoExistsResp {
    result: bool,
}

// remote session open response
#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct SessionOpenResp {
    _status: String,
    session_token: String,
    update_seq: u64,
    ttl: u64,
}

// http headers
#[derive(Clone)]
struct Headers {
    map: HeaderMap,
}

impl Headers {
    fn new() -> Self {
        let mut map = HeaderMap::new();
        let ver = Version::lib_version().to_string();

        // add zbox version header
        let version_header = HeaderName::from_static("zbox-version");
        let version_value = HeaderValue::from_str(&ver).unwrap();
        map.insert(version_header, version_value);

        // set headers for non-browser request only, because some browsers will
        // not allow us to do that
        #[cfg(not(feature = "storage-zbox-wasm"))]
        {
            map.insert(
                header::USER_AGENT,
                HeaderValue::from_str(&format!("zboxfs/{}", ver)).unwrap(),
            );

            map.insert(
                header::ORIGIN,
                HeaderValue::from_str("http://localhost").unwrap(),
            );
        }

        Headers { map }
    }

    fn bearer_auth(mut self, auth_key: &str) -> Self {
        let value_str = format!("Bearer {}", auth_key);
        let value = HeaderValue::from_str(&value_str).unwrap();
        self.map.insert(header::AUTHORIZATION, value);
        self
    }

    #[inline]
    fn cache_control(mut self, cache_ctl: CacheControl) -> Self {
        self.map
            .insert(header::CACHE_CONTROL, cache_ctl.to_header_value());
        self
    }

    #[inline]
    fn json(mut self) -> Self {
        self.map.insert(
            header::CONTENT_TYPE,
            HeaderValue::from_static("application/json"),
        );
        self
    }

    fn put_range(mut self, begin: usize, end: usize) -> Self {
        let header = HeaderName::from_static("zbox-range");
        let value =
            HeaderValue::from_str(&format!("{}-{}", begin, end)).unwrap();
        self.map.insert(header, value);
        self
    }
}

impl AsRef<HeaderMap> for Headers {
    #[inline]
    fn as_ref(&self) -> &HeaderMap {
        &self.map
    }
}

/// Zbox storage HTTP client
pub struct HttpClient {
    base_url: String,
    access_key: String,
    session_token: String,
    is_updated: bool,
    update_seq: u64,
    ttl: u64,
    retry_cnt: u64,
    headers: Headers,
    transport: Box<Transport>,
    del_bulk: Vec<PathBuf>,
}

impl HttpClient {
    // remote root url
    const ROOT_URL: &'static str = "https://data.zbox.io/";

    // bulk request uri
    const BULK_URI: &'static str = "bulk";

    // default timeout, in secnods
    const DEFAULT_TIMEOUT: u32 = 30;

    pub fn new(repo_id: &str, access_key: &str) -> Result<Self> {
        // create transport
        let transport: Box<Transport> = {
            #[cfg(feature = "storage-zbox-faulty")]
            {
                Box::new(super::transport::faulty::FaultyTransport::new(
                    Self::DEFAULT_TIMEOUT,
                )?)
            }

            #[cfg(feature = "storage-zbox-native")]
            {
                Box::new(super::transport::native::NativeTransport::new(
                    Self::DEFAULT_TIMEOUT,
                )?)
            }

            #[cfg(feature = "storage-zbox-jni")]
            {
                Box::new(super::transport::jni::JniTransport::new(
                    Self::DEFAULT_TIMEOUT,
                )?)
            }

            #[cfg(feature = "storage-zbox-wasm")]
            {
                Box::new(super::transport::wasm::WasmTransport::new(
                    Self::DEFAULT_TIMEOUT,
                )?)
            }
        };

        Ok(HttpClient {
            base_url: Self::ROOT_URL.to_owned() + repo_id + "/",
            access_key: access_key.to_string(),
            session_token: String::new(),
            is_updated: false,
            update_seq: 0,
            ttl: 0,
            retry_cnt: 0,
            headers: Headers::new(),
            transport,
            del_bulk: Vec::new(),
        })
    }

    #[inline]
    pub fn get_update_seq(&self) -> u64 {
        self.update_seq
    }

    // set session is updated
    #[inline]
    fn set_updated(&mut self) {
        if !self.is_updated {
            self.is_updated = true;
            self.update_seq += 1;
        }
    }

    #[inline]
    fn make_uri<P: AsRef<Path>>(&self, rel_path: P) -> Result<Uri> {
        let s = self.base_url.clone() + rel_path.as_ref().to_str().unwrap();
        HttpTryFrom::try_from(s)
            .map_err(HttpError::from)
            .map_err(Error::from)
    }

    // check if repo exists
    pub fn repo_exists(&self) -> Result<bool> {
        debug!("check repo exists");

        let uri = self.make_uri("exists")?;
        let headers = self.headers.clone().bearer_auth(&self.access_key);
        let mut resp = self
            .transport
            .get(&uri, headers.as_ref())?
            .error_for_status()?;
        let result: RepoExistsResp = resp.as_json()?;
        Ok(result.result)
    }

    // open remote session, return remote update sequence
    pub fn open_session(&mut self) -> Result<u64> {
        if self.retry_cnt == 0 {
            debug!("open session 1st time");
        } else {
            debug!("open session, retry {}", self.retry_cnt);
        }

        let uri = self.make_uri("open")?;
        let headers = self
            .headers
            .clone()
            .bearer_auth(&self.access_key)
            .cache_control(CacheControl::NoCache);
        let mut resp = self
            .transport
            .get(&uri, headers.as_ref())?
            .error_for_status()?;
        let result: SessionOpenResp = resp.as_json()?;

        // if we're re-opening session, but the local update sequence is not
        // equal to remote update sequence, this could happen when remote
        // session is expired and repo is updated in another session.
        if self.update_seq > 0 && result.update_seq != self.update_seq {
            return Err(Error::NotInSync);
        }

        // save session status
        self.session_token = result.session_token.clone();
        self.update_seq = result.update_seq;
        self.ttl = result.ttl;
        self.retry_cnt += 1;

        debug!(
            "session opened, update seq {}, ttl {}",
            self.update_seq, self.ttl
        );

        Ok(self.update_seq)
    }

    // send get request
    fn send_get_req(
        &mut self,
        uri: &Uri,
        cache_ctl: CacheControl,
    ) -> Result<Response> {
        let headers = self
            .headers
            .clone()
            .bearer_auth(&self.session_token)
            .cache_control(cache_ctl);
        self.transport
            .get(uri, headers.as_ref())?
            .error_for_status()
            .map_err(|err| {
                if err == Error::HttpStatus(StatusCode::NOT_FOUND) {
                    Error::NotFound
                } else {
                    err
                }
            })
    }

    fn get_response(
        &mut self,
        rel_path: &Path,
        cache_ctl: CacheControl,
    ) -> Result<Response> {
        trace!("get {:?}", rel_path);

        let uri = self.make_uri(rel_path)?;

        match self.send_get_req(&uri, cache_ctl) {
            Ok(resp) => Ok(resp),
            Err(err) => {
                // this could happen if remote time and local time isn't in
                // sync, so if we got 401 unauthorized error, that means
                // it is not expired locally but expired remotely, in this case
                // we need to reopen the session, but just try once only
                if err == Error::HttpStatus(StatusCode::UNAUTHORIZED) {
                    self.open_session()?;
                    self.send_get_req(&uri, cache_ctl)
                } else {
                    Err(err)
                }
            }
        }
    }

    pub fn get(
        &mut self,
        rel_path: &Path,
        cache_ctl: CacheControl,
    ) -> Result<Vec<u8>> {
        // if the object is already in deletion bulk
        if self.del_bulk.iter().any(|p| p == rel_path) {
            return Err(Error::NotFound);
        }

        let mut resp = self.get_response(rel_path, cache_ctl)?;
        let mut buf: Vec<u8> = Vec::new();
        resp.copy_to(&mut buf)?;
        Ok(buf)
    }

    // send put request
    fn send_put_req(
        &mut self,
        uri: &Uri,
        offset: usize,
        cache_ctl: CacheControl,
        body: &[u8],
    ) -> Result<()> {
        let headers = self
            .headers
            .clone()
            .bearer_auth(&self.session_token)
            .cache_control(cache_ctl)
            .put_range(offset, offset + body.len() - 1);
        self.transport
            .put(uri, headers.as_ref(), body)?
            .error_for_status()
            .map(|_| ())
    }

    pub fn put(
        &mut self,
        rel_path: &Path,
        offset: usize,
        cache_ctl: CacheControl,
        body: &[u8],
    ) -> Result<()> {
        trace!("put {:?}, offset {}, len {}", rel_path, offset, body.len());

        // if the object is already in deletion bulk, we need to remove it
        if let Some(idx) = self.del_bulk.iter().position(|p| p == rel_path) {
            self.del_bulk.remove(idx);
        }

        let uri = self.make_uri(rel_path)?;

        self.send_put_req(&uri, offset, cache_ctl, body)
            .or_else(|err| {
                // try reopen remote session once if it is expired
                if err == Error::HttpStatus(StatusCode::UNAUTHORIZED) {
                    self.open_session()?;
                    self.send_put_req(&uri, offset, cache_ctl, body)
                } else {
                    Err(err)
                }
            })?;

        self.set_updated();

        Ok(())
    }

    #[inline]
    pub fn del(&mut self, rel_path: &Path) -> Result<()> {
        self.del_bulk.push(rel_path.to_path_buf());
        self.set_updated();
        Ok(())
    }

    // send bulk delelte
    fn send_bulk_del_req(&mut self, bulk: &[PathBuf]) -> Result<()> {
        if bulk.is_empty() {
            return Ok(());
        }

        trace!("bulk del {:?} objects", bulk.len());

        // make bulk deletion uri
        let uri = self.make_uri(Self::BULK_URI)?;

        // serialize body as json bytes
        let mut map = HashMap::new();
        map.insert("paths".to_owned(), bulk.to_vec());
        let buf = serde_json::to_vec(&map)?;

        let headers = self
            .headers
            .clone()
            .bearer_auth(&self.session_token)
            .cache_control(CacheControl::NoCache)
            .json();
        self.transport
            .delete_bulk(&uri, headers.as_ref(), &buf)?
            .error_for_status()
            .map(|_| ())
            .or_else(|err| {
                // ignore not found error
                if err == Error::HttpStatus(StatusCode::NOT_FOUND) {
                    Ok(())
                } else {
                    Err(err)
                }
            })?;

        Ok(())
    }

    #[inline]
    pub fn flush(&mut self) -> Result<()> {
        let bulk = self.del_bulk.clone();
        self.send_bulk_del_req(&bulk).or_else(|err| {
            // try reopen remote session once if it is expired
            if err == Error::HttpStatus(StatusCode::UNAUTHORIZED) {
                self.open_session()?;
                self.send_bulk_del_req(&bulk)
            } else {
                Err(err)
            }
        })?;
        self.del_bulk.clear();
        Ok(())
    }
}

impl Debug for HttpClient {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("HttpClient")
            .field("base_url", &self.base_url)
            .field("access_key", &self.access_key)
            .field("session_token", &self.session_token)
            .field("is_updated", &self.is_updated)
            .field("update_seq", &self.update_seq)
            .field("ttl", &self.ttl)
            .field("retry_cnt", &self.retry_cnt)
            .field("del_bulk", &self.del_bulk)
            .finish()
    }
}

impl Default for HttpClient {
    fn default() -> Self {
        HttpClient {
            base_url: Self::ROOT_URL.to_owned() + "/",
            access_key: String::new(),
            session_token: String::new(),
            is_updated: false,
            update_seq: 0,
            ttl: 0,
            retry_cnt: 0,
            headers: Headers::new(),
            transport: Box::new(DummyTransport),
            del_bulk: Vec::new(),
        }
    }
}

impl Drop for HttpClient {
    fn drop(&mut self) {
        if self.session_token.is_empty() {
            return;
        }

        // send close requests to remote
        let uri = self.make_uri("close").unwrap();
        let headers = self.headers.clone().bearer_auth(&self.session_token);
        if self.transport.get(&uri, headers.as_ref()).is_err() {
            warn!("close session failed");
        } else {
            debug!("session closed");
        };
    }
}

#[cfg(test)]
mod tests {

    use std::{thread, time};

    use super::*;
    use base::init_env;
    use volume::BLK_SIZE;

    #[test]
    fn http_test() {
        init_env();

        let repo_id = "repo456";
        let access_key = "accessKey456";
        let mut client = HttpClient::new(&repo_id, &access_key).unwrap();
        let blks = vec![42u8; BLK_SIZE];

        // test open session
        let update_seq = client.open_session().unwrap();

        // test put/get
        let rel_path = Path::new("data/xx/yy/test");
        client
            .put(&rel_path, 0, CacheControl::NoCache, &blks[..])
            .unwrap();
        let dst = client.get(&rel_path, CacheControl::NoCache).unwrap();
        assert_eq!(dst.len(), blks.len());
        assert_eq!(&dst[..], &blks[..]);

        // test partial put
        let rel_path2 = Path::new("data/xx/yy/test2");
        client
            .put(&rel_path2, 0, CacheControl::Long, &blks[..])
            .unwrap();
        client
            .put(&rel_path2, 3, CacheControl::Long, &blks[..])
            .unwrap();
        let dst = client.get(&rel_path2, CacheControl::Long).unwrap();
        assert_eq!(dst.len(), blks.len() + 3);

        // open session again should fail
        assert_eq!(client.open_session().unwrap_err(), Error::Opened);

        // test delete
        client.del(&rel_path).unwrap();

        client.flush().unwrap();

        // close session and open it again
        drop(client);
        let mut client = HttpClient::new(&repo_id, &access_key).unwrap();
        let new_update_seq = client.open_session().unwrap();
        assert_eq!(new_update_seq, update_seq + 1);
    }

    #[test]
    #[ignore]
    fn retry_test() {
        init_env();

        let repo_id = "repo456";
        let access_key = "accessKey456";
        let mut client = HttpClient::new(&repo_id, &access_key).unwrap();
        let blks = vec![42u8; BLK_SIZE];
        let delay = time::Duration::from_secs(180);

        client.open_session().unwrap();
        let rel_path = Path::new("test");
        client
            .put(&rel_path, 0, CacheControl::NoCache, &blks[..])
            .unwrap();

        for _ in 0..3 {
            client
                .put(&rel_path, 0, CacheControl::NoCache, &blks[..])
                .unwrap();
            thread::sleep(delay);
        }
    }
}
