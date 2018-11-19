use std::fmt::{self, Debug};
use std::io::Write;
use std::path::Path;
use std::sync::{Arc, RwLock};
use std::time::Duration;

use reqwest::header::{self, HeaderMap, HeaderName, HeaderValue};
use reqwest::{self, Client, Response, StatusCode};

use base::{IntoRef, Version};
use error::{Error, Result};

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
    _ttl: u64,
}

/// HTTP client
pub struct HttpClient {
    base_url: String,
    access_key: String,
    session_token: String,
    is_updated: bool,
    update_seq: u64,
    client: Client,
}

impl HttpClient {
    // remote root url
    const ROOT_URL: &'static str = "https://data.zbox.io/";

    // default timeout, in secnods
    const DEFAULT_TIMEOUT: u64 = 30;

    // cache object max age, pushed to S3 object's Cache-Control header
    // 1 year, 31536000 seconds
    const MAX_AGE: &'static str = "max-age=31536000";

    pub fn new(repo_id: &str, access_key: &str) -> Result<Self> {
        // default headers applied to all requests
        let mut headers = HeaderMap::new();
        let version_header = HeaderName::from_static("zbox-version");
        let version_value =
            HeaderValue::from_str(&Version::current_lib_version().to_string())
                .unwrap();
        headers.insert(version_header, version_value);

        // create http client
        let client = Client::builder()
            .default_headers(headers)
            .timeout(Duration::from_secs(Self::DEFAULT_TIMEOUT))
            .build()?;

        Ok(HttpClient {
            base_url: Self::ROOT_URL.to_owned() + repo_id + "/",
            access_key: access_key.to_string(),
            session_token: String::new(),
            is_updated: false,
            update_seq: 0,
            client,
        })
    }

    #[inline]
    pub fn get_update_seq(&self) -> u64 {
        self.update_seq
    }

    // check if repo exists
    pub fn repo_exists(&self) -> Result<bool> {
        let url = self.base_url.clone() + "exists";
        let mut resp = self
            .client
            .get(&url)
            .bearer_auth(&self.access_key)
            .send()?
            .error_for_status()?;
        let result: RepoExistsResp = resp.json()?;
        Ok(result.result)
    }

    // open remote session
    pub fn open_session(&mut self) -> Result<u64> {
        let url = self.base_url.clone() + "open";
        let mut resp = self
            .client
            .get(&url)
            .bearer_auth(&self.access_key)
            .send()?
            .error_for_status()
            .map_err(|err| {
                // 409 conflict error means remote session is already opened
                if err.status() == Some(StatusCode::CONFLICT) {
                    Error::Opened
                } else {
                    Error::from(err)
                }
            })?;
        let result: SessionOpenResp = resp.json()?;

        // if we're re-opening session, but the local update sequence is not
        // equal to remote update sequence, this could happen when remote
        // session is expired and repo is updated in another session.
        if self.update_seq > 0 && result.update_seq != self.update_seq {
            return Err(Error::NotInSync);
        }

        // save session status
        self.session_token = result.session_token;
        self.update_seq = result.update_seq;

        debug!("session opened, update seq {}", result.update_seq);

        Ok(self.update_seq)
    }

    // send get request
    #[inline]
    fn send_get_req(&self, url: &str) -> reqwest::Result<Response> {
        self.client
            .get(url)
            .bearer_auth(&self.session_token)
            .send()?
            .error_for_status()
    }

    fn get_response(&mut self, rel_path: &Path) -> Result<Response> {
        debug!("get {:?}", rel_path);

        let url = self.base_url.clone() + rel_path.to_str().unwrap();

        // translate 404 error to NotFound error
        let translate_err = |err: reqwest::Error| -> Error {
            if err.status() == Some(StatusCode::NOT_FOUND) {
                Error::NotFound
            } else {
                Error::from(err)
            }
        };

        match self.send_get_req(&url) {
            Ok(resp) => Ok(resp),
            Err(err) => {
                // this could happen if remote time and local time isn't in
                // sync, so if we got 401 unauthorized error, that means
                // it is not expired locally but expired remotely, in this case
                // we need to reopen the session, but just try once only
                if err.status() == Some(StatusCode::UNAUTHORIZED) {
                    self.open_session()?;
                    let mut resp =
                        self.send_get_req(&url).map_err(translate_err)?;
                    Ok(resp)
                } else {
                    Err(translate_err(err))
                }
            }
        }
    }

    pub fn get(&mut self, rel_path: &Path) -> Result<Vec<u8>> {
        let mut resp = self.get_response(rel_path)?;
        let mut buf: Vec<u8> = Vec::new();
        resp.copy_to(&mut buf)?;
        Ok(buf)
    }

    pub fn get_to<W: Write>(
        &mut self,
        rel_path: &Path,
        w: &mut W,
    ) -> Result<usize> {
        let mut resp = self.get_response(rel_path)?;
        let copied = resp.copy_to(w)?;
        Ok(copied as usize)
    }

    // set session is updated
    #[inline]
    fn set_updated(&mut self) {
        if !self.is_updated {
            self.is_updated = true;
            self.update_seq += 1;
        }
    }

    // send put request
    fn send_put_req(
        &self,
        url: &str,
        offset: usize,
        body: &[u8],
    ) -> reqwest::Result<()> {
        let offset_header = HeaderName::from_static("zbox-offset");
        let offset_value =
            HeaderValue::from_str(&format!("{}", offset)).unwrap();
        let max_age_value = HeaderValue::from_str(Self::MAX_AGE).unwrap();

        self.client
            .put(url)
            .bearer_auth(&self.session_token)
            .header(offset_header, offset_value)
            .header(header::CACHE_CONTROL, max_age_value)
            .body(body.to_owned())
            .send()?
            .error_for_status()
            .map(|_| ())
    }

    pub fn put(
        &mut self,
        rel_path: &Path,
        offset: usize,
        body: &[u8],
    ) -> Result<()> {
        debug!(
            "put {:?}, offset: {}, body_len: {}",
            rel_path,
            offset,
            body.len()
        );

        let url = self.base_url.clone() + rel_path.to_str().unwrap();

        self.send_put_req(&url, offset, body).or_else(|err| {
            // try reopen remote session once if it is expired
            if err.status() == Some(StatusCode::UNAUTHORIZED) {
                self.open_session()?;
                self.send_put_req(&url, offset, body)?;
                Ok(())
            } else {
                Err(Error::from(err))
            }
        })?;

        self.set_updated();

        Ok(())
    }

    // send del request
    #[inline]
    fn send_del_req(&self, url: &str) -> reqwest::Result<()> {
        self.client
            .delete(url)
            .bearer_auth(&self.session_token)
            .send()?
            .error_for_status()
            .map(|_| ())
    }

    pub fn del(&mut self, rel_path: &Path) -> Result<()> {
        debug!("del {:?}", rel_path);

        let url = self.base_url.clone() + rel_path.to_str().unwrap();

        // ignore not found error
        let ignore_not_found = |err: reqwest::Error| -> Result<()> {
            if err.status() == Some(StatusCode::NOT_FOUND) {
                Ok(())
            } else {
                Err(Error::from(err))
            }
        };

        self.send_del_req(&url).or_else(|err| {
            // try reopen remote session once if it is expired
            if err.status() == Some(StatusCode::UNAUTHORIZED) {
                self.open_session()?;
                self.send_del_req(&url).or_else(ignore_not_found)
            } else {
                ignore_not_found(err)
            }
        })?;

        self.set_updated();

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
            client: Client::builder().build().unwrap(),
        }
    }
}

impl Drop for HttpClient {
    fn drop(&mut self) {
        if self.session_token.is_empty() {
            return;
        }

        // send close requests and ignore result
        let url = self.base_url.clone() + "close";
        match self
            .client
            .get(&url)
            .bearer_auth(&self.session_token)
            .send()
        {
            Ok(result) => match result.error_for_status() {
                Ok(_) => debug!("session closed"),
                Err(_) => warn!("close session failed"),
            },
            Err(_) => warn!("close session failed"),
        }
    }
}

impl IntoRef for HttpClient {}

pub type HttpClientRef = Arc<RwLock<HttpClient>>;

#[cfg(test)]
mod tests {

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
        client.put(&rel_path, 0, &blks[..]).unwrap();
        let dst = client.get(&rel_path).unwrap();
        assert_eq!(dst.len(), blks.len());
        assert_eq!(&dst[..], &blks[..]);

        // test partial put
        client.put(&rel_path, 3, &blks[..]).unwrap();
        let dst = client.get(&rel_path).unwrap();
        assert_eq!(dst.len(), blks.len() + 3);

        // open session again should fail
        assert_eq!(client.open_session().unwrap_err(), Error::Opened);

        // test delete
        client.del(&rel_path).unwrap();

        // close session and open it again
        drop(client);
        let mut client = HttpClient::new(&repo_id, &access_key).unwrap();
        let new_update_seq = client.open_session().unwrap();
        assert_eq!(new_update_seq, update_seq + 1);
    }
}
