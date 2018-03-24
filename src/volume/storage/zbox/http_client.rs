use std::time::Duration;

use serde::Serialize;
use reqwest::{header, Client, Response, StatusCode};

use error::Result;

#[derive(Debug)]
pub struct HttpClient {
    base_url: String,
    client: Client,
}

impl HttpClient {
    const URL_ROOT: &'static str = "https://data.zbox.io/repos/";

    pub fn new(repo_id: &str, access_key: &str) -> Result<Self> {
        // set http authorization header
        let mut headers = header::Headers::new();
        headers.set(header::Authorization("Basic ".to_owned() + access_key));

        // create http client
        let client = Client::builder()
            .timeout(Duration::from_secs(30))
            .default_headers(headers)
            .build()?;

        // create base URL
        let base_url = Self::URL_ROOT.to_owned() + repo_id + "/";

        Ok(HttpClient { base_url, client })
    }

    #[inline]
    pub fn base_url(&self) -> &str {
        &self.base_url
    }

    pub fn head(&self, loc: &str) -> Result<Response> {
        let url = self.base_url.clone() + loc;
        debug!("http head: {}", url);
        let resp = self.client.head(&url).send()?.error_for_status()?;
        Ok(resp)
    }

    pub fn get(&self, loc: &str) -> Result<Vec<u8>> {
        let url = self.base_url.clone() + loc;
        debug!("http get: {}", url);
        let mut resp = self.client.get(&url).send()?.error_for_status()?;
        let mut buf: Vec<u8> = Vec::new();
        resp.copy_to(&mut buf)?;
        Ok(buf)
    }

    pub fn put(&mut self, loc: &str, buf: &[u8]) -> Result<()> {
        let url = self.base_url.clone() + loc;
        debug!("http put: {}", url);
        self.client
            .put(&url)
            .body(buf.to_vec())
            .send()?
            .error_for_status()?;
        Ok(())
    }

    pub fn delete(&mut self, loc: &str) -> Result<()> {
        let url = self.base_url.clone() + loc;
        debug!("http delete: {}", url);
        self.client.delete(&url).send()?.error_for_status()?;
        Ok(())
    }

    pub fn put_json<T: Serialize + ?Sized>(
        &mut self,
        loc: &str,
        body: &T,
    ) -> Result<()> {
        let url = self.base_url.clone() + loc;
        debug!("http put json: {}", url);
        self.client.put(&url).json(body).send()?.error_for_status()?;
        Ok(())
    }
}
