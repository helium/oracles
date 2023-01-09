use crate::{error::Error, models::metadata::DenyListMetaData, Result};
use std::{str, time::Duration};

/// The default client useragent for denylist http requests
static USERAGENT: &str = "oracle/iot_verifier/1.0";
/// The default timeout for http requests
pub const DEFAULT_TIMEOUT: Duration = Duration::from_secs(5);

#[derive(Clone, Debug)]
pub struct DenyListClient {
    pub client: reqwest::Client,
}

// TODO: basic non production client. productionise....
impl DenyListClient {
    pub fn new() -> Result<Self> {
        let client = reqwest::Client::builder()
            .gzip(true)
            .user_agent(USERAGENT)
            .timeout(DEFAULT_TIMEOUT)
            .build()
            .map_err(Error::from)?;
        Ok(Self { client })
    }

    pub async fn get_metadata(&mut self, url: &String) -> Result<DenyListMetaData> {
        let response = self.client.get(url).send().await?;
        match response.status() {
            reqwest::StatusCode::OK => {
                let json = response.json::<DenyListMetaData>().await?;
                Ok(json)
            }
            other => Err(Error::UnexpectedStatus(other.to_string())),
        }
    }

    pub async fn get_bin(&mut self, url: &String) -> Result<Vec<u8>> {
        let response = self.client.get(url).send().await?;
        match response.status() {
            reqwest::StatusCode::OK => {
                let bytes = response.bytes().await?;
                Ok(bytes.to_vec())
            }
            other => Err(Error::UnexpectedStatus(other.to_string())),
        }
    }
}
