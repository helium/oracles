use crate::Result;
use crate::{error::Error, models::metadata::DenyListMetaData};
use std::str;
use std::time::Duration;

/// The default client useragent for denylist http requests
static DEFAULT_USERAGENT: &str = "https://github.com/helium/miner";
/// The default timeout for http requests
pub const DEFAULT_TIMEOUT: Duration = Duration::from_secs(5);

#[derive(Clone, Debug)]
pub struct DenyListClient {
    pub client: reqwest::Client,
}

// TODO: basic non production client. productionise....
impl DenyListClient {
    pub fn new() -> Result<Self> {
        match reqwest::Client::builder()
            .gzip(true)
            .user_agent(DEFAULT_USERAGENT)
            .timeout(DEFAULT_TIMEOUT)
            .build()
        {
            Ok(client) => Ok(Self { client }),
            Err(err) => {
                tracing::error!("failing to initialize denylist client, error: {err:?}");
                Err(Error::Request(err))
            }
        }
    }

    pub async fn get_metadata(&mut self, url: &String) -> Option<DenyListMetaData> {
        if let Ok(response) = self.client.get(url).send().await {
            match response.status() {
                reqwest::StatusCode::OK => match response.json::<DenyListMetaData>().await {
                    Ok(json) => Some(json),
                    Err(err) => {
                        tracing::error!("failing to get denylist metadata, error: {err:?}");
                        None
                    }
                },
                _ => None,
            }
        } else {
            None
        }
    }

    pub async fn get_bin(&mut self, url: &String) -> Option<Vec<u8>> {
        if let Ok(response) = self.client.get(url).send().await {
            match response.status() {
                reqwest::StatusCode::OK => match response.bytes().await {
                    Ok(bytes) => Some(bytes.into()),
                    Err(err) => {
                        tracing::error!("failing to get denylist binary, error: {err:?}");
                        None
                    }
                },
                _ => None,
            }
        } else {
            None
        }
    }
}
