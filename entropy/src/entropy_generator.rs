use crate::{Error, Result};
use chrono::Utc;
use file_store::file_sink;
use futures::TryFutureExt;
use helium_proto::EntropyReportV1;
use jsonrpsee::{
    core::client::ClientT,
    http_client::{HttpClient, HttpClientBuilder},
    rpc_params,
};
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::env;
use tokio::{sync::watch, time};

pub const ENTROPY_TICK_TIME: time::Duration = time::Duration::from_secs(60);
const ENTROPY_TIMEOUT: time::Duration = time::Duration::from_secs(5);

pub type MessageSender = watch::Sender<Entropy>;
pub type MessageReceiver = watch::Receiver<Entropy>;

pub const ENTROPY_VERSION: u32 = 0;

pub fn message_channel(init: Entropy) -> (MessageSender, MessageReceiver) {
    watch::channel(init)
}

#[derive(Debug, Clone, Serialize)]
pub struct Entropy {
    pub version: u32,
    pub timestamp: i64,
    #[serde(serialize_with = "ser_base64")]
    pub data: Vec<u8>,
}

fn ser_base64<T, S>(key: &T, serializer: S) -> std::result::Result<S::Ok, S::Error>
where
    T: AsRef<[u8]>,
    S: serde::ser::Serializer,
{
    serializer.serialize_str(&base64::encode(key.as_ref()))
}
impl std::fmt::Display for Entropy {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&base64::encode(&self.data))
    }
}

impl From<&Entropy> for EntropyReportV1 {
    fn from(v: &Entropy) -> Self {
        Self {
            data: v.data.clone(),
            timestamp: v.timestamp as u64,
            version: v.version,
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
struct JsonRpcResult {
    context: serde_json::Map<String, serde_json::Value>,
    value: serde_json::Map<String, serde_json::Value>,
}

pub struct EntropyGenerator {
    pub receiver: MessageReceiver,

    client: HttpClient,
    sender: MessageSender,
}

impl EntropyGenerator {
    pub async fn from_env() -> Result<Self> {
        let url = env::var("ENTROPY_URL")?;
        Self::new(url).await
    }

    pub async fn new(url: impl AsRef<str>) -> Result<Self> {
        let client = HttpClientBuilder::default()
            .request_timeout(ENTROPY_TIMEOUT)
            .build(url)?;
        let entropy = Self::get_entropy(&client)
            .map_ok(|data| Entropy {
                data,
                timestamp: Utc::now().timestamp(),
                version: ENTROPY_VERSION,
            })
            .inspect_ok(|entropy| {
                tracing::info!(
                    "initialized entropy: {} at: {}",
                    entropy.to_string(),
                    entropy.timestamp
                )
            })
            .await?;
        let (sender, receiver) = watch::channel(entropy);
        Ok(Self {
            client,
            receiver,
            sender,
        })
    }

    pub async fn run(
        &mut self,
        file_sink: file_sink::MessageSender,
        shutdown: &triggered::Listener,
    ) -> Result {
        tracing::info!("started entropy generator");
        let mut entropy_timer = time::interval(ENTROPY_TICK_TIME);
        entropy_timer.set_missed_tick_behavior(time::MissedTickBehavior::Delay);

        loop {
            if shutdown.is_triggered() {
                break;
            }
            tokio::select! {
                _ = shutdown.clone() => break,
                _ = entropy_timer.tick() => match self.handle_entropy_tick(&file_sink).await {
                    Ok(()) => (),
                    Err(err) => {
                        tracing::error!("fatal entropy generator error: {err:?}");
                        return Err(err)
                    }
                }
            }
        }
        tracing::info!("stopping entropy generator");
        Ok(())
    }

    pub fn receiver(&self) -> MessageReceiver {
        self.receiver.clone()
    }

    async fn handle_entropy_tick(&mut self, file_sink: &file_sink::MessageSender) -> Result {
        match Self::get_entropy(&self.client).await {
            Ok(data) => self.sender.send_modify(|entry| {
                entry.timestamp = Utc::now().timestamp();
                entry.data = data;
                entry.version = ENTROPY_VERSION;
            }),
            Err(err) => {
                tracing::warn!("failed to get entropy: {err:?}");
                self.sender
                    .send_modify(|entry| entry.timestamp = Utc::now().timestamp());
            }
        }
        let entropy = &*self.receiver.borrow();
        tracing::info!(
            "using entropy: {} at: {}",
            entropy.to_string(),
            entropy.timestamp
        );
        file_sink::write(file_sink, EntropyReportV1::from(entropy)).await?;
        Ok(())
    }

    async fn get_entropy(client: &HttpClient) -> Result<Vec<u8>> {
        let params = rpc_params!(json!({"commitment": "processed"}));
        client
            .request("getLatestBlockhash", params)
            .map_err(Error::from)
            .and_then(|result: JsonRpcResult| async move {
                result
                    .value
                    .get("blockhash")
                    .and_then(|v| v.as_str())
                    .ok_or_else(|| Error::not_found("no blockhash found"))
                    .and_then(|hash| bs58::decode(hash).into_vec().map_err(Error::from))
            })
            .await
    }
}
