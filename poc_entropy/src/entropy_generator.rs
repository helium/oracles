use base64::Engine;
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

impl From<Entropy> for EntropyReportV1 {
    fn from(value: Entropy) -> Self {
        Self {
            version: value.version,
            timestamp: value.timestamp as u64,
            data: value.data,
        }
    }
}

fn ser_base64<T, S>(key: &T, serializer: S) -> std::result::Result<S::Ok, S::Error>
where
    T: AsRef<[u8]>,
    S: serde::ser::Serializer,
{
    serializer.serialize_str(&base64::engine::general_purpose::STANDARD.encode(key.as_ref()))
}
impl std::fmt::Display for Entropy {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&base64::engine::general_purpose::STANDARD.encode(&self.data))
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

#[derive(thiserror::Error, Debug)]
pub enum GetEntropyError {
    #[error("no blockhash found")]
    NoBlockHashFound,
    #[error("failed to decode hash: {0}")]
    DecodeError(#[from] bs58::decode::Error),
    #[error("json rpc error: {0}")]
    JsonRpcError(#[from] jsonrpsee::core::Error),
}

impl EntropyGenerator {
    pub async fn new(url: impl AsRef<str>) -> Result<Self, GetEntropyError> {
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
        file_sink: file_sink::FileSinkClient,
        shutdown: &triggered::Listener,
    ) -> anyhow::Result<()> {
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

    async fn handle_entropy_tick(
        &mut self,
        file_sink: &file_sink::FileSinkClient,
    ) -> anyhow::Result<()> {
        let source_data = match Self::get_entropy(&self.client).await {
            Ok(data) => data,
            Err(err) => {
                tracing::warn!("failed to get entropy: {err:?}");
                (*self.receiver.borrow().data).to_vec()
            }
        };
        let timestamp = Utc::now().timestamp();

        let mut hasher = blake3::Hasher::new();
        hasher.update(&timestamp.to_le_bytes());
        hasher.update(&source_data);
        let data = hasher.finalize().as_bytes().to_vec();

        self.sender.send_modify(|entry| {
            entry.timestamp = timestamp;
            entry.data = data;
        });

        let entropy = &*self.receiver.borrow();
        tracing::info!(
            "using entropy: {} at: {}",
            entropy.to_string(),
            entropy.timestamp
        );

        file_sink.write(EntropyReportV1::from(entropy), []).await?;

        Ok(())
    }

    async fn get_entropy(client: &HttpClient) -> Result<Vec<u8>, GetEntropyError> {
        let params = rpc_params!(json!({"commitment": "processed"}));
        client
            .request("getLatestBlockhash", params)
            .map_err(GetEntropyError::from)
            .and_then(|result: JsonRpcResult| async move {
                result
                    .value
                    .get("blockhash")
                    .and_then(|v| v.as_str())
                    .ok_or(GetEntropyError::NoBlockHashFound)
                    .and_then(|hash| bs58::decode(hash).into_vec().map_err(GetEntropyError::from))
            })
            .await
    }
}
