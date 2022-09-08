use crate::{env_var, Error, Result};
use chrono::Utc;
use futures::TryFutureExt;
use helium_proto::Entropy as ProtoEntropy;
use jsonrpsee::{
    core::client::ClientT,
    http_client::{HttpClient, HttpClientBuilder},
    rpc_params,
};
use poc_store::file_sink;
use serde::{Deserialize, Serialize};
use serde_json::json;
use tokio::{sync::watch, time};

const ENTROPY_TICK_TIME: time::Duration = time::Duration::from_secs(60);
const ENTROPY_TIMEOUT: time::Duration = time::Duration::from_secs(5);

pub type MessageSender = watch::Sender<Entropy>;
pub type MessageReceiver = watch::Receiver<Entropy>;

pub fn message_channel(init: Entropy) -> (MessageSender, MessageReceiver) {
    watch::channel(init)
}

#[derive(Debug, Clone)]
pub struct Entropy {
    pub timestamp: i64,
    pub data: Vec<u8>,
}

impl std::fmt::Display for Entropy {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&bs58::encode(&self.data).into_string())
    }
}

impl From<&Entropy> for ProtoEntropy {
    fn from(v: &Entropy) -> Self {
        Self {
            data: v.data.clone(),
            timestamp: v.timestamp as u64,
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
        let url =
            env_var("ENTROPY_URL")?.ok_or_else(|| Error::not_found("ENTROPY_URL var not found"))?;
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
        file_sink::write(file_sink, ProtoEntropy::from(entropy)).await?;
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
