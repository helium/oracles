use crate::{reward_index, settings, Settings};
use anyhow::{bail, Result};
use file_store::{
    file_info_poller::FileInfoStream, reward_manifest::RewardManifest, FileInfo, FileStore,
};
use futures::{stream, StreamExt, TryStreamExt};
use helium_proto::services::poc_lora::iot_reward_share::Reward as ProtoReward;
use helium_proto::{
    services::poc_lora::IotRewardShare, services::poc_mobile::RadioRewardShare, Message,
};
use poc_metrics::record_duration;
use sqlx::{Pool, Postgres, Transaction};
use std::fmt;
use std::{collections::HashMap, str::FromStr};
use tokio::sync::mpsc::Receiver;

pub struct Indexer {
    pool: Pool<Postgres>,
    verifier_store: FileStore,
    mode: settings::Mode,
    op_fund_key: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum RewardType {
    Mobile,
    IotGateway,
    IotOperational,
}

impl fmt::Display for RewardType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Mobile => f.write_str("mobile"),
            Self::IotGateway => f.write_str("iot_gateway"),
            Self::IotOperational => f.write_str("iot_operational"),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct RewardKey {
    key: Vec<u8>,
    reward_type: RewardType,
}

impl Indexer {
    pub async fn new(settings: &Settings, pool: Pool<Postgres>) -> Result<Self> {
        Ok(Self {
            mode: settings.mode,
            verifier_store: FileStore::from_settings(&settings.verifier).await?,
            pool,
            op_fund_key: settings.operation_fund_key(),
        })
    }

    pub async fn run(
        &mut self,
        shutdown: triggered::Listener,
        mut receiver: Receiver<FileInfoStream<RewardManifest>>,
    ) -> Result<()> {
        tracing::info!(mode = self.mode.to_string(), "starting index");

        loop {
            tokio::select! {
            _ = shutdown.clone() => {
                    tracing::info!("Indexer shutting down");
                    return Ok(());
            }
            msg = receiver.recv() => if let Some(file_info_stream) = msg {
                    tracing::info!("Processing reward file {}", file_info_stream.file_info.key);
                    let mut txn = self.pool.begin().await?;
                    let mut stream = file_info_stream.into_stream(&mut txn).await?;

                    while let Some(reward_manifest) = stream.next().await {
                        record_duration!(
                            "reward_index_duration",
                            self.handle_rewards(&mut txn, reward_manifest).await?
                        )
                    }

                    txn.commit().await?;
                }
            }
        }
    }

    async fn handle_rewards(
        &mut self,
        txn: &mut Transaction<'_, Postgres>,
        manifest: RewardManifest,
    ) -> Result<()> {
        let manifest_time = manifest.end_timestamp;

        let reward_files = stream::iter(
            manifest
                .written_files
                .into_iter()
                .map(|file_name| FileInfo::from_str(&file_name)),
        )
        .boxed();

        let mut reward_shares = self.verifier_store.source_unordered(5, reward_files);
        let mut hotspot_rewards: HashMap<RewardKey, u64> = HashMap::new();

        while let Some(msg) = reward_shares.try_next().await? {
            let (key, amount) = self.extract_reward_share(&msg)?;
            *hotspot_rewards.entry(key).or_default() += amount;
        }

        for (reward_key, amount) in hotspot_rewards {
            reward_index::insert(
                &mut *txn,
                &reward_key.key,
                amount,
                reward_key.reward_type.to_string(),
                &manifest_time,
            )
            .await?;
        }

        Ok(())
    }

    fn extract_reward_share(&self, msg: &[u8]) -> Result<(RewardKey, u64)> {
        match self.mode {
            settings::Mode::Mobile => {
                let share = RadioRewardShare::decode(msg)?;
                let key = RewardKey {
                    key: share.hotspot_key,
                    reward_type: RewardType::Mobile,
                };
                Ok((key, share.amount))
            }
            settings::Mode::Iot => {
                let share = IotRewardShare::decode(msg)?;
                match share.reward {
                    Some(ProtoReward::GatewayReward(r)) => {
                        let key = RewardKey {
                            key: r.hotspot_key,
                            reward_type: RewardType::IotGateway,
                        };
                        Ok((
                            key,
                            r.witness_amount + r.beacon_amount + r.dc_transfer_amount,
                        ))
                    }
                    Some(ProtoReward::OperationalReward(r)) => {
                        let key = RewardKey {
                            key: self.op_fund_key.clone().to_vec(),
                            reward_type: RewardType::IotOperational,
                        };
                        Ok((key, r.amount))
                    }
                    _ => bail!("got an invalid reward share"),
                }
            }
        }
    }
}
