use crate::{reward_index, settings, Settings};
use anyhow::{anyhow, bail, Result};
use file_store::{
    file_info_poller::FileInfoStream, reward_manifest::RewardManifest, FileInfo, FileStore,
};
use futures::{stream, StreamExt, TryStreamExt};
use helium_proto::{
    services::poc_lora::{iot_reward_share::Reward as IotReward, IotRewardShare},
    services::poc_mobile::{mobile_reward_share::Reward as MobileReward, MobileRewardShare},
    Message,
};
use poc_metrics::record_duration;
use sqlx::{Pool, Postgres, Transaction};
use std::{collections::HashMap, str::FromStr};
use tokio::sync::mpsc::Receiver;

pub struct Indexer {
    pool: Pool<Postgres>,
    verifier_store: FileStore,
    mode: settings::Mode,
    op_fund_key: Vec<u8>,
}

#[derive(sqlx::Type, Debug, Clone, PartialEq, Eq, Hash)]
#[sqlx(type_name = "reward_type", rename_all = "snake_case")]
pub enum RewardType {
    MobileGateway,
    IotGateway,
    IotOperational,
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
            op_fund_key: match settings.mode {
                settings::Mode::Iot => settings
                    .operation_fund_key()
                    .ok_or_else(|| anyhow!("operation fund key is required for IOT mode"))?,
                settings::Mode::Mobile => vec![],
            },
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
                reward_key.reward_type,
                &manifest_time,
            )
            .await?;
        }

        Ok(())
    }

    fn extract_reward_share(&self, msg: &[u8]) -> Result<(RewardKey, u64)> {
        match self.mode {
            settings::Mode::Mobile => {
                let share = MobileRewardShare::decode(msg)?;
                match share.reward {
                    Some(MobileReward::RadioReward(r)) => Ok((
                        RewardKey {
                            key: r.hotspot_key,
                            reward_type: RewardType::MobileGateway,
                        },
                        r.dc_transfer_reward + r.poc_reward,
                    )),
                    _ => bail!("got an invalid mobile reward share"),
                }
            }
            settings::Mode::Iot => {
                let share = IotRewardShare::decode(msg)?;
                match share.reward {
                    Some(IotReward::GatewayReward(r)) => Ok((
                        RewardKey {
                            key: r.hotspot_key,
                            reward_type: RewardType::IotGateway,
                        },
                        r.witness_amount + r.beacon_amount + r.dc_transfer_amount,
                    )),
                    Some(IotReward::OperationalReward(r)) => Ok((
                        RewardKey {
                            key: self.op_fund_key.clone().to_vec(),
                            reward_type: RewardType::IotOperational,
                        },
                        r.amount,
                    )),
                    _ => bail!("got an invalid iot reward share"),
                }
            }
        }
    }
}
