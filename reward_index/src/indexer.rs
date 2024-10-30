use crate::{reward_index, settings, telemetry, Settings};
use anyhow::{anyhow, bail, Result};
use chrono::Utc;
use file_store::{
    file_info_poller::FileInfoStream, reward_manifest::RewardManifest, FileInfo, FileStore,
};
use futures::{stream, StreamExt, TryStreamExt};
use helium_crypto::PublicKeyBinary;
use helium_proto::{
    services::{
        poc_lora::{iot_reward_share::Reward as IotReward, IotRewardShare},
        poc_mobile::{mobile_reward_share::Reward as MobileReward, MobileRewardShare},
    },
    Message, ServiceProvider,
};
use poc_metrics::record_duration;
use sqlx::{Pool, Postgres, Transaction};
use std::{collections::HashMap, str::FromStr};
use tokio::sync::mpsc::Receiver;

pub struct Indexer {
    pool: Pool<Postgres>,
    verifier_store: FileStore,
    mode: settings::Mode,
    op_fund_key: String,
    unallocated_reward_key: String,
}

#[derive(sqlx::Type, Debug, Clone, PartialEq, Eq, Hash)]
#[sqlx(type_name = "reward_type", rename_all = "snake_case")]
pub enum RewardType {
    MobileGateway,
    IotGateway,
    IotOperational,
    MobileSubscriber,
    MobileServiceProvider,
    MobileUnallocated,
    IotUnallocated,
    MobilePromotion,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct RewardKey {
    key: String,
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
                settings::Mode::Mobile => String::new(),
            },
            unallocated_reward_key: settings
                .unallocated_reward_entity_key()
                .ok_or_else(|| anyhow!("missing unallocated reward key"))?,
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
            biased;
            _ = shutdown.clone() => {
                    tracing::info!("Indexer shutting down");
                    return Ok(());
            }
            msg = receiver.recv() => if let Some(file_info_stream) = msg {
                    let key = &file_info_stream.file_info.key.clone();
                    tracing::info!(file = %key, "Processing reward file");
                    let mut txn = self.pool.begin().await?;
                    let mut stream = file_info_stream.into_stream(&mut txn).await?;

                    while let Some(reward_manifest) = stream.next().await {
                        record_duration!(
                            "reward_index_duration",
                            self.handle_rewards(&mut txn, reward_manifest).await?
                        )
                    }
                    txn.commit().await?;
                    tracing::info!(file = %key, "Completed processing reward file");
                    telemetry::last_reward_processed_time(&self.pool, Utc::now()).await?;
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
            if let Some((key, amount)) = self.extract_reward_share(&msg)? {
                *hotspot_rewards.entry(key).or_default() += amount;
            };
        }

        for (reward_key, amount) in hotspot_rewards {
            reward_index::insert(
                &mut *txn,
                reward_key.key,
                amount,
                reward_key.reward_type,
                &manifest_time,
            )
            .await?;
        }

        Ok(())
    }

    fn extract_reward_share(&self, msg: &[u8]) -> Result<Option<(RewardKey, u64)>> {
        match self.mode {
            settings::Mode::Mobile => {
                let share = MobileRewardShare::decode(msg)?;
                match share.reward {
                    Some(MobileReward::RadioReward(_r)) => {
                        // RadioReward has been replaced by RadioRewardV2
                        Ok(None)
                    }
                    Some(MobileReward::RadioRewardV2(r)) => Ok(Some((
                        RewardKey {
                            key: PublicKeyBinary::from(r.hotspot_key).to_string(),
                            reward_type: RewardType::MobileGateway,
                        },
                        r.base_poc_reward + r.boosted_poc_reward,
                    ))),

                    Some(MobileReward::GatewayReward(r)) => Ok(Some((
                        RewardKey {
                            key: PublicKeyBinary::from(r.hotspot_key).to_string(),
                            reward_type: RewardType::MobileGateway,
                        },
                        r.dc_transfer_reward,
                    ))),
                    Some(MobileReward::SubscriberReward(r)) => Ok(Some((
                        RewardKey {
                            key: bs58::encode(&r.subscriber_id).into_string(),
                            reward_type: RewardType::MobileSubscriber,
                        },
                        r.discovery_location_amount + r.verification_mapping_amount,
                    ))),
                    Some(MobileReward::ServiceProviderReward(r)) => {
                        ServiceProvider::try_from(r.service_provider_id)
                            .map(|sp| {
                                Ok(Some((
                                    RewardKey {
                                        key: sp.to_string(),
                                        reward_type: RewardType::MobileServiceProvider,
                                    },
                                    r.amount,
                                )))
                            })
                            .map_err(|_| anyhow!("failed to decode service provider"))?
                    }
                    Some(MobileReward::UnallocatedReward(r)) => Ok(Some((
                        RewardKey {
                            key: self.unallocated_reward_key.clone(),
                            reward_type: RewardType::MobileUnallocated,
                        },
                        r.amount,
                    ))),
                    Some(MobileReward::PromotionReward(promotion)) => Ok(Some((
                        RewardKey {
                            key: promotion.entity,
                            reward_type: RewardType::MobilePromotion,
                        },
                        promotion.service_provider_amount + promotion.matched_amount,
                    ))),
                    _ => bail!("got an invalid reward share"),
                }
            }
            settings::Mode::Iot => {
                let share = IotRewardShare::decode(msg)?;
                match share.reward {
                    Some(IotReward::GatewayReward(r)) => Ok(Some((
                        RewardKey {
                            key: PublicKeyBinary::from(r.hotspot_key).to_string(),
                            reward_type: RewardType::IotGateway,
                        },
                        r.witness_amount + r.beacon_amount + r.dc_transfer_amount,
                    ))),
                    Some(IotReward::OperationalReward(r)) => Ok(Some((
                        RewardKey {
                            key: self.op_fund_key.clone(),
                            reward_type: RewardType::IotOperational,
                        },
                        r.amount,
                    ))),
                    Some(IotReward::UnallocatedReward(r)) => Ok(Some((
                        RewardKey {
                            key: self.unallocated_reward_key.clone(),
                            reward_type: RewardType::IotUnallocated,
                        },
                        r.amount,
                    ))),
                    _ => bail!("got an invalid iot reward share"),
                }
            }
        }
    }
}
