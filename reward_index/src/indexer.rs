use crate::{reward_index, settings, telemetry, Settings};
use anyhow::{anyhow, bail, Result};
use chrono::{DateTime, Utc};
use file_store::{
    file_info_poller::FileInfoStream,
    reward_manifest::{
        RewardData::{self, IotRewardData, MobileRewardData},
        RewardManifest,
    },
    FileInfo, FileStore, Stream,
};
use futures::{future::LocalBoxFuture, stream, StreamExt, TryFutureExt, TryStreamExt};
use helium_crypto::PublicKeyBinary;
use helium_proto::Message;
use poc_metrics::record_duration;
use prost::bytes::BytesMut;
use sqlx::{PgPool, Postgres, Transaction};
use std::{collections::HashMap, str::FromStr};
use tokio::sync::mpsc::Receiver;

pub mod proto {
    pub use helium_proto::{
        services::{
            poc_lora::{iot_reward_share::Reward as IotReward, IotRewardShare},
            poc_mobile::{mobile_reward_share::Reward as MobileReward, MobileRewardShare},
        },
        IotRewardToken, MobileRewardToken, ServiceProvider,
    };
}

pub struct Indexer {
    pool: PgPool,
    verifier_store: FileStore,
    mode: settings::Mode,
    op_fund_key: String,
    unallocated_reward_key: String,
    reward_manifest_rx: Receiver<FileInfoStream<RewardManifest>>,
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
    pub key: String,
    pub reward_type: RewardType,
}

impl task_manager::ManagedTask for Indexer {
    fn start_task(
        self: Box<Self>,
        shutdown: triggered::Listener,
    ) -> LocalBoxFuture<'static, anyhow::Result<()>> {
        let handle = tokio::spawn(self.run(shutdown));
        Box::pin(
            handle
                .map_err(anyhow::Error::from)
                .and_then(|res| async move { res }),
        )
    }
}

impl Indexer {
    fn new(
        pool: PgPool,
        verifier_store: FileStore,
        reward_manifest_rx: Receiver<FileInfoStream<RewardManifest>>,
        mode: settings::Mode,
        op_fund_key: String,
        unallocated_reward_key: String,
    ) -> Self {
        Self {
            pool,
            verifier_store,
            mode,
            op_fund_key,
            unallocated_reward_key,
            reward_manifest_rx,
        }
    }

    pub async fn from_settings(
        settings: &Settings,
        pool: PgPool,
        verifier_store: FileStore,
        reward_manifest_rx: Receiver<FileInfoStream<RewardManifest>>,
    ) -> Result<Self> {
        Ok(Self::new(
            pool,
            verifier_store,
            reward_manifest_rx,
            settings.mode,
            settings.operation_fund_key()?,
            settings.unallocated_reward_entity_key.clone(),
        ))
    }

    pub async fn run(mut self, shutdown: triggered::Listener) -> Result<()> {
        tracing::info!(mode = self.mode.to_string(), "starting index");

        loop {
            tokio::select! {
                biased;
                _ = shutdown.clone() => {
                    tracing::info!("Indexer shutting down");
                    return Ok(());
                }
                msg = self.reward_manifest_rx.recv() => if let Some(file_info_stream) = msg {
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

        // if the token type defined in the reward data is not HNT, then bail
        self.verify_token_type(&manifest.reward_data)?;

        let reward_shares = self.verifier_store.source_unordered(5, reward_files);

        match self.mode {
            settings::Mode::Iot => {
                handle_iot_rewards(
                    txn,
                    reward_shares,
                    &self.op_fund_key,
                    &self.unallocated_reward_key,
                    &manifest_time,
                )
                .await?;
            }
            settings::Mode::Mobile => {
                handle_mobile_rewards(
                    txn,
                    reward_shares,
                    &self.unallocated_reward_key,
                    &manifest_time,
                )
                .await?;
            }
        };

        Ok(())
    }

    fn verify_token_type(&self, reward_data: &Option<RewardData>) -> Result<()> {
        match reward_data {
            Some(MobileRewardData { token, .. }) => {
                if *token != proto::MobileRewardToken::Hnt {
                    bail!(
                        "legacy token type defined in manifest: {}",
                        token.as_str_name()
                    );
                }
            }
            Some(IotRewardData { token, .. }) => {
                if *token != proto::IotRewardToken::Hnt {
                    bail!(
                        "legacy token type defined in manifest: {}",
                        token.as_str_name()
                    );
                }
            }
            None => bail!("missing reward data in manifest"),
        }
        Ok(())
    }
}

pub async fn handle_iot_rewards(
    txn: &mut Transaction<'_, Postgres>,
    mut reward_shares: Stream<BytesMut>,
    op_fund_key: &str,
    unallocated_reward_key: &str,
    manifest_time: &DateTime<Utc>,
) -> anyhow::Result<()> {
    let mut rewards = HashMap::new();

    while let Some(msg) = reward_shares.try_next().await? {
        let share = proto::IotRewardShare::decode(msg)?;
        if let Some((key, amount)) = extract_iot_reward(share, op_fund_key, unallocated_reward_key)?
        {
            *rewards.entry(key).or_default() += amount;
        }
    }

    for (reward_key, amount) in rewards {
        reward_index::insert(
            &mut *txn,
            reward_key.key,
            amount,
            reward_key.reward_type,
            manifest_time,
        )
        .await?;
    }

    Ok(())
}

pub async fn handle_mobile_rewards(
    txn: &mut Transaction<'_, Postgres>,
    mut reward_shares: Stream<BytesMut>,
    unallocated_reward_key: &str,
    manifest_time: &DateTime<Utc>,
) -> anyhow::Result<()> {
    let mut rewards = HashMap::new();

    while let Some(msg) = reward_shares.try_next().await? {
        let share = proto::MobileRewardShare::decode(msg)?;
        if let Some((key, amount)) = extract_mobile_reward(share, unallocated_reward_key)? {
            *rewards.entry(key).or_default() += amount;
        }
    }

    for (reward_key, amount) in rewards {
        reward_index::insert(
            &mut *txn,
            reward_key.key,
            amount,
            reward_key.reward_type,
            manifest_time,
        )
        .await?;
    }

    Ok(())
}

pub fn extract_reward_share(
    mode: settings::Mode,
    msg: &[u8],
    op_fund_key: &str,
    unallocated_reward_key: &str,
) -> Result<Option<(RewardKey, u64)>> {
    match mode {
        settings::Mode::Mobile => {
            let share = proto::MobileRewardShare::decode(msg)?;
            extract_mobile_reward(share, unallocated_reward_key)
        }
        settings::Mode::Iot => {
            let share = proto::IotRewardShare::decode(msg)?;
            extract_iot_reward(share, op_fund_key, unallocated_reward_key)
        }
    }
}

pub fn extract_mobile_reward(
    share: proto::MobileRewardShare,
    unallocated_reward_key: &str,
) -> anyhow::Result<Option<(RewardKey, u64)>> {
    let Some(reward) = share.reward else {
        bail!("got an invalid mobile reward share");
    };

    use proto::{MobileReward, ServiceProvider};

    match reward {
        MobileReward::RadioReward(_r) => {
            // RadioReward has been replaced by RadioRewardV2
            Ok(None)
        }
        MobileReward::RadioRewardV2(r) => Ok(Some((
            RewardKey {
                key: PublicKeyBinary::from(r.hotspot_key).to_string(),
                reward_type: RewardType::MobileGateway,
            },
            r.base_poc_reward + r.boosted_poc_reward,
        ))),
        MobileReward::GatewayReward(r) => Ok(Some((
            RewardKey {
                key: PublicKeyBinary::from(r.hotspot_key).to_string(),
                reward_type: RewardType::MobileGateway,
            },
            r.dc_transfer_reward,
        ))),
        MobileReward::SubscriberReward(r) => Ok(Some((
            RewardKey {
                key: bs58::encode(&r.subscriber_id).into_string(),
                reward_type: RewardType::MobileSubscriber,
            },
            r.discovery_location_amount + r.verification_mapping_amount,
        ))),
        MobileReward::ServiceProviderReward(r) => ServiceProvider::try_from(r.service_provider_id)
            .map(|sp| {
                Ok(Some((
                    RewardKey {
                        key: sp.to_string(),
                        reward_type: RewardType::MobileServiceProvider,
                    },
                    r.amount,
                )))
            })
            .map_err(|_| anyhow!("failed to decode service provider"))?,
        MobileReward::UnallocatedReward(r) => Ok(Some((
            RewardKey {
                key: unallocated_reward_key.to_string(),
                reward_type: RewardType::MobileUnallocated,
            },
            r.amount,
        ))),
        MobileReward::PromotionReward(promotion) => Ok(Some((
            RewardKey {
                key: promotion.entity,
                reward_type: RewardType::MobilePromotion,
            },
            promotion.service_provider_amount + promotion.matched_amount,
        ))),
    }
}

pub fn extract_iot_reward(
    share: proto::IotRewardShare,
    op_fund_key: &str,
    unallocated_reward_key: &str,
) -> anyhow::Result<Option<(RewardKey, u64)>> {
    let Some(reward) = share.reward else {
        bail!("got an invalid iot reward share")
    };

    use proto::IotReward;

    match reward {
        IotReward::GatewayReward(r) => Ok(Some((
            RewardKey {
                key: PublicKeyBinary::from(r.hotspot_key).to_string(),
                reward_type: RewardType::IotGateway,
            },
            r.witness_amount + r.beacon_amount + r.dc_transfer_amount,
        ))),
        IotReward::OperationalReward(r) => Ok(Some((
            RewardKey {
                key: op_fund_key.to_string(),
                reward_type: RewardType::IotOperational,
            },
            r.amount,
        ))),
        IotReward::UnallocatedReward(r) => Ok(Some((
            RewardKey {
                key: unallocated_reward_key.to_string(),
                reward_type: RewardType::IotUnallocated,
            },
            r.amount,
        ))),
    }
}

#[cfg(test)]
mod tests {

    use super::*;

    use helium_proto::services::poc_lora::GatewayReward as IotGatewayReward;
    use helium_proto::services::poc_mobile::GatewayReward as MobileGatewayReward;

    #[test]
    fn test_extract_mobile_reward() -> anyhow::Result<()> {
        let mobile_reward = proto::MobileRewardShare {
            start_period: Utc::now().timestamp_millis() as u64,
            end_period: Utc::now().timestamp_millis() as u64,
            reward: Some(proto::MobileReward::GatewayReward(MobileGatewayReward {
                hotspot_key: vec![1],
                dc_transfer_reward: 1,
                rewardable_bytes: 2,
                price: 3,
            })),
        };

        let (reward_key, amount) =
            extract_mobile_reward(mobile_reward, "unallocated-key")?.expect("valid reward share");
        assert_eq!(reward_key.key, PublicKeyBinary::from(vec![1]).to_string());
        assert_eq!(reward_key.reward_type, RewardType::MobileGateway);
        assert_eq!(amount, 1, "only dc_transfer_reward");

        Ok(())
    }

    #[test]
    fn test_extract_iot_reward() -> anyhow::Result<()> {
        let iot_reward = proto::IotRewardShare {
            start_period: Utc::now().timestamp_millis() as u64,
            end_period: Utc::now().timestamp_millis() as u64,
            reward: Some(proto::IotReward::GatewayReward(IotGatewayReward {
                hotspot_key: vec![1],
                beacon_amount: 1,
                witness_amount: 2,
                dc_transfer_amount: 3,
            })),
        };

        let (reward_key, amount) =
            extract_iot_reward(iot_reward, "op-fund-key", "unallocated-key")?
                .expect("valid reward share");
        assert_eq!(reward_key.key, PublicKeyBinary::from(vec![1]).to_string());
        assert_eq!(reward_key.reward_type, RewardType::IotGateway);
        assert_eq!(amount, 6, "all reward added together");

        Ok(())
    }

    #[test]
    fn test_extract_reward_share() -> anyhow::Result<()> {
        let mobile_reward = proto::MobileRewardShare {
            start_period: Utc::now().timestamp_millis() as u64,
            end_period: Utc::now().timestamp_millis() as u64,
            reward: Some(proto::MobileReward::GatewayReward(MobileGatewayReward {
                hotspot_key: vec![1],
                dc_transfer_reward: 1,
                rewardable_bytes: 2,
                price: 3,
            })),
        };
        let iot_reward = proto::IotRewardShare {
            start_period: Utc::now().timestamp_millis() as u64,
            end_period: Utc::now().timestamp_millis() as u64,
            reward: Some(proto::IotReward::GatewayReward(IotGatewayReward {
                hotspot_key: vec![1],
                beacon_amount: 1,
                witness_amount: 2,
                dc_transfer_amount: 3,
            })),
        };

        let key = PublicKeyBinary::from(vec![1]).to_string();

        let (mobile_reward_key, mobile_amount) =
            extract_reward_share(settings::Mode::Mobile, &mobile_reward.as_bytes(), "", "")?
                .expect("valid mobile share");

        assert_eq!(mobile_reward_key.key, key);
        assert_eq!(mobile_reward_key.reward_type, RewardType::MobileGateway);
        assert_eq!(mobile_amount, 1, "only dc_transfer_reward");

        let (iot_reward_key, iot_amount) =
            extract_reward_share(settings::Mode::Iot, &iot_reward.as_bytes(), "", "")?
                .expect("valid iot share");

        assert_eq!(iot_reward_key.key, key);
        assert_eq!(iot_reward_key.reward_type, RewardType::IotGateway);
        assert_eq!(iot_amount, 6, "all reward added together");

        Ok(())
    }
}
