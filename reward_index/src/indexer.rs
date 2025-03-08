use crate::{reward_index, settings, telemetry, Settings};
use anyhow::{anyhow, bail, Result};
use chrono::Utc;
use file_store::{
    file_info_poller::FileInfoStream,
    reward_manifest::RewardData::{self, IotRewardData, MobileRewardData},
    reward_manifest::RewardManifest,
    FileInfo, FileStore,
};
use futures::{future::LocalBoxFuture, stream, StreamExt, TryFutureExt, TryStreamExt};
use helium_crypto::PublicKeyBinary;
use helium_proto::{
    services::{
        poc_lora::{iot_reward_share::Reward as IotReward, IotRewardShare},
        poc_mobile::{mobile_reward_share::Reward as MobileReward, MobileRewardShare},
    },
    IotRewardToken, Message, MobileRewardToken, ServiceProvider,
};
use poc_metrics::record_duration;
use sqlx::{PgPool, Postgres, Transaction};
use std::{collections::HashMap, str::FromStr};
use tokio::sync::mpsc::Receiver;

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
        extract_reward_share(
            self.mode,
            msg,
            &self.op_fund_key,
            &self.unallocated_reward_key,
        )
    }

    fn verify_token_type(&self, reward_data: &Option<RewardData>) -> Result<()> {
        match reward_data {
            Some(MobileRewardData { token, .. }) => {
                if *token != MobileRewardToken::Hnt {
                    bail!(
                        "legacy token type defined in manifest: {}",
                        token.as_str_name()
                    );
                }
            }
            Some(IotRewardData { token, .. }) => {
                if *token != IotRewardToken::Hnt {
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

pub fn extract_reward_share(
    mode: settings::Mode,
    msg: &[u8],
    op_fund_key: &str,
    unallocated_reward_key: &str,
) -> Result<Option<(RewardKey, u64)>> {
    match mode {
        settings::Mode::Mobile => {
            let share = MobileRewardShare::decode(msg)?;
            extract_mobile_reward(share, unallocated_reward_key)
        }
        settings::Mode::Iot => {
            let share = IotRewardShare::decode(msg)?;
            extract_iot_reward(share, op_fund_key, unallocated_reward_key)
        }
    }
}

pub fn extract_mobile_reward(
    share: MobileRewardShare,
    unallocated_reward_key: &str,
) -> anyhow::Result<Option<(RewardKey, u64)>> {
    let Some(reward) = share.reward else {
        bail!("got an invalid mobile reward share");
    };

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
    share: IotRewardShare,
    op_fund_key: &str,
    unallocated_reward_key: &str,
) -> anyhow::Result<Option<(RewardKey, u64)>> {
    let Some(reward) = share.reward else {
        bail!("got an invalid iot reward share")
    };

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
    use chrono::DateTime;
    use sqlx::{postgres::PgRow, FromRow};

    use super::*;

    #[sqlx::test]
    async fn rewards_accumulate_to_rows_with_same_key_and_type(pool: PgPool) -> anyhow::Result<()> {
        let now = Utc::now();
        let key = "simple-key".to_string();

        let mut txn = pool.begin().await?;
        reward_index::insert(&mut txn, key.clone(), 1, RewardType::MobileGateway, &now).await?;
        reward_index::insert(&mut txn, key.clone(), 2, RewardType::MobileGateway, &now).await?;
        txn.commit().await?;

        let value = get(&pool, &key, RewardType::MobileGateway).await?;
        assert_eq!(value.rewards, 3);

        Ok(())
    }

    #[sqlx::test]
    async fn zero_amount_rewards_do_not_update_last_rewarded_timestamp(
        pool: PgPool,
    ) -> anyhow::Result<()> {
        let now = Utc::now();
        let before = now - chrono::Duration::days(1);

        let key = "simple-key".to_string();

        let mut txn = pool.begin().await?;
        reward_index::insert(&mut txn, key.clone(), 1, RewardType::MobileGateway, &before).await?;
        reward_index::insert(&mut txn, key.clone(), 0, RewardType::MobileGateway, &now).await?;
        txn.commit().await?;

        let res = get(&pool, &key, RewardType::MobileGateway).await?;
        assert_eq!(res.last_reward, before);
        assert_eq!(res.rewards, 1);

        Ok(())
    }

    // pub async fn all(pool: &PgPool) -> anyhow::Result<Vec<RewardIndex>> {
    //     let reward = sqlx::query_as("SELECT * FROM reward_index")
    //         .fetch_all(pool)
    //         .await?;

    //     Ok(reward)
    // }

    pub async fn get(
        pool: &PgPool,
        key: &str,
        reward_type: RewardType,
    ) -> anyhow::Result<RewardIndex> {
        let reward: RewardIndex = sqlx::query_as(
            r#"
        SELECT *
        FROM reward_index 
        WHERE address = $1 AND reward_type = $2
        "#,
        )
        .bind(key)
        .bind(reward_type)
        .fetch_one(pool)
        .await?;

        Ok(reward)
    }

    #[derive(Debug, Clone, PartialEq, Eq)]
    pub struct RewardIndex {
        pub address: String,
        pub rewards: u64,
        pub last_reward: DateTime<Utc>,
        pub reward_type: RewardType,
    }

    // impl From row for Rewardindex
    impl FromRow<'_, PgRow> for RewardIndex {
        fn from_row(row: &PgRow) -> Result<Self, sqlx::Error> {
            Ok(Self {
                address: row.get("address"),
                rewards: row.get::<i64, _>("rewards") as u64,
                last_reward: row.try_get("last_reward")?,
                reward_type: row.try_get("reward_type")?,
            })
        }
    }
}
