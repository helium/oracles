use chrono::{DateTime, Utc};
use file_store::{
    file_info_poller::{FileInfoStream, LookbackBehavior},
    file_sink::{self, FileSinkClient},
    file_source,
    file_upload::FileUpload,
    promotion_reward::{Entity, PromotionReward},
    traits::TimestampEncode,
    FileType,
};
use futures::{Stream, StreamExt, TryFutureExt, TryStreamExt};
use helium_proto::{
    services::poc_mobile::{
        self as proto, PromotionRewardIngestReportV1, PromotionRewardStatus,
        VerifiedPromotionRewardV1,
    },
    ServiceProvider,
};
use mobile_config::client::{carrier_service_client::CarrierServiceVerifier, CarrierServiceClient};
use rust_decimal::prelude::*;
use rust_decimal::{Decimal, RoundingStrategy};
use sqlx::{postgres::PgRow, PgPool, Postgres, Row, Transaction};
use std::{
    collections::HashMap,
    ops::Range,
    pin::pin,
    time::{Duration, Instant},
};
use task_manager::{ManagedTask, TaskManager};
use tokio::sync::mpsc::Receiver;

use crate::{reward_shares::ServiceProviderRewards, Settings};

pub struct PromotionRewardDaemon {
    pool: PgPool,
    carrier_client: CarrierServiceClient,
    promotion_rewards: Receiver<FileInfoStream<PromotionReward>>,
    promotion_rewards_sink: FileSinkClient,
}

impl PromotionRewardDaemon {
    pub async fn create_managed_task(
        pool: PgPool,
        settings: &Settings,
        file_upload: FileUpload,
        file_store: file_store::FileStore,
        carrier_client: CarrierServiceClient,
    ) -> anyhow::Result<impl ManagedTask> {
        let (promotion_rewards_sink, valid_promotion_rewards_server) =
            file_sink::FileSinkBuilder::new(
                FileType::VerifiedPromotionRewardIngestReport,
                settings.store_base_path(),
                file_upload.clone(),
                concat!(env!("CARGO_PKG_NAME"), "_promotion_reward"),
            )
            .auto_commit(false)
            .roll_time(Duration::from_secs(15 * 60))
            .create()
            .await?;

        let (promotion_rewards, promotion_rewards_server) =
            file_source::continuous_source::<PromotionReward, _>()
                .state(pool.clone())
                .store(file_store)
                .lookback(LookbackBehavior::StartAfter(settings.start_after))
                .prefix(FileType::PromotionRewardIngestReport.to_string())
                .create()
                .await?;

        let promotion_reward_daemon = Self {
            pool,
            carrier_client,
            promotion_rewards,
            promotion_rewards_sink,
        };

        Ok(TaskManager::builder()
            .add_task(valid_promotion_rewards_server)
            .add_task(promotion_rewards_server)
            .add_task(promotion_reward_daemon)
            .build())
    }

    async fn run(mut self, shutdown: triggered::Listener) -> anyhow::Result<()> {
        loop {
            tokio::select! {
                _ = shutdown.clone() => {
                    tracing::info!("PromotionRewardDaemon shutting down");
                    break;
                }
                Some(file) = self.promotion_rewards.recv() => {
                    let start = Instant::now();
                    self.process_file(file).await?;
                    metrics::histogram!("promotion_reward_processing_time")
                        .record(start.elapsed());
                }
            }
        }

        Ok(())
    }

    async fn process_file(&self, file: FileInfoStream<PromotionReward>) -> anyhow::Result<()> {
        tracing::info!("Processing promotion reward file {}", file.file_info.key);

        let mut transaction = self.pool.begin().await?;
        let reports = file.into_stream(&mut transaction).await?;

        let mut verified_promotion_rewards = pin!(
            VerifiedPromotionReward::validate_promotion_rewards(reports, &self.carrier_client,)
        );

        while let Some(promotion_reward) = verified_promotion_rewards.try_next().await? {
            promotion_reward.write(&self.promotion_rewards_sink).await?;
            if promotion_reward.is_valid() {
                promotion_reward.save(&mut transaction).await?;
            }
        }

        self.promotion_rewards_sink.commit().await?;
        transaction.commit().await?;

        Ok(())
    }
}

impl ManagedTask for PromotionRewardDaemon {
    fn start_task(
        self: Box<Self>,
        shutdown: triggered::Listener,
    ) -> futures::prelude::future::LocalBoxFuture<'static, anyhow::Result<()>> {
        let handle = tokio::spawn(self.run(shutdown));
        Box::pin(
            handle
                .map_err(anyhow::Error::from)
                .and_then(|result| async move { result.map_err(anyhow::Error::from) }),
        )
    }
}

pub struct VerifiedPromotionReward {
    validity: PromotionRewardStatus,
    promotion_reward: PromotionReward,
    service_provider: Option<ServiceProvider>,
}

impl VerifiedPromotionReward {
    async fn validate(
        promotion_reward: PromotionReward,
        carrier_client: &CarrierServiceClient,
    ) -> anyhow::Result<Self> {
        // mobile_config::client::ClientError::UnknownServiceProvider(_)
        let (service_provider, validity) = match carrier_client
            .payer_key_to_service_provider(
                &promotion_reward.carrier_pub_key.to_string(), // ????
            )
            .await
        {
            Ok(service_provider) => (Some(service_provider), PromotionRewardStatus::Valid),
            Err(mobile_config::client::ClientError::UnknownServiceProvider(_)) => {
                (None, PromotionRewardStatus::InvalidCarrierKey)
            }
            Err(x) => return Err(x.into()),
        };
        // TODO: Check if entity is rewardable
        Ok(Self {
            validity,
            promotion_reward,
            service_provider,
        })
    }

    fn validate_promotion_rewards<'a>(
        promotion_rewards: impl Stream<Item = PromotionReward> + 'a,
        carrier_client: &'a CarrierServiceClient,
    ) -> impl Stream<Item = anyhow::Result<Self>> + 'a {
        promotion_rewards.then(move |promotion_reward| async move {
            Self::validate(promotion_reward, carrier_client).await
        })
    }

    fn is_valid(&self) -> bool {
        matches!(self.validity, PromotionRewardStatus::Valid)
    }

    async fn write(&self, promotion_rewards: &FileSinkClient) -> anyhow::Result<()> {
        promotion_rewards
            .write(
                VerifiedPromotionRewardV1 {
                    report: Some(PromotionRewardIngestReportV1 {
                        received_timestamp: self
                            .promotion_reward
                            .received_timestamp
                            .encode_timestamp_millis(),
                        report: Some(self.promotion_reward.clone().into()),
                    }),
                    status: self.validity as i32,
                    timestamp: Utc::now().encode_timestamp_millis(),
                },
                &[("validity", self.validity.as_str_name())],
            )
            .await?;
        Ok(())
    }

    async fn save(self, transaction: &mut Transaction<'_, Postgres>) -> anyhow::Result<()> {
        let (subscriber_id, gateway_key) = match self.promotion_reward.entity {
            Entity::SubscriberId(sub_id) => (Some(sub_id), None),
            Entity::GatewayKey(gk) => (None, Some(gk)),
        };
        sqlx::query(
            r#"
            INSERT INTO promotion_rewards (time_of_reward, subscriber_id, gateway_key, service_provider, shares)
            VALUES ($1, COALESCE($2, '{}'), COALESCE($3, ''), $4, $5)
            ON CONFLICT DO NOTHING
            "#
        )
            .bind(self.promotion_reward.timestamp)
            .bind(subscriber_id)
            .bind(gateway_key)
            .bind(self.service_provider.map(|x| x as i64))
            .bind(self.promotion_reward.shares as i64)
            .execute(&mut *transaction)
            .await?;
        Ok(())
    }
}

pub async fn clear_promotion_rewards(
    tx: &mut Transaction<'_, Postgres>,
    timestamp: &DateTime<Utc>,
) -> Result<(), sqlx::Error> {
    sqlx::query("DELETE FROM promotion_rewards WHERE time_of_rewards < $1")
        .bind(timestamp)
        .execute(&mut *tx)
        .await?;
    Ok(())
}

pub struct AggregatePromotionRewards {
    rewards: Vec<PromotionRewardShares>,
}

pub struct PromotionRewardShares {
    service_provider: ServiceProvider,
    rewardable_entity: Entity,
    shares: u64,
}

impl sqlx::FromRow<'_, PgRow> for PromotionRewardShares {
    fn from_row(row: &PgRow) -> sqlx::Result<Self> {
        let subscriber_id: Vec<u8> = row.try_get("subscriber_id")?;
        let shares: i64 = row.try_get("shares")?;
        let service_provider: i64 = row.try_get("service_provider")?;
        Ok(Self {
            rewardable_entity: if subscriber_id.is_empty() {
                Entity::GatewayKey(row.try_get("gateway_key")?)
            } else {
                Entity::SubscriberId(subscriber_id)
            },
            shares: shares as u64,
            service_provider: ServiceProvider::try_from(service_provider as i32).unwrap(),
        })
    }
}

// This could use a better name
#[derive(Copy, Clone, Default)]
struct SpPromotionRewardShares {
    shares_per_reward: Decimal,
    shares_per_matched_reward: Decimal,
}

impl AggregatePromotionRewards {
    pub async fn aggregate(pool: &PgPool, epoch: &Range<DateTime<Utc>>) -> sqlx::Result<Self> {
        let rewards = sqlx::query_as(
            r#"
            SELECT service_provider, subscriber_id, gateway_key, SUM(shares)
            FROM promotion_rewards
            WHERE time_of_reward >= $1 AND time_of_reward < $2
            GROUP BY service_provider, subscriber_id, gateway_key
            "#,
        )
        .bind(epoch.start)
        .bind(epoch.end)
        .fetch_all(pool)
        .await?;
        Ok(Self { rewards })
    }

    pub fn into_rewards<'a>(
        self,
        sp_rewards: &mut ServiceProviderRewards,
        unallocated_sp_rewards: Decimal,
        reward_period: &'a Range<DateTime<Utc>>,
    ) -> impl Iterator<Item = (u64, proto::MobileRewardShare)> + 'a {
        let total_promotion_rewards_allocated =
            sp_rewards.get_total_rewards_allocated_for_promotion();
        let total_shares_per_service_provider = self.rewards.iter().fold(
            HashMap::<ServiceProvider, Decimal>::default(),
            |mut shares, promotion_reward_share| {
                *shares
                    .entry(promotion_reward_share.service_provider)
                    .or_default() += Decimal::from(promotion_reward_share.shares);
                shares
            },
        );
        let sp_promotion_reward_shares: HashMap<_, _> = total_shares_per_service_provider
            .iter()
            .map(|(sp, total_shares)| {
                let rewards_allocated_for_promotion =
                    sp_rewards.take_rewards_allocated_for_promotion(sp);
                let share_of_unallocated_pool =
                    rewards_allocated_for_promotion / total_promotion_rewards_allocated;
                let sp_share = SpPromotionRewardShares {
                    shares_per_reward: rewards_allocated_for_promotion / total_shares,
                    shares_per_matched_reward: unallocated_sp_rewards * share_of_unallocated_pool
                        / total_shares,
                };
                (*sp, sp_share)
            })
            .collect();

        self.rewards
            .into_iter()
            .map(move |reward| {
                let shares = Decimal::from(reward.shares);
                let sp_promotion_reward_share = sp_promotion_reward_shares
                    .get(&reward.service_provider)
                    .copied()
                    .unwrap_or_default();
                let service_provider_amount = sp_promotion_reward_share.shares_per_reward * shares;
                let matched_amount = (sp_promotion_reward_share.shares_per_matched_reward * shares)
                    .min(service_provider_amount);
                proto::PromotionReward {
                    entity: Some(reward.rewardable_entity.into()),
                    service_provider_amount: service_provider_amount
                        .round_dp_with_strategy(0, RoundingStrategy::ToZero)
                        .to_u64()
                        .unwrap_or(0),
                    matched_amount: matched_amount
                        .round_dp_with_strategy(0, RoundingStrategy::ToZero)
                        .to_u64()
                        .unwrap_or(0),
                }
            })
            .filter(|x| x.service_provider_amount > 0)
            .map(move |reward| {
                (
                    reward.matched_amount,
                    proto::MobileRewardShare {
                        start_period: reward_period.start.encode_timestamp(),
                        end_period: reward_period.end.encode_timestamp(),
                        reward: Some(proto::mobile_reward_share::Reward::PromotionReward(reward)),
                    },
                )
            })
    }
}
