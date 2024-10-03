use std::time::{Duration, Instant};

use chrono::Utc;
use file_store::{
    file_info_poller::{FileInfoStream, LookbackBehavior},
    file_sink::FileSinkClient,
    file_source,
    file_upload::FileUpload,
    promotion_reward::{Entity, PromotionReward},
    traits::{FileSinkCommitStrategy, FileSinkRollTime, FileSinkWriteExt, TimestampEncode},
    FileType,
};
use futures::{StreamExt, TryFutureExt};
use helium_proto::{
    services::{
        mobile_config::NetworkKeyRole,
        poc_mobile::{
            PromotionRewardIngestReportV1, PromotionRewardStatus, VerifiedPromotionRewardV1,
        },
    },
    ServiceProviderPromotionFundV1,
};
use mobile_config::{
    client::{
        authorization_client::AuthorizationVerifier, entity_client::EntityVerifier,
        AuthorizationClient, EntityClient,
    },
    GatewayClient,
};
use sqlx::PgPool;
use task_manager::{ManagedTask, TaskManager};
use tokio::sync::mpsc::Receiver;

use crate::{
    service_provider::promotions::{funds, rewards},
    GatewayResolver, Settings,
};

pub struct PromotionDaemon {
    pool: PgPool,
    gateway_info_resolver: GatewayClient,
    authorization_verifier: AuthorizationClient,
    entity_verifier: EntityClient,
    promotion_funds: Receiver<FileInfoStream<ServiceProviderPromotionFundV1>>,
    promotion_rewards: Receiver<FileInfoStream<PromotionReward>>,
    promotion_rewards_sink: FileSinkClient<VerifiedPromotionRewardV1>,
}

impl ManagedTask for PromotionDaemon {
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

impl PromotionDaemon {
    #[allow(clippy::too_many_arguments)]
    pub async fn create_managed_task(
        pool: PgPool,
        settings: &Settings,
        file_upload: FileUpload,
        report_file_store: file_store::FileStore,
        promotion_file_store: file_store::FileStore,
        gateway_info_resolver: GatewayClient,
        authorization_verifier: AuthorizationClient,
        entity_verifier: EntityClient,
    ) -> anyhow::Result<impl ManagedTask> {
        let (promotion_rewards_sink, valid_promotion_rewards_server) =
            VerifiedPromotionRewardV1::file_sink(
                settings.store_base_path(),
                file_upload.clone(),
                FileSinkCommitStrategy::Automatic,
                FileSinkRollTime::Duration(Duration::from_secs(15 * 60)),
                env!("CARGO_PKG_NAME"),
            )
            .await?;

        let (promotion_rewards, promotion_rewards_server) =
            file_source::Continuous::msg_source::<PromotionReward, _>()
                .state(pool.clone())
                .store(report_file_store.clone())
                .lookback(LookbackBehavior::StartAfter(settings.start_after))
                .prefix(FileType::PromotionRewardIngestReport.to_string())
                .create()
                .await?;

        let (promotion_funds, promotion_funds_server) =
            file_source::Continuous::prost_source::<ServiceProviderPromotionFundV1, _>()
                .state(pool.clone())
                .store(promotion_file_store)
                .lookback(LookbackBehavior::StartAfter(settings.start_after))
                .prefix(FileType::ServiceProviderPromotionFund.to_string())
                .create()
                .await?;

        let promotion_reward_daemon = Self {
            pool,
            gateway_info_resolver,
            authorization_verifier,
            entity_verifier,
            promotion_funds,
            promotion_rewards,
            promotion_rewards_sink,
        };

        Ok(TaskManager::builder()
            .add_task(valid_promotion_rewards_server)
            .add_task(promotion_funds_server)
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
                    self.process_rewards_file(file).await?;
                    metrics::histogram!("promotion_reward_processing_time").record(start.elapsed());
                }
                Some(file) = self.promotion_funds.recv() => {
                    let start = Instant::now();
                    self.process_funds_file(file).await?;
                    metrics::histogram!("promotion_funds_processing_time").record(start.elapsed());
                }
            }
        }

        Ok(())
    }

    async fn process_rewards_file(
        &self,
        file: FileInfoStream<PromotionReward>,
    ) -> anyhow::Result<()> {
        tracing::info!(key = file.file_info.key, "Processing promotion reward file");

        let mut transaction = self.pool.begin().await?;
        let mut promotion_rewards = file.into_stream(&mut transaction).await?;

        while let Some(promotion_reward) = promotion_rewards.next().await {
            let promotion_reward_status = validate_promotion_reward(
                &promotion_reward,
                &self.authorization_verifier,
                &self.gateway_info_resolver,
                &self.entity_verifier,
            )
            .await?;

            if promotion_reward_status == PromotionRewardStatus::Valid {
                rewards::save_promotion_reward(&mut transaction, &promotion_reward).await?;
            }

            write_promotion_reward(
                &self.promotion_rewards_sink,
                &promotion_reward,
                promotion_reward_status,
            )
            .await?;
        }

        self.promotion_rewards_sink.commit().await?;
        transaction.commit().await?;

        Ok(())
    }

    async fn process_funds_file(
        &self,
        file: FileInfoStream<ServiceProviderPromotionFundV1>,
    ) -> anyhow::Result<()> {
        tracing::info!(key = file.file_info.key, "Processing promotion funds file");

        let mut txn = self.pool.begin().await?;

        let mut promotion_funds = file.into_stream(&mut txn).await?;
        while let Some(promotion_fund) = promotion_funds.next().await {
            funds::save_promotion_fund(
                &mut txn,
                promotion_fund.service_provider,
                promotion_fund.bps as u16,
            )
            .await?;
        }

        txn.commit().await?;

        Ok(())
    }
}

async fn validate_promotion_reward(
    promotion_reward: &PromotionReward,
    authorization_verifier: &impl AuthorizationVerifier,
    gateway_info_resolver: &impl GatewayResolver,
    entity_verifier: &impl EntityVerifier,
) -> anyhow::Result<PromotionRewardStatus> {
    if authorization_verifier
        .verify_authorized_key(
            &promotion_reward.carrier_pub_key,
            NetworkKeyRole::MobileCarrier,
        )
        .await
        .is_err()
    {
        return Ok(PromotionRewardStatus::InvalidCarrierKey);
    }
    match &promotion_reward.entity {
        Entity::SubscriberId(ref subscriber_id)
            if entity_verifier
                .verify_rewardable_entity(subscriber_id)
                .await
                .is_err() =>
        {
            Ok(PromotionRewardStatus::InvalidSubscriberId)
        }
        Entity::GatewayKey(ref gateway_key)
            if gateway_info_resolver
                .resolve_gateway(gateway_key)
                .await?
                .is_not_found() =>
        {
            Ok(PromotionRewardStatus::InvalidGatewayKey)
        }
        _ => Ok(PromotionRewardStatus::Valid),
    }
}

async fn write_promotion_reward(
    file_sink: &FileSinkClient<VerifiedPromotionRewardV1>,
    promotion_reward: &PromotionReward,
    status: PromotionRewardStatus,
) -> anyhow::Result<()> {
    file_sink
        .write(
            VerifiedPromotionRewardV1 {
                report: Some(PromotionRewardIngestReportV1 {
                    received_timestamp: promotion_reward
                        .received_timestamp
                        .encode_timestamp_millis(),
                    report: Some(promotion_reward.clone().into()),
                }),
                status: status as i32,
                timestamp: Utc::now().encode_timestamp_millis(),
            },
            &[("validity", status.as_str_name())],
        )
        .await?;
    Ok(())
}
