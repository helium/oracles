use crate::{db, extract, settings, telemetry, Settings};
use anyhow::{bail, Result};
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
        let (key, amount) = extract::iot_reward(share, op_fund_key, unallocated_reward_key)?;
        *rewards.entry(key).or_default() += amount;
    }

    for (reward_key, amount) in rewards {
        db::insert(
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
        match extract::mobile_reward(share, unallocated_reward_key) {
            Ok((key, amount)) => {
                *rewards.entry(key).or_default() += amount;
            }
            Err(extract::ExtractError::UnsupportedType(unsupported)) => {
                tracing::debug!("ignoring unsupported: {unsupported}");
            }
            Err(err) => bail!(err),
        }
    }

    for (reward_key, amount) in rewards {
        db::insert(
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
