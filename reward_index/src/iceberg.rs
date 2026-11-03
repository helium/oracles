//! Index rewards from the `rewards` Iceberg tables written by mobile-verifier.
//!
//! Each table is streamed snapshot-by-snapshot by its own
//! [`helium_iceberg::stream`] poller. The rewarder appends one snapshot per
//! table per epoch, and the tables are independent of each other here (every
//! row is just an amount added to a key), so a committed snapshot is all the
//! signal needed that an epoch's rewards for that table are complete.
//!
//! The stream is at-least-once: a crash after the Postgres commit but before
//! the watermark is recorded re-streams the snapshot. Rows are grouped by epoch
//! and each group is recorded in `processed_writes` in the same transaction as
//! its reward inserts, so a re-streamed epoch is skipped rather than counted
//! twice.

use crate::{
    db,
    reward::{RewardKey, RewardType},
    settings::IcebergSettings,
    telemetry,
};
use anyhow::Result;
use chrono::{DateTime, Utc};
use helium_iceberg::{Catalog, IcebergEvent, IcebergStreamPollerServer};
use helium_iceberg_oracles::rewards::{
    self, IcebergGatewayReward, IcebergServiceProviderReward, IcebergUnallocatedReward,
};
use serde::de::DeserializeOwned;
use sqlx::{PgPool, Postgres, Transaction};
use std::collections::{BTreeMap, HashMap};
use tokio::sync::mpsc::Receiver;

const PROCESS_NAME: &str = "reward_index";

/// A row of one of the `rewards` tables.
pub trait IcebergReward: DeserializeOwned + Send + Sync + 'static {
    const TABLE_NAME: &'static str;

    fn start_period(&self) -> DateTime<Utc>;

    fn end_period(&self) -> DateTime<Utc>;

    fn reward(&self, unallocated_reward_key: &str) -> (RewardKey, u64);
}

impl IcebergReward for IcebergGatewayReward {
    const TABLE_NAME: &'static str = rewards::data_transfer::TABLE_NAME;

    fn start_period(&self) -> DateTime<Utc> {
        self.start_period.to_utc()
    }

    fn end_period(&self) -> DateTime<Utc> {
        self.end_period.to_utc()
    }

    fn reward(&self, _unallocated_reward_key: &str) -> (RewardKey, u64) {
        (
            RewardKey {
                key: self.hotspot_key.clone(),
                reward_type: RewardType::MobileGateway,
            },
            self.dc_transfer_reward,
        )
    }
}

impl IcebergReward for IcebergServiceProviderReward {
    const TABLE_NAME: &'static str = rewards::service_provider::TABLE_NAME;

    fn start_period(&self) -> DateTime<Utc> {
        self.start_period.to_utc()
    }

    fn end_period(&self) -> DateTime<Utc> {
        self.end_period.to_utc()
    }

    fn reward(&self, _unallocated_reward_key: &str) -> (RewardKey, u64) {
        (
            RewardKey {
                key: self.rewardable_entity_key.clone(),
                reward_type: RewardType::MobileServiceProvider,
            },
            self.amount,
        )
    }
}

impl IcebergReward for IcebergUnallocatedReward {
    const TABLE_NAME: &'static str = rewards::unallocated::TABLE_NAME;

    fn start_period(&self) -> DateTime<Utc> {
        self.start_period.to_utc()
    }

    fn end_period(&self) -> DateTime<Utc> {
        self.end_period.to_utc()
    }

    fn reward(&self, unallocated_reward_key: &str) -> (RewardKey, u64) {
        (
            RewardKey {
                key: unallocated_reward_key.to_string(),
                reward_type: RewardType::MobileUnallocated,
            },
            self.amount,
        )
    }
}

/// Identifies one table's rewards for one epoch, independent of the snapshot
/// that carried them.
pub fn write_id<T: IcebergReward>(start_period: DateTime<Utc>) -> String {
    format!("{}-{}", T::TABLE_NAME, start_period.to_rfc3339())
}

/// Index a batch of rows from `T`'s table.
///
/// Rows are grouped by epoch. Epochs starting before `start_epoch_period` are
/// skipped, as are epochs already recorded in `processed_writes`. Returns the
/// latest `end_period` indexed, if any.
pub async fn handle_rewards<T: IcebergReward>(
    txn: &mut Transaction<'_, Postgres>,
    rows: Vec<T>,
    unallocated_reward_key: &str,
    start_epoch_period: DateTime<Utc>,
) -> Result<Option<DateTime<Utc>>> {
    let mut epochs: BTreeMap<DateTime<Utc>, Vec<T>> = BTreeMap::new();
    for row in rows {
        epochs.entry(row.start_period()).or_default().push(row);
    }

    let mut indexed_through = None;
    for (start_period, rows) in epochs {
        if start_period < start_epoch_period {
            tracing::debug!(
                table = T::TABLE_NAME,
                %start_period,
                "skipping epoch before iceberg start epoch"
            );
            continue;
        }

        let write_id = write_id::<T>(start_period);
        if !db::insert_processed_write(&mut **txn, &write_id).await? {
            tracing::info!(write_id, "epoch already indexed, skipping");
            continue;
        }

        let mut end_period = start_period;
        let mut rewards: HashMap<RewardKey, u64> = HashMap::new();
        for row in &rows {
            end_period = end_period.max(row.end_period());
            let (key, amount) = row.reward(unallocated_reward_key);
            *rewards.entry(key).or_default() += amount;
        }

        for (reward_key, amount) in rewards {
            db::insert(
                &mut **txn,
                reward_key.key,
                amount,
                reward_key.reward_type,
                &end_period,
            )
            .await?;
        }

        tracing::info!(write_id, rows = rows.len(), "indexed epoch");
        indexed_through = indexed_through.max(Some(end_period));
    }

    Ok(indexed_through)
}

/// Consumes one table's snapshot stream into the reward index.
pub struct IcebergIndexer<T> {
    pool: PgPool,
    receiver: Receiver<IcebergEvent<T>>,
    unallocated_reward_key: String,
    start_epoch_period: DateTime<Utc>,
}

impl<T: IcebergReward> task_manager::ManagedTask for IcebergIndexer<T> {
    fn start_task(self: Box<Self>, shutdown: triggered::Listener) -> task_manager::TaskFuture {
        task_manager::spawn(self.run(shutdown))
    }
}

impl<T: IcebergReward> IcebergIndexer<T> {
    pub async fn run(mut self, shutdown: triggered::Listener) -> Result<()> {
        tracing::info!(table = T::TABLE_NAME, "starting iceberg indexer");

        loop {
            tokio::select! {
                biased;
                _ = shutdown.clone() => {
                    tracing::info!(table = T::TABLE_NAME, "iceberg indexer shutting down");
                    return Ok(());
                }
                msg = self.receiver.recv() => match msg {
                    Some(event) => self.handle_event(event).await?,
                    None => anyhow::bail!("{} snapshot stream closed", T::TABLE_NAME),
                }
            }
        }
    }

    async fn handle_event(&self, mut event: IcebergEvent<T>) -> Result<()> {
        tracing::info!(
            table = T::TABLE_NAME,
            sequence_number = event.snapshot.sequence_number,
            rows = event.len(),
            "processing snapshot"
        );

        let rows = event.take_rows();
        let mut txn = self.pool.begin().await?;
        let indexed_through = handle_rewards(
            &mut txn,
            rows,
            &self.unallocated_reward_key,
            self.start_epoch_period,
        )
        .await?;
        txn.commit().await?;

        // Only advance the watermark once the rewards are committed.
        event.commit().await?;

        if let Some(end_period) = indexed_through {
            telemetry::last_reward_processed_time(&self.pool, end_period).await?;
        }
        Ok(())
    }
}

/// Create the snapshot poller and indexer for `T`'s table.
pub async fn create<T: IcebergReward>(
    catalog: Catalog,
    settings: &IcebergSettings,
    pool: PgPool,
    unallocated_reward_key: String,
) -> Result<(IcebergStreamPollerServer<T>, IcebergIndexer<T>)> {
    let mut builder = helium_iceberg::continuous::<T>()
        .catalog(catalog)
        .namespace(rewards::NAMESPACE)
        .table(T::TABLE_NAME)
        .process_name(PROCESS_NAME.to_string())
        .poll_duration(settings.poll_duration)
        .lookback_start_after(settings.snapshot_start_after)
        // Rewards are only ever appended. Stop on anything else rather than
        // guess whether its rows were already indexed.
        .skip_non_append(false);
    if let Some(db_dir) = &settings.db_dir {
        builder = builder.db_dir(db_dir.clone());
    }

    let (receiver, server) = builder.create().await?;
    let indexer = IcebergIndexer {
        pool,
        receiver,
        unallocated_reward_key,
        start_epoch_period: settings.start_epoch_period,
    };
    Ok((server, indexer))
}
