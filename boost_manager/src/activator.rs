use crate::{db, telemetry};
use anyhow::Result;
use chrono::{DateTime, Utc};
use file_store::{
    file_info_poller::FileInfoStream, reward_manifest::RewardManifest, FileInfo, FileStore,
};
use futures::{future::LocalBoxFuture, stream, StreamExt, TryFutureExt, TryStreamExt};
use helium_proto::{
    services::poc_mobile::{mobile_reward_share::Reward as MobileReward, MobileRewardShare},
    Message,
};
use hextree::Cell;
use mobile_config::{
    boosted_hex_info::BoostedHexes, client::hex_boosting_client::HexBoostingInfoResolver,
};
use poc_metrics::record_duration;
use rust_decimal::Decimal;
use sqlx::{Pool, Postgres, Transaction};
use std::str::FromStr;
use task_manager::ManagedTask;
use tokio::sync::mpsc::Receiver;

pub struct Activator<A> {
    pool: Pool<Postgres>,
    verifier_store: FileStore,
    receiver: Receiver<FileInfoStream<RewardManifest>>,
    hex_boosting_client: A,
}

impl<A> ManagedTask for Activator<A>
where
    A: HexBoostingInfoResolver,
{
    fn start_task(
        self: Box<Self>,
        shutdown: triggered::Listener,
    ) -> LocalBoxFuture<'static, anyhow::Result<()>> {
        let handle = tokio::spawn(self.run(shutdown));
        Box::pin(
            handle
                .map_err(anyhow::Error::from)
                .and_then(|result| async move { result }),
        )
    }
}

impl<A> Activator<A>
where
    A: HexBoostingInfoResolver,
{
    pub async fn new(
        pool: Pool<Postgres>,
        receiver: Receiver<FileInfoStream<RewardManifest>>,
        hex_boosting_client: A,
        verifier_store: FileStore,
    ) -> Result<Self> {
        Ok(Self {
            pool,
            receiver,
            hex_boosting_client,
            verifier_store,
        })
    }

    pub async fn run(mut self, shutdown: triggered::Listener) -> anyhow::Result<()> {
        tracing::info!("starting Activator");
        loop {
            tokio::select! {
            biased;
            _ = shutdown.clone() => break,
            msg = self.receiver.recv() => if let Some(file_info_stream) = msg {
                    let key = &file_info_stream.file_info.key.clone();
                    tracing::info!(file = %key, "Received reward manifest file");

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
        tracing::info!("stopping Activator");
        Ok(())
    }

    async fn handle_rewards(
        &mut self,
        txn: &mut Transaction<'_, Postgres>,
        manifest: RewardManifest,
    ) -> Result<()> {
        // get latest boosted hexes info from mobile config
        let boosted_hexes = BoostedHexes::get_all(&self.hex_boosting_client).await?;

        // get the rewards file from the manifest
        let manifest_time = manifest.end_timestamp;
        let reward_files = stream::iter(
            manifest
                .written_files
                .into_iter()
                .map(|file_name| FileInfo::from_str(&file_name)),
        )
        .boxed();

        // read in the rewards file
        let mut reward_shares = self.verifier_store.source_unordered(5, reward_files);

        while let Some(msg) = reward_shares.try_next().await? {
            let share = MobileRewardShare::decode(msg)?;

            if let Some(MobileReward::RadioRewardV2(r)) = share.reward {
                for hex in r.covered_hexes {
                    if from_proto_decimal(hex.boosted_coverage_points.as_ref())?
                        .map(|d| d > Decimal::ZERO)
                        .unwrap_or(false)
                    {
                        let hex_location = Cell::from_raw(hex.location)?;
                        process_boosted_hex(txn, manifest_time, &boosted_hexes, hex_location)
                            .await?;
                    }
                }
            }
        }
        Ok(())
    }
}

pub async fn process_boosted_hex(
    txn: &mut Transaction<'_, Postgres>,
    manifest_time: DateTime<Utc>,
    boosted_hexes: &BoostedHexes,
    hex_location: Cell,
) -> Result<()> {
    match boosted_hexes.hexes.get(&hex_location) {
        Some(info) => {
            if info.start_ts.is_none() {
                db::insert_activated_hex(
                    txn,
                    hex_location.into_raw(),
                    &info.boosted_hex_pubkey.to_string(),
                    &info.boost_config_pubkey.to_string(),
                    manifest_time,
                )
                .await?;
            }
        }
        None => {
            tracing::warn!(hex = %hex_location, "got an invalid boosted hex");
        }
    }
    Ok(())
}

fn from_proto_decimal(opt: Option<&helium_proto::Decimal>) -> Result<Option<Decimal>> {
    opt.map(|d| Decimal::from_str(&d.value).map_err(anyhow::Error::from))
        .transpose()
}
