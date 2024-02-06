use crate::{db, telemetry, Settings};
use anyhow::Result;
use chrono::{DateTime, Utc};
use file_store::{
    file_info_poller::FileInfoStream, reward_manifest::RewardManifest, FileInfo, FileStore,
};
use futures::{future::LocalBoxFuture, stream, StreamExt, TryFutureExt, TryStreamExt};
use helium_proto::{
    services::poc_mobile::{
        mobile_reward_share::Reward as MobileReward, BoostedHex as BoostedHexProto,
        MobileRewardShare,
    },
    Message,
};
use mobile_config::{
    boosted_hex_info::BoostedHexes,
    client::{hex_boosting_client::HexBoostingInfoResolver, ClientError},
};
use poc_metrics::record_duration;
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
    A: HexBoostingInfoResolver<Error = ClientError>,
{
    fn start_task(
        self: Box<Self>,
        shutdown: triggered::Listener,
    ) -> LocalBoxFuture<'static, anyhow::Result<()>> {
        let handle = tokio::spawn(self.run(shutdown));
        Box::pin(
            handle
                .map_err(anyhow::Error::from)
                .and_then(|result| async move { result.map_err(anyhow::Error::from) }),
        )
    }
}

impl<A> Activator<A>
where
    A: HexBoostingInfoResolver<Error = ClientError>,
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
            if let Some(MobileReward::RadioReward(r)) = share.reward {
                for hex in r.boosted_hexes.into_iter() {
                    self.process_boosted_hex(txn, manifest_time, &boosted_hexes, &hex)
                        .await?
                }
            }
        }
        Ok(())
    }

    pub async fn process_boosted_hex(
        &mut self,
        txn: &mut Transaction<'_, Postgres>,
        manifest_time: DateTime<Utc>,
        boosted_hexes: &BoostedHexes,
        hex: &BoostedHexProto,
    ) -> Result<()> {
        match boosted_hexes.hexes.get(&hex.location) {
            Some(info) => {
                if info.start_ts.is_none() {
                    db::insert_activated_hex(
                        txn,
                        hex.location,
                        &info.boosted_hex_pubkey,
                        &info.boost_config_pubkey,
                        manifest_time,
                    )
                    .await?;
                }
            }
            None => {
                tracing::warn!(hex = %hex.location, "got an invalid boosted hex");
            }
        }
        Ok(())
    }
}
