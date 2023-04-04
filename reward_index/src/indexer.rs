use crate::{reward_index, settings, Settings};
use anyhow::{Result, bail};
use file_store::{
    file_info_poller::FileInfoStream, reward_manifest::RewardManifest, FileInfo, FileStore,
};
use futures::{stream, StreamExt, TryStreamExt};
use helium_proto::{
    services::poc_mobile::RadioRewardShare, services::poc_lora::IotRewardShare, Message,
};
use helium_proto::services::poc_lora::iot_reward_share::Reward as ProtoReward;
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

impl Indexer {
    pub async fn new(settings: &Settings, pool: Pool<Postgres>) -> Result<Self> {
        Ok(Self {
            mode: settings.mode,
            verifier_store: FileStore::from_settings(&settings.verifier).await?,
            pool,
            op_fund_key: settings.operation_fund_key(),
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

        let mut hotspot_rewards: HashMap<Vec<u8>, u64> = HashMap::new();

        while let Some(msg) = reward_shares.try_next().await? {
            let (key, amount) = extract_reward_share(&self.mode, &self.op_fund_key, &msg)?;
            *hotspot_rewards.entry(key).or_default() += amount;
        }

        for (key, amount) in hotspot_rewards {
            reward_index::insert(&mut *txn, &key, amount, &manifest_time).await?;
        }

        Ok(())
    }
}

fn extract_reward_share(mode: &settings::Mode, op_fund_key: &[u8], msg: &[u8]) -> Result<(Vec<u8>, u64)> {
    match mode {
        settings::Mode::Mobile => {
            let share = RadioRewardShare::decode(msg)?;
            Ok((share.hotspot_key, share.amount))
        }
        settings::Mode::Iot => {
            let share = IotRewardShare::decode(msg)?;
            match share.reward {
                Some(ProtoReward::GatewayReward(r)) =>  {
                    Ok((
                        r.hotspot_key,
                        r.witness_amount + r.beacon_amount + r.dc_transfer_amount,
                    ))
                },
                Some(ProtoReward::OperationalReward(r)) =>  {
                    Ok((
                        op_fund_key.to_vec(),
                        r.amount
                    ))
                },
                _ => bail!("got an invalid reward share"),
            }
        }
    }
}
