use crate::{
    reward_shares::TransferRewards,
    speedtests::EmptyDatabase,
    verifier::{VerifiedEpoch, Verifier},
    Settings,
};
use anyhow::Result;
use chrono::{DateTime, NaiveDateTime, Utc};
use file_store::FileStore;
use futures::stream::StreamExt;
use helium_crypto::PublicKey;
use serde_json::json;
use std::collections::HashMap;

/// Verify the shares for a given time range
#[derive(Debug, clap::Args)]
pub struct Cmd {
    #[clap(long)]
    start: NaiveDateTime,
    #[clap(long)]
    end: NaiveDateTime,
}

impl Cmd {
    pub async fn run(self, settings: &Settings) -> Result<()> {
        let Self { start, end } = self;

        let start = DateTime::from_utc(start, Utc);
        let end = DateTime::from_utc(end, Utc);

        tracing::info!("Verifying shares from the following time range: {start} to {end}");
        let epoch = start..end;

        let file_store = FileStore::from_settings(&settings.ingest).await?;
        let follower = settings.follower.connect_follower();
        let mut verifier = Verifier::new(file_store, follower);

        let VerifiedEpoch {
            heartbeats,
            speedtests,
        } = verifier.verify_epoch(EmptyDatabase, &epoch).await?;

        let reward_shares = verifier
            .reward_epoch(
                heartbeats.collect().await,
                speedtests.filter_map(|x| async { x.ok() }).collect().await,
            )
            .await?;

        let mut total_rewards = 0_u64;
        let mut owner_rewards = HashMap::<_, u64>::new();
        let transfer_rewards = TransferRewards::empty();
        for reward in reward_shares.into_radio_shares(&transfer_rewards, &epoch) {
            total_rewards += reward.amount;
            *owner_rewards
                .entry(PublicKey::try_from(reward.owner_key)?)
                .or_default() += reward.amount;
        }

        let rewards: Vec<_> = owner_rewards.into_iter().collect();

        println!(
            "{}",
            serde_json::to_string_pretty(&json!({
                "rewards": rewards,
                "total_rewards": total_rewards,
            }))?
        );

        Ok(())
    }
}
