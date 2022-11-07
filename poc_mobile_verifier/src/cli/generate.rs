use crate::{
    speedtests::EmptyDatabase,
    verifier::{VerifiedEpoch, Verifier},
    Result, Settings,
};
use chrono::{DateTime, NaiveDateTime, Utc};
use file_store::FileStore;
use futures::stream::StreamExt;
use helium_crypto::PublicKey;
use serde_json::json;

/// Verify the shares for a given time range
#[derive(Debug, clap::Args)]
pub struct Cmd {
    #[clap(long)]
    start: NaiveDateTime,
    #[clap(long)]
    end: NaiveDateTime,
}

impl Cmd {
    pub async fn run(self, settings: &Settings) -> Result {
        let Self { start, end } = self;

        let start = DateTime::from_utc(start, Utc);
        let end = DateTime::from_utc(end, Utc);

        tracing::info!("Verifying shares from the following time range: {start} to {end}");
        let epoch = start..end;

        let file_store = FileStore::from_settings(&settings.ingest).await?;
        let follower = settings.follower.connect_follower()?;

        let mut verifier = Verifier::new(file_store, follower).await?;

        let VerifiedEpoch {
            heartbeats,
            speedtests,
        } = verifier.verify_epoch(EmptyDatabase, &epoch).await?;

        let rewards = verifier
            .reward_epoch(
                &epoch,
                heartbeats.collect().await,
                speedtests.filter_map(|x| async { x.ok() }).collect().await,
            )
            .await?;

        let total_rewards = rewards
            .rewards
            .iter()
            .fold(0, |acc, reward| acc + reward.amount);
        let rewards: Vec<(PublicKey, u64)> = rewards
            .rewards
            .iter()
            .map(|r| {
                (
                    PublicKey::try_from(r.account.as_slice()).expect("unable to get public key"),
                    r.amount,
                )
            })
            .collect();

        println!(
            "{}",
            serde_json::to_string_pretty(&json!({
                "rewards": rewards,
                "total_rewards": total_rewards,
            }))
            .unwrap()
        );

        Ok(())
    }
}
