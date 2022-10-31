use crate::{
    env_var,
    speedtests::EmptyDatabase,
    verifier::{VerifiedEpoch, Verifier},
    Result,
};
use chrono::{DateTime, NaiveDateTime, Utc};
use file_store::FileStore;
use helium_crypto::PublicKey;
use helium_proto::services::{follower, Endpoint, Uri};
use serde_json::json;
use rust_decimal::Decimal;
use rust_decimal_macros::dec;

use super::{CONNECT_TIMEOUT, DEFAULT_URI, RPC_TIMEOUT};

/// Verify the shares for a given time range
#[derive(Debug, clap::Args)]
pub struct Cmd {
    #[clap(long)]
    start: NaiveDateTime,
    #[clap(long)]
    end: NaiveDateTime,
}

impl Cmd {
    pub async fn run(self) -> Result {
        let Self { start, end } = self;

        let start = DateTime::from_utc(start, Utc);
        let end = DateTime::from_utc(end, Utc);

        tracing::info!("Verifying shares from the following time range: {start} to {end}");
        let epoch = start..end;

        let file_store = FileStore::from_env_with_prefix("INPUT").await?;

        let follower = follower::Client::new(
            Endpoint::from(env_var("FOLLOWER_URI", Uri::from_static(DEFAULT_URI))?)
                .connect_timeout(CONNECT_TIMEOUT)
                .timeout(RPC_TIMEOUT)
                .connect_lazy(),
        );

        let mut verifier = Verifier::new(file_store, follower).await?;

        let VerifiedEpoch {
            heartbeats,
            speedtests,
        } = verifier.verify_epoch(EmptyDatabase, &epoch).await?;

        let rewards = verifier
            .reward_epoch(&epoch, heartbeats.into_iter().collect(), speedtests)
            .await?;

        let reward_percent_sum = rewards
            .rewards
            .iter()
            .fold(dec!(0.0), |acc, reward| acc + reward.reward_percent.clone().map(Decimal::from).unwrap_or_default());

        let rewards: Vec<(PublicKey, _)> = rewards
            .rewards
            .into_iter()
            .map(|r| {
                (
                    PublicKey::try_from(r.account.as_slice()).expect("unable to get public key"),
                    r.reward_percent.map(Decimal::from).unwrap_or_default(),
                )
            })
            .collect();

        println!(
            "{}",
            serde_json::to_string_pretty(&json!({
                "rewards": rewards,
                "reward_percent_sum": reward_percent_sum
            }))?
        );

        Ok(())
    }
}
