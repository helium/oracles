use crate::{env_var, verifier::Verifier, Result};
use chrono::{DateTime, NaiveDateTime, Utc};
use file_store::FileStore;
use helium_crypto::PublicKey;
use helium_proto::services::{follower, Endpoint, Uri};
use serde_json::json;
use sqlx::postgres::PgPoolOptions;

use super::{CONNECT_TIMEOUT, DEFAULT_URI, RPC_TIMEOUT};

/// Verify the shares for a given time range
#[derive(Debug, clap::Args)]
pub struct Cmd {
    #[clap(long)]
    after: NaiveDateTime,
    #[clap(long)]
    before: NaiveDateTime,
    #[clap(long)]
    periods: i32,
}

impl Cmd {
    pub async fn run(self) -> Result {
        let Self {
            after,
            before,
            periods,
        } = self;

        let after = DateTime::from_utc(after, Utc);
        let before = DateTime::from_utc(before, Utc);

        tracing::info!("Verifying shares from the following time range: {after} to {before}");

        let file_store = FileStore::from_env().await?;

        let follower = follower::Client::new(
            Endpoint::from(env_var("FOLLOWER_URI", Uri::from_static(DEFAULT_URI))?)
                .connect_timeout(CONNECT_TIMEOUT)
                .timeout(RPC_TIMEOUT)
                .connect_lazy(),
        );

        let db_connection_str = dotenv::var("DATABASE_URL")?;
        let pool = PgPoolOptions::new()
            .max_connections(10)
            .connect(&db_connection_str)
            .await?;
        sqlx::migrate!().run(&pool).await?;

        let mut verifier = Verifier::new(&pool, file_store, follower).await?;

        let mut transaction = pool.begin().await?;
        let reward_period = before - after;
        let verification_period = reward_period / periods;
        let mut now = after;

        for period in 0..periods {
            let verify_epoch = verifier.epoch_since_last_verify(now);
            tracing::info!("Verifying epoch {period} of {periods}...");
            let _ = verifier
                .verify_epoch(&mut transaction, verify_epoch)
                .await?;
            now += verification_period;
        }

        tracing::info!("Calculating rewards...");

        let rewards = verifier
            .reward_epoch(&mut transaction, after..before)
            .await?;

        transaction.rollback().await?;

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
