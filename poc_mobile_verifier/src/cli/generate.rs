use crate::{env_var, heartbeats::Heartbeat, verifier::Verifier, Result};
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

        let mut verifier = Verifier::new(file_store, follower).await?;

        let heartbeats: Vec<Heartbeat> = verifier
            .verify_epoch(&epoch)
            .await?
            .valid_shares
            .into_iter()
            .map(Heartbeat::from)
            .collect();

        let rewards = verifier.reward_epoch(&epoch, heartbeats).await?;

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
