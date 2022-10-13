use crate::{
    env_var,
    heartbeats::Heartbeats,
    server::{CONNECT_TIMEOUT, DEFAULT_URI, RPC_TIMEOUT},
    shares::Shares,
    subnetwork_rewards::SubnetworkRewards,
    Result,
};
use chrono::{DateTime, NaiveDateTime, Utc};
use file_store::FileStore;
use helium_crypto::PublicKey;
use helium_proto::services::{follower, Endpoint, Uri};
use serde_json::json;
use sqlx::postgres::PgPoolOptions;

/// Verify the shares for a given time range
#[derive(Debug, clap::Args)]
pub struct Cmd {
    #[clap(long)]
    after: NaiveDateTime,
    #[clap(long)]
    before: NaiveDateTime,
}

impl Cmd {
    pub async fn run(self) -> Result {
        let Self { after, before } = self;

        tracing::info!("Verifying shares from the following time range: {after} to {before}");

        let input_store = FileStore::from_env().await?;

        let follower_service = follower::Client::new(
            Endpoint::from(env_var("FOLLOWER_URI", Uri::from_static(DEFAULT_URI))?)
                .connect_timeout(CONNECT_TIMEOUT)
                .timeout(RPC_TIMEOUT)
                .connect_lazy(),
        );

        let epoch = DateTime::from_utc(after, Utc)..DateTime::from_utc(before, Utc);

        let db_connection_str = dotenv::var("DATABASE_URL")?;

        let pool = PgPoolOptions::new()
            .max_connections(10)
            .connect(&db_connection_str)
            .await?;

        let mut transaction = pool.begin().await?;
        let _shares = Shares::validate_heartbeats(&mut transaction, &input_store, &epoch).await?;
        let heartbeats = Heartbeats::new(&mut transaction, epoch.start).await?;
        let rewards = SubnetworkRewards::from_epoch(follower_service, &epoch, &heartbeats).await?;
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
