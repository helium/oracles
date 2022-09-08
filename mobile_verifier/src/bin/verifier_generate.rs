use chrono::{DateTime, NaiveDateTime, Utc};
use clap::Parser;
use helium_proto::{
    follower_client::FollowerClient,
    services::{Endpoint, Uri},
};
use mobile_verifier::{
    env_var,
    server::{CONNECT_TIMEOUT, DEFAULT_URI, RPC_TIMEOUT},
    subnetwork_rewards::SubnetworkRewards,
    Result,
};
use poc_store::FileStore;

/// Verify the shares for a given time range
#[derive(clap::Parser)]
#[clap(version = env!("CARGO_PKG_VERSION"))]
#[clap(about = "Verify Shares")]
pub struct Cli {
    #[clap(long)]
    after: NaiveDateTime,
    #[clap(long)]
    before: NaiveDateTime,
    #[clap(long)]
    input_bucket: String,
    #[clap(long)]
    output_bucket: String,
}

#[tokio::main]
async fn main() -> Result {
    dotenv::dotenv()?;
    tracing_subscriber::fmt::init();

    let Cli {
        after,
        before,
        input_bucket,
        output_bucket,
    } = Cli::parse();

    tracing::info!(
        "Verifying shares from bucket {input_bucket} within the following time range: {after} to {before}"
    );

    let input_store = FileStore::new(None, "us-west-2", input_bucket).await?;
    let output_store = FileStore::new(None, "us-west-2", output_bucket).await?;

    let follower_service = FollowerClient::new(
        Endpoint::from(env_var("FOLLOWER_URI", Uri::from_static(DEFAULT_URI))?)
            .connect_timeout(CONNECT_TIMEOUT)
            .timeout(RPC_TIMEOUT)
            .connect_lazy(),
    );

    SubnetworkRewards::from_period(
        &input_store,
        &output_store,
        follower_service,
        DateTime::from_utc(after, Utc),
        DateTime::from_utc(before, Utc),
    )
    .await?;

    Ok(())
}
