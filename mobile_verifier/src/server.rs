use crate::{
    env_var,
    error::{Error, Result},
    subnetwork_rewards::SubnetworkRewards,
};
use chrono::{DateTime, Duration, NaiveDateTime, Utc};
use db_store::MetaValue;
use file_store::{file_sink, file_upload, FileStore, FileType};
use futures_util::TryFutureExt;
use helium_proto::services::{follower, Endpoint, Uri};
use sqlx::postgres::PgPoolOptions;
use tokio::{select, time::sleep};

pub const CONNECT_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(5);
pub const RPC_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(5);
pub const DEFAULT_URI: &str = "http://127.0.0.1:8080";

/// Default hours to delay lookup from now
pub const DEFAULT_LOOKUP_DELAY: i64 = 3;

pub async fn run_server(shutdown: triggered::Listener) -> Result {
    tracing::info!("Starting verifier service");

    let db_connection_str = dotenv::var("DATABASE_URL")?;
    let pool = PgPoolOptions::new()
        .max_connections(10)
        .connect(&db_connection_str)
        .await?;

    let (file_upload_tx, file_upload_rx) = file_upload::message_channel();
    let file_upload =
        file_upload::FileUpload::from_env_with_prefix("OUTPUT", file_upload_rx).await?;

    let store_path = dotenv::var("VERIFIER_STORE")?;
    let store_base_path = std::path::Path::new(&store_path);

    // valid shares
    let (shares_tx, shares_rx) = file_sink::message_channel(50);
    let mut shares_sink =
        file_sink::FileSinkBuilder::new(FileType::Shares, store_base_path, shares_rx)
            .deposits(Some(file_upload_tx.clone()))
            .create()
            .await?;

    // invalid shares
    let (invalid_shares_tx, invalid_shares_rx) = file_sink::message_channel(50);
    let mut invalid_shares_sink = file_sink::FileSinkBuilder::new(
        FileType::InvalidShares,
        store_base_path,
        invalid_shares_rx,
    )
    .deposits(Some(file_upload_tx.clone()))
    .create()
    .await?;

    // subnetwork rewards
    let (subnet_tx, subnet_rx) = file_sink::message_channel(50);
    let mut subnet_sink =
        file_sink::FileSinkBuilder::new(FileType::SubnetworkRewards, store_base_path, subnet_rx)
            .deposits(Some(file_upload_tx.clone()))
            .create()
            .await?;

    let file_store = FileStore::from_env_with_prefix("INPUT").await?;

    let follower_client = follower::Client::new(
        Endpoint::from(env_var("FOLLOWER_URI", Uri::from_static(DEFAULT_URI))?)
            .connect_timeout(CONNECT_TIMEOUT)
            .timeout(RPC_TIMEOUT)
            .connect_lazy(),
    );

    let mut last_verified_end_time =
        MetaValue::<i64>::fetch_or_insert_with(&pool, "last_verified_end_time", || {
            env_var("LAST_VERIFIED_END_TIME", 0).unwrap()
        })
        .await?;
    let lookup_delay = env_var("LOOKUP_DELAY", DEFAULT_LOOKUP_DELAY)?;

    let shutdown_clone = shutdown.clone();
    let server = tokio::spawn(async move {
        loop {
            let (start, stop) = get_time_range(*last_verified_end_time.value(), lookup_delay);
            SubnetworkRewards::from_period(&file_store, follower_client.clone(), start, stop)
                .await?
                .write(&shares_tx, &invalid_shares_tx, &subnet_tx)
                .await?;
            last_verified_end_time
                .update(&pool, stop.timestamp())
                .await?;
            select! {
                _ = sleep(std::time::Duration::from_secs(lookup_delay as u64 * 60 * 60)) => continue,
                _ = shutdown_clone.clone() => break,
            }
        }

        Ok(())
    });

    tokio::try_join!(
        flatten(server),
        shares_sink.run(&shutdown).map_err(Error::from),
        invalid_shares_sink.run(&shutdown).map_err(Error::from),
        subnet_sink.run(&shutdown).map_err(Error::from),
        file_upload.run(&shutdown).map_err(Error::from),
    )?;

    Ok(())
}

fn get_time_range(
    last_reward_end_time: i64, // DateTime<Utc>,
    lookup_delay: i64,
) -> (DateTime<Utc>, DateTime<Utc>) {
    let after_utc =
        DateTime::<Utc>::from_utc(NaiveDateTime::from_timestamp(last_reward_end_time, 0), Utc);
    let now = Utc::now();
    let stop_utc = now - Duration::hours(lookup_delay);
    let start_utc = after_utc.min(stop_utc);
    (start_utc, stop_utc)
}

async fn flatten(handle: tokio::task::JoinHandle<Result>) -> Result {
    match handle.await {
        Ok(Ok(result)) => Ok(result),
        Ok(Err(err)) => Err(err),
        Err(err) => Err(Error::JoinError(err)),
    }
}
