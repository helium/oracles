use chrono::{DateTime, Duration, DurationRound, Utc};
use helium_crypto::PublicKey;
use helium_crypto::{KeyTag, Keypair};
use helium_proto::services::iot_config::{self as proto};
use iot_config::admin::AuthCache;
use iot_config::region_map::RegionMapReader;
use iot_config::{org, GatewayService};
use sqlx::PgPool;
use std::sync::Arc;
use tokio::net::TcpListener;
use tonic::transport;

pub mod gateway_metadata_db;

pub fn make_keypair() -> Keypair {
    Keypair::generate(KeyTag::default(), &mut rand::rngs::OsRng)
}

pub async fn spawn_gateway_service(
    pool: PgPool,
    admin_pub_key: PublicKey,
) -> anyhow::Result<(
    String,
    tokio::task::JoinHandle<std::result::Result<(), transport::Error>>,
)> {
    let server_key = make_keypair();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    let (_auth_updater, auth_cache) = AuthCache::new(admin_pub_key.clone(), &pool).await?;
    let (_region_updater, region_map) = RegionMapReader::new(&pool).await?;
    let (_delegate_key_updater, delegate_key_cache) = org::delegate_keys_cache(&pool).await?;

    // Start the gateway server
    let gws = GatewayService::new(
        Arc::new(server_key),
        pool.clone(),
        region_map.clone(),
        auth_cache.clone(),
        delegate_key_cache,
    )?;

    let handle = tokio::spawn(
        transport::Server::builder()
            .add_service(proto::GatewayServer::new(gws))
            .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener)),
    );

    Ok((format!("http://{addr}"), handle))
}

// When retrieving a timestamp from DB, depending on the version of postgres
// the timestamp may be truncated. When comparing datetimes, to ones generated
// in a test with `Utc::now()`, you should truncate it.
pub fn nanos_trunc(ts: DateTime<Utc>) -> DateTime<Utc> {
    ts.duration_trunc(Duration::nanoseconds(1000)).unwrap()
}
