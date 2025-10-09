use chrono::{DateTime, Duration, DurationRound, Utc};
use helium_crypto::PublicKey;
use helium_crypto::{KeyTag, Keypair};
use helium_proto::services::mobile_config::{self as proto};
use mobile_config::{
    gateway::service::GatewayService,
    key_cache::{CacheKeys, KeyCache},
    KeyRole,
};
use sqlx::PgPool;
use std::sync::Arc;
use tokio::net::TcpListener;
use tonic::transport;

pub mod gateway_db;
pub mod gateway_metadata_db;
pub mod partial_migrator;

pub fn make_keypair() -> Keypair {
    Keypair::generate(KeyTag::default(), &mut rand::rngs::OsRng)
}

pub async fn spawn_gateway_service(
    pool: PgPool,
    admin_pub_key: PublicKey,
) -> (
    String,
    tokio::task::JoinHandle<std::result::Result<(), transport::Error>>,
) {
    let server_key = make_keypair();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    // Start the gateway server
    let keys = CacheKeys::from_iter([(admin_pub_key.to_owned(), KeyRole::Administrator)]);
    let (_key_cache_tx, key_cache) = KeyCache::new(keys);
    let gws = GatewayService::new(key_cache, pool.clone(), Arc::new(server_key));
    let handle = tokio::spawn(
        transport::Server::builder()
            .add_service(proto::GatewayServer::new(gws))
            .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener)),
    );

    (format!("http://{addr}"), handle)
}

// When retreiving a timestamp from DB, depending on the version of postgres
// the timestamp may be truncated. When comparing datetimes, to ones generated
// in a test with `Utc::now()`, you should truncate it.
pub fn nanos_trunc(ts: DateTime<Utc>) -> DateTime<Utc> {
    ts.duration_trunc(Duration::nanoseconds(1000)).unwrap()
}
