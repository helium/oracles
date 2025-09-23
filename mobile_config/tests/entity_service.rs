use std::{sync::Arc, vec};

use helium_crypto::{PublicKey, Sign};
use helium_proto::services::mobile_config::{self as proto, EntityClient, EntityVerifyReqV1};
use mobile_config::{
    entity_service::EntityService,
    key_cache::{CacheKeys, KeyCache},
    KeyRole,
};
use prost::Message;
use sqlx::PgPool;
use tokio::net::TcpListener;
use tonic::transport;

pub mod common;
use common::*;

async fn spawn_entity_service(
    pool: PgPool,
    admin_pub_key: PublicKey,
) -> (
    String,
    tokio::task::JoinHandle<std::result::Result<(), transport::Error>>,
) {
    let server_key = make_keypair();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    // Start the entity server
    let keys = CacheKeys::from_iter([(admin_pub_key.to_owned(), KeyRole::Administrator)]);
    let (_key_cache_tx, key_cache) = KeyCache::new(keys);
    let gws = EntityService::new(key_cache, pool.clone(), Arc::new(server_key));
    let handle = tokio::spawn(
        transport::Server::builder()
            .add_service(proto::EntityServer::new(gws))
            .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener)),
    );

    (format!("http://{addr}"), handle)
}

#[sqlx::test(fixtures("key_to_assets_es.sql"))]
async fn entity_service_verify(pool: PgPool) {
    let admin_key = make_keypair();
    let (addr, _handle) = spawn_entity_service(pool.clone(), admin_key.public_key().clone()).await;
    let mut client = EntityClient::connect(addr).await.unwrap();
    // Case 1: utf8 serialization
    let mut req = EntityVerifyReqV1 {
        entity_id: "Helium Mobile Mapping Rewards".to_string().into_bytes(),
        signer: admin_key.public_key().clone().into(),
        signature: vec![],
    };
    req.signature = admin_key.sign(&req.encode_to_vec()).unwrap();

    // Should not fail (return Ok message)
    client.verify(req).await.unwrap();

    // Case 2: b58 serialization
    let entity_bytes = bs58::decode("112d6JcLCyi5cs5BCXhK22VAgEAQNRvKkP9KW5XHeRAQpHoHhFyF")
        .into_vec()
        .unwrap();

    let mut req = EntityVerifyReqV1 {
        entity_id: entity_bytes,
        signer: admin_key.public_key().clone().into(),
        signature: vec![],
    };
    req.signature = admin_key.sign(&req.encode_to_vec()).unwrap();
    // Should not fail (return Ok message)
    client.verify(req).await.unwrap();

    // Case 3: entity doesn't exist
    let mut req = EntityVerifyReqV1 {
        entity_id: "nonexistent".to_string().into_bytes(),
        signer: admin_key.public_key().clone().into(),
        signature: vec![],
    };
    req.signature = admin_key.sign(&req.encode_to_vec()).unwrap();
    // Should return error
    client.verify(req).await.unwrap_err();
}
