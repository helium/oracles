use std::path::PathBuf;

use chrono::Utc;
use clap::Parser;
use rand_chacha::rand_core::SeedableRng;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

// use helium_crypto::Network;
use helium_proto::{services::poc_mobile::CellHeartbeatReqV1, Message};
use poc_ingest::server_5g;
use tokio::task::JoinHandle;

#[derive(Parser, Debug)]
struct Cli {
    #[clap(long = "settings-ingest")]
    ingestor_settings_path: PathBuf,

    #[clap(long = "settings-verifier")]
    verifier_settings_path: PathBuf,

    #[clap(long = "settings-rewarder")]
    rewarder_settings_path: PathBuf,
}
#[tokio::main]
async fn main() {
    // panic!("This is a tests-only crate. Please re-run as test.")
    tracing_subscriber::registry()
        .with(tracing_subscriber::EnvFilter::new(
            "integration_tests=debug",
        )) // TODO Maybe per-crate levels.
        .with(tracing_subscriber::fmt::layer())
        .init();
    let cli = Cli::parse();
    tracing::info!(
        "starting {:?} in dir: {:?}, with params: {cli:?}",
        env!("CARGO_PKG_NAME"),
        std::env::current_dir().unwrap()
    );
    test_heartbeat(&cli).await
}

async fn test_heartbeat(cli: &Cli) {
    // Test plan:
    // 1. env start (assuming already done via docker-compose before running this)
    //      - postgres
    //      - minio
    // 2. services start:
    //      - [x] start_ingest
    //      - [/] start_verifier
    //      - [/] start_rewards
    //      - [/] start_follower
    // 3. [x] send data through ingest
    // 4. [ ] ensure we get the proper reward calculated

    let ingestor_settings = poc_ingest::Settings::new(Some(&cli.ingestor_settings_path)).unwrap();
    let verifier_settings = poc_mobile_verifier::Settings::new(Some(&cli.verifier_settings_path)).unwrap();
    let rewarder_settings = mobile_rewards::Settings::new(Some(&cli.rewarder_settings_path)).unwrap();
    let follower_settings = rewarder_settings.follower.clone();

    let api_token = ingestor_settings.token.clone().unwrap();
    let grpc_addr = ingestor_settings.listen.clone();
    let grpc_endpoint = format!("http://{grpc_addr}");

    let (shutdown_trigger, shutdown_listener) = triggered::trigger();

    let ingestor = start_ingest(shutdown_listener, ingestor_settings);
    let verifier = start_verifier(verifier_settings);
    let rewarder = start_rewarder(rewarder_settings);
    let follower = start_follower(follower_settings);

    send_heartbeat(grpc_endpoint, api_token).await;

    shutdown_trigger.trigger();

    ingestor.await.unwrap();
    verifier.await.unwrap();
    rewarder.await.unwrap();
    follower.await.unwrap();

    assert_rewards();
}

fn start_ingest(shutdown_listener: triggered::Listener, settings: poc_ingest::Settings) -> JoinHandle<()> {
    let handle = tokio::spawn(async move {
        server_5g::grpc_server(shutdown_listener, &settings)
            .await
            .expect("start_ingest FAILED");
    });
    tracing::debug!("spawned ingest_thread");
    handle
}

fn start_verifier(_settings: poc_mobile_verifier::Settings) -> JoinHandle<()> {
    let handle = tokio::spawn(async move {
        // let srv_cmd = poc_mobile_verifier::cli::server::Cmd{};
        tracing::error!("FIXME future created by async block is not `Send`");
        // FIXME future created by async block is not `Send`
        // srv_cmd.run(&settings).await.expect("start_verifier FAILED");
    });
    handle
}

fn start_rewarder(_settings: mobile_rewards::Settings) -> JoinHandle<()> {
    let handle = tokio::spawn(async move {
        tracing::error!("TODO start rewarder");
    });
    handle
}

fn start_follower(_settings: node_follower::Settings) -> JoinHandle<()> {
    let handle = tokio::spawn(async move {
        tracing::error!("TODO mock follower");
    });
    handle
}

fn make_keypair() -> helium_crypto::Keypair {
    let mut seed = rand_chacha::ChaCha8Rng::seed_from_u64(10);
    helium_crypto::Keypair::generate(helium_crypto::KeyTag::default(), &mut seed)
}

fn make_heartbeat() -> CellHeartbeatReqV1 {
    use helium_crypto::Sign;

    let hotspot_type = "fake_hotspot_type".to_string();
    let cbsd_category = "fake_cbsd_category".to_string();
    let cbsd_id = "fake_cbsd_id".to_string();
    let keypair = make_keypair();
    let mut heartbeat = CellHeartbeatReqV1 {
        pub_key: keypair.public_key().to_vec(),
        hotspot_type,
        cell_id: 123,
        timestamp: Utc::now().timestamp() as u64,
        lat: 72.63,
        lon: 72.53,
        operation_mode: true,
        cbsd_category,
        cbsd_id,
        signature: vec![],
    };

    let buf = heartbeat.clone().encode_to_vec();
    let sig = keypair.sign(&buf).expect("unable to sign message");
    heartbeat.signature = sig;

    heartbeat
}

async fn send_heartbeat(grpc_endpoint: String, api_token: String) {
    // TODO Remove sleep. How? Client connect retries?
    tokio::time::sleep(tokio::time::Duration::from_millis(1000)).await;

    let mut poc_client = helium_proto::services::poc_mobile::Client::connect(grpc_endpoint)
        .await
        .expect("Unable to connect to server");

    tracing::debug!("connected to poc_mobile");

    let heartbeat = make_heartbeat();
    let mut request = tonic::Request::new(heartbeat);
    request.metadata_mut().append(
        "authorization",
        format!("Bearer {api_token}").parse().unwrap(),
    );
    let response = poc_client
        .submit_cell_heartbeat(request)
        .await
        .expect("unable to submit cell heartbeat");
    tracing::debug!("received response: {response:?}");
    // tokio::time::sleep(tokio::time::Duration::from_millis(200000)).await;
}

fn assert_rewards() {
    todo!("assert rewards")
}
