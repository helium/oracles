use std::path::PathBuf;
use std::str::FromStr;

use clap::Parser;
use rand_chacha::rand_core::SeedableRng; // seed_from_u64
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

// use helium_crypto::Network;
use helium_proto::{services::poc_mobile::CellHeartbeatReqV1, Message};
use http::Uri;
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
    let verifier_settings =
        poc_mobile_verifier::Settings::new(Some(&cli.verifier_settings_path)).unwrap();
    let rewarder_settings =
        mobile_rewards::Settings::new(Some(&cli.rewarder_settings_path)).unwrap();
    let follower_settings = rewarder_settings.follower.clone();

    let api_token = ingestor_settings.token.clone().unwrap();
    let grpc_addr = ingestor_settings.listen.clone();
    let grpc_endpoint = format!("http://{grpc_addr}");
    let net = ingestor_settings.network;
    let aws_region = "us-west-2"; // TODO Pull from settings. From which one?
    let aws_endpoint = "http://localhost:9000";

    // TODO Determine which service needs which, separate and grab from settings.
    let aws_buckets = [
        ingestor_settings.output.bucket.as_str(),
        verifier_settings.output.bucket.as_str(),
        verifier_settings.ingest.bucket.as_str(),
        rewarder_settings.output.bucket.as_str(),
    ];
    s3_create_buckets(aws_endpoint, aws_region, &aws_buckets)
        .await
        .unwrap();

    let (shutdown_trigger, shutdown_listener) = triggered::trigger();

    let ingestor = start_ingest(shutdown_listener, ingestor_settings);
    let verifier = start_verifier(verifier_settings);
    let rewarder = start_rewarder(rewarder_settings);
    let follower = start_follower(follower_settings);

    send_heartbeat(net, grpc_endpoint, api_token).await;

    shutdown_trigger.trigger();

    ingestor.await.unwrap();
    verifier.await; //.unwrap();
    rewarder.await.unwrap();
    follower.await.unwrap();

    assert_rewards();
}

async fn s3_create_buckets(
    endpoint: &str,
    region: &str,
    buckets: &[&str],
) -> Result<(), aws_sdk_s3::types::SdkError<aws_sdk_s3::error::CreateBucketError>> {
    for bucket in buckets {
        s3_create_bucket(endpoint, region, bucket).await?
    }
    Ok(())
}

async fn s3_create_bucket(
    endpoint: &str,
    region: &str,
    bucket_name: &str,
) -> Result<(), aws_sdk_s3::types::SdkError<aws_sdk_s3::error::CreateBucketError>> {
    tracing::debug!("bucket create BEGIN {bucket_name:?}");

    let endpoint = Uri::from_str(endpoint)
        .map(aws_sdk_s3::Endpoint::immutable)
        .unwrap();
    let config = aws_config::from_env()
        .region(aws_sdk_s3::Region::new(region.to_string()))
        .endpoint_resolver(endpoint)
        .load()
        .await;
    let client = aws_sdk_s3::Client::new(&config);
    let constraint = aws_sdk_s3::model::BucketLocationConstraint::from(region);
    let cfg = aws_sdk_s3::model::CreateBucketConfiguration::builder()
        .location_constraint(constraint)
        .build();
    let result = client
        .create_bucket()
        .create_bucket_configuration(cfg)
        .bucket(bucket_name)
        .send()
        .await;
    match result {
        Ok(_) => {
            tracing::info!("bucket create OK created {bucket_name:?}");
            Ok(())
        }
        Err(aws_sdk_s3::types::SdkError::ServiceError {
            err:
                aws_sdk_s3::error::CreateBucketError {
                    kind: aws_sdk_s3::error::CreateBucketErrorKind::BucketAlreadyOwnedByYou(_),
                    ..
                },
            ..
        }) => {
            tracing::warn!("bucket create OK already owned {bucket_name:?}");
            Ok(())
        }
        Err(aws_sdk_s3::types::SdkError::ServiceError {
            err:
                aws_sdk_s3::error::CreateBucketError {
                    kind: aws_sdk_s3::error::CreateBucketErrorKind::BucketAlreadyExists(_),
                    ..
                },
            ..
        }) => {
            tracing::warn!("bucket create OK already exists {bucket_name:?}");
            Ok(())
        }
        Err(e) => {
            tracing::error!("bucket create ERROR {bucket_name:?} {e:?}");
            Err(e)
        }
    }
}

fn start_ingest(
    shutdown_listener: triggered::Listener,
    settings: poc_ingest::Settings,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        server_5g::grpc_server(shutdown_listener, &settings)
            .await
            .map(|_| tracing::info!("start_ingest OK"))
            .map_err(|e| {
                tracing::error!("start_ingest ERROR: {e:?}");
                e
            })
            .unwrap();
    })
}

async fn start_verifier(settings: poc_mobile_verifier::Settings) {
    seed_db_for_verifier(&settings).await;
    let srv_cmd = poc_mobile_verifier::cli::server::Cmd {};
    srv_cmd
        .run(&settings)
        .await
        .map(|_| tracing::info!("start_verifier OK"))
        .map_err(|e| {
            tracing::error!("start_verifier ERROR: {e:?}");
            e
        })
        .unwrap();
}

fn start_rewarder(_settings: mobile_rewards::Settings) -> JoinHandle<()> {
    tokio::spawn(async move {
        tracing::error!("TODO start rewarder");
    })
}

fn start_follower(_settings: node_follower::Settings) -> JoinHandle<()> {
    tokio::spawn(async move {
        tracing::error!("TODO mock follower");
    })
}

fn make_keypair(net: helium_crypto::Network) -> helium_crypto::Keypair {
    let key_tag = helium_crypto::KeyTag {
        network: net,
        key_type: helium_crypto::KeyType::Ed25519,
    };
    let mut seed = rand_chacha::ChaCha8Rng::seed_from_u64(10);
    helium_crypto::Keypair::generate(key_tag, &mut seed)
}

fn make_heartbeat(net: helium_crypto::Network) -> CellHeartbeatReqV1 {
    use helium_crypto::Sign;

    let hotspot_type = "fake_hotspot_type".to_string();
    let cbsd_category = "fake_cbsd_category".to_string();
    let cbsd_id = "fake_cbsd_id".to_string();
    let keypair = make_keypair(net);
    let mut heartbeat = CellHeartbeatReqV1 {
        pub_key: keypair.public_key().to_vec(),
        hotspot_type,
        cell_id: 123,
        timestamp: chrono::Utc::now().timestamp() as u64,
        lat: 72.63,
        lon: 72.53,
        operation_mode: true,
        cbsd_category,
        cbsd_id,
        signature: vec![],
    };

    let buf = heartbeat.encode_to_vec();
    let sig = keypair.sign(&buf).expect("unable to sign message");
    heartbeat.signature = sig;

    heartbeat
}

async fn send_heartbeat(net: helium_crypto::Network, grpc_endpoint: String, api_token: String) {
    // TODO Remove sleep. How? Client connect retries?
    tokio::time::sleep(tokio::time::Duration::from_millis(1000)).await;

    let mut poc_client = helium_proto::services::poc_mobile::Client::connect(grpc_endpoint)
        .await
        .map(|c| {
            tracing::info!("poc_mobile connection OK");
            c
        })
        .map_err(|e| {
            tracing::error!("poc_mobile connection ERROR: {e:?}");
            e
        })
        .unwrap();

    let heartbeat = make_heartbeat(net);
    let mut request = tonic::Request::new(heartbeat);
    request.metadata_mut().append(
        "authorization",
        format!("Bearer {api_token}").parse().unwrap(),
    );
    poc_client
        .submit_cell_heartbeat(request)
        .await
        .map(|r| {
            tracing::info!("response receive OK: {r:?}");
            r
        })
        .map_err(|e| {
            tracing::error!("response receive ERROR: {e:?}");
            e
        })
        .unwrap();
    tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;
}

fn assert_rewards() {
    todo!("assert rewards")
}

async fn seed_db_for_verifier(settings: &poc_mobile_verifier::Settings) {
    let exec = settings.database.connect(1).await.unwrap();
    let now = chrono::Utc::now().timestamp() as i64;
    // FIXME What are good values to use?
    let last_verified = now;
    let last_rewarded = now;
    let next_rewarded = now;
    db_store::meta::store(&exec, "last_verified_end_time", last_verified)
        .await
        .unwrap();
    db_store::meta::store(&exec, "last_rewarded_end_time", last_rewarded)
        .await
        .unwrap();
    db_store::meta::store(&exec, "next_rewarded_end_time", next_rewarded)
        .await
        .unwrap();
}
