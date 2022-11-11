use std::path::PathBuf;
use std::str::FromStr;

use anyhow::{anyhow, Result};
use clap::Parser;
use rand_chacha::rand_core::SeedableRng; // seed_from_u64
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

// use helium_crypto::Network;
use helium_proto::{services::poc_mobile::CellHeartbeatReqV1, Message};
use http::Uri;
use poc_ingest::server_5g;

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
async fn main() -> Result<()> {
    // panic!("This is a tests-only crate. Please re-run as test.")
    tracing_subscriber::registry()
        .with(tracing_subscriber::EnvFilter::new(
            "tests=debug,poc_ingest=debug,poc_mobile_verifier=debug,mobile_rewards=debug",
        )) // TODO Maybe per-crate levels.
        .with(tracing_subscriber::fmt::layer())
        .init();
    let cli = Cli::parse();
    tracing::info!(
        "starting {:?} in dir: {:?}, with params: {cli:?}",
        env!("CARGO_PKG_NAME"),
        std::env::current_dir().unwrap()
    );
    suite(&cli).await
}

async fn suite(cli: &Cli) -> Result<()> {
    // Test plan:
    // 1. env start (assuming already done via docker-compose before running this)
    //      - postgres
    //      - minio
    // 2. services start:
    //      - [x] start_ingest
    //      - [x] start_verifier
    //      - [x] start_rewarder
    //      - [/] start_follower
    // 3. [x] send data through ingest
    // 4. [ ] ensure we get the proper reward calculated

    let ingestor_settings = poc_ingest::Settings::new(Some(&cli.ingestor_settings_path)).unwrap();
    let verifier_settings =
        poc_mobile_verifier::Settings::new(Some(&cli.verifier_settings_path)).unwrap();
    let rewarder_settings =
        mobile_rewards::Settings::new(Some(&cli.rewarder_settings_path)).unwrap();
    let follower_settings = rewarder_settings.follower.clone();

    // TODO Set manually here or get from settings? If settings then which ones?
    // TODO Ensure all settings have the same net set. Assert or override?
    let net = ingestor_settings.network;

    // TODO ensure keypair path is set correctly in all settings.
    let keypair = make_keypair(net);

    let api_token = ingestor_settings.token.clone().unwrap();
    let grpc_addr = ingestor_settings.listen.clone();
    let grpc_endpoint = format!("http://{grpc_addr}");
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
    tokio::spawn(async move {
        let _ = tokio::signal::ctrl_c().await;
        shutdown_trigger.trigger()
    });

    tokio::try_join!(
        start_ingest(shutdown_listener.clone(), ingestor_settings),
        start_verifier(verifier_settings),
        start_rewarder(shutdown_listener.clone(), rewarder_settings, &keypair),
        start_follower(follower_settings),
        test(&keypair, grpc_endpoint, api_token),
    )?;
    Ok(())
}

async fn test(
    keypair: &helium_crypto::Keypair,
    grpc_endpoint: String,
    api_token: String,
) -> Result<()> {
    send_heartbeat(keypair, grpc_endpoint, api_token).await?;
    assert_rewards().await?;
    Ok(())
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

async fn start_ingest(
    shutdown_listener: triggered::Listener,
    settings: poc_ingest::Settings,
) -> Result<()> {
    server_5g::grpc_server(shutdown_listener, &settings)
        .await
        .map(|_| tracing::info!("START start_ingest OK"))
        .map_err(|e| {
            tracing::error!("start_ingest ERROR: {e:?}");
            e
        })
        .map_err(|e| anyhow!("ingestor failure: {e:?}"))
}

async fn start_verifier(settings: poc_mobile_verifier::Settings) -> Result<()> {
    // XXX While Cmd.run runs the migrations, it also calls things that
    //     fail without seeding, but seeding fails without migrations.
    let pool = settings.database.connect(10).await?;
    sqlx::migrate!("../poc_mobile_verifier/migrations")
        .run(&pool)
        .await
        .map_err(|e| {
            tracing::error!("START start_verifier migration: {e:?}");
            e
        })?;
    seed_db_for_verifier(&settings).await;
    poc_mobile_verifier::cli::server::Cmd {}
        .run(&settings)
        .await
        .map(|_| tracing::info!("START start_verifier OK"))
        .map_err(|e| {
            tracing::error!("start_verifier ERROR: {e:?}");
            e
        })
        .map_err(|e| anyhow!("verifier failure: {e:?}"))
}

async fn start_rewarder(
    shutdown_listener: triggered::Listener,
    settings: mobile_rewards::Settings,
    keypair: &helium_crypto::Keypair,
) -> Result<()> {
    // FIXME mobile_rewards::server: failed to connec to txn stream: grpc error trying to connect: tcp connect error: Connection refused (os error 111)
    // TODO What grpc stream to connect to?
    let pool = settings.database.connect(2).await?;
    let keypair_filepath = &settings.keypair;
    std::fs::write(keypair_filepath, keypair.to_vec())?;
    sqlx::migrate!("../mobile_rewards/migrations")
        .run(&pool)
        .await
        .map_err(|e| {
            tracing::error!("START start_rewarder migration: {e:?}");
            e
        })?;
    let mut reward_server = mobile_rewards::server::Server::new(&settings)
        .await
        .map_err(|e| {
            tracing::error!("START start_rewarder server construct: {e:?}");
            e
        })?;
    reward_server.run(shutdown_listener).await.map_err(|e| {
        tracing::error!("START start_rewarder server run: {e:?}");
        e
    })?;
    Ok(())
}

async fn start_follower(_settings: node_follower::Settings) -> Result<()> {
    tracing::error!("START TODO mock follower");
    Ok(())
}

fn make_keypair(net: helium_crypto::Network) -> helium_crypto::Keypair {
    let key_tag = helium_crypto::KeyTag {
        network: net,
        key_type: helium_crypto::KeyType::Ed25519,
    };
    let mut seed = rand_chacha::ChaCha8Rng::seed_from_u64(10);
    helium_crypto::Keypair::generate(key_tag, &mut seed)
}

fn make_heartbeat(keypair: &helium_crypto::Keypair) -> CellHeartbeatReqV1 {
    use helium_crypto::Sign;

    let hotspot_type = "fake_hotspot_type".to_string();
    let cbsd_category = "fake_cbsd_category".to_string();
    let cbsd_id = "fake_cbsd_id".to_string();
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

async fn send_heartbeat(
    keypair: &helium_crypto::Keypair,
    grpc_endpoint: String,
    api_token: String,
) -> Result<()> {
    // TODO Remove sleep. How? Client connect retries?
    tokio::time::sleep(tokio::time::Duration::from_millis(2000)).await;

    tracing::debug!("connecting to grpc endpoint: {:?}", grpc_endpoint);
    let mut poc_client = helium_proto::services::poc_mobile::Client::connect(grpc_endpoint)
        .await
        .map(|c| {
            tracing::info!("poc_mobile connection OK");
            c
        })
        .map_err(|e| {
            tracing::error!("poc_mobile connection ERROR: {e:?}");
            e
        })?;

    let heartbeat = make_heartbeat(keypair);
    let mut request = tonic::Request::new(heartbeat);
    request
        .metadata_mut()
        .append("authorization", format!("Bearer {api_token}").parse()?);
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
        })?;
    tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;
    Ok(())
}

async fn assert_rewards() -> Result<()> {
    tracing::error!("TODO assert rewards");
    Ok(())
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
