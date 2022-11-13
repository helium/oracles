use std::path::PathBuf;
use std::str::FromStr;

use anyhow::{anyhow, Result};
use clap::Parser;
use rand_chacha::rand_core::SeedableRng; // seed_from_u64
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

use helium_proto::services::follower::{
    FollowerGatewayReqV1, FollowerGatewayRespV1, FollowerGatewayStreamReqV1,
    FollowerGatewayStreamRespV1, FollowerSubnetworkLastRewardHeightReqV1,
    FollowerSubnetworkLastRewardHeightRespV1, FollowerTxnStreamReqV1, FollowerTxnStreamRespV1,
};
use helium_proto::{
    services::poc_mobile::CellHeartbeatReqV1, BlockchainTxn, BlockchainTxnSubnetworkRewardsV1,
    Message, SubnetworkReward,
};
use tonic::{Request, Status};

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
            "tests=debug,poc_ingest=debug,poc_mobile_verifier=debug,mobile_rewards=debug,node_follower=debug",
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
    //      - [x] ingestor_start
    //      - [x] verifier_start
    //      - [x] rewarder_start
    //      - [ ] txn_stream_start
    //      - [/] follower_start
    // 3. [x] send data through ingest
    // 4. [ ] ensure we get the proper reward calculated
    //      - how?
    //          - pg should have txn_hash_str set to Status::Pending
    //          - mock node_follower::txn_service::TransactionService
    //              intercepting submission of BlockchainTxn { txn: Some(Txn::SubnetworkRewards(txn)), }

    let ingestor_settings = poc_ingest::Settings::new(Some(&cli.ingestor_settings_path)).unwrap();
    let verifier_settings =
        poc_mobile_verifier::Settings::new(Some(&cli.verifier_settings_path)).unwrap();
    let rewarder_settings =
        mobile_rewards::Settings::new(Some(&cli.rewarder_settings_path)).unwrap();
    let follower_settings = rewarder_settings.follower.clone();

    // TODO Ensure all settings have the same net set. Assert or override?
    let net = helium_crypto::Network::TestNet;

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

    // TODO Start before rewarder? Since it connects to txn_stream.
    // TODO Pass addr and port args.
    // TODO Actually correct implementation.
    tokio::spawn(async move {
        txn_stream_start().await.expect("txn streamer failed");
    });
    tokio::try_join!(
        ingestor_start(shutdown_listener.clone(), ingestor_settings),
        verifier_start(verifier_settings),
        rewarder_start(shutdown_listener.clone(), rewarder_settings, &keypair),
        follower_start(follower_settings),
        test(&keypair, grpc_endpoint, api_token),
    )?;
    Ok(())
}

#[derive(Debug, Default)] // TODO Remove Default
pub struct FollowerServerInstance {}

#[derive(Debug, Default)] // TODO Remove Default
pub struct TxnStream {
    txs: Vec<BlockchainTxn>,
}

impl TxnStream {
    pub fn new(tx: BlockchainTxn) -> TxnStream {
        TxnStream { txs: vec![tx] }
    }
}

impl futures::Stream for TxnStream {
    type Item = std::result::Result<FollowerTxnStreamRespV1, Status>;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        _cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        match self.txs.pop() {
            None => std::task::Poll::Ready(None),
            Some(tx) => {
                let resp = FollowerTxnStreamRespV1 {
                    height: 0,         // FIXME What's a good value?
                    txn_hash: vec![0], // FIXME What's a good value?
                    txn: Some(tx),     // FIXME What's a good value?
                    timestamp: 0,      // FIXME What's a good value?
                };
                std::task::Poll::Ready(Some(Ok(resp)))
            }
        }
    }
}

#[derive(Debug, Default)] // TODO Remove Default
pub struct GwStream {}

impl futures::Stream for GwStream {
    type Item = std::result::Result<FollowerGatewayStreamRespV1, Status>;

    fn poll_next(
        self: std::pin::Pin<&mut Self>,
        _cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        //let gw = GatewayInfo {
        //    /// / The asserted h3 (string) location of the gateway. Empty string if the
        //    /// gateway is not found
        //    #[prost(string, tag = "1")]
        //    pub location: ::prost::alloc::string::String,
        //    /// / The pubkey_bin address of the requested gateway
        //    #[prost(bytes = "vec", tag = "2")]
        //    pub address: ::prost::alloc::vec::Vec<u8>,
        //    /// / The pubkey_bin address of the current owner of the gateway. An empty bytes
        //    /// / if the hotspot is not found
        //    #[prost(bytes = "vec", tag = "3")]
        //    pub owner: ::prost::alloc::vec::Vec<u8>,
        //    /// / the staking mode of the gateway
        //    #[prost(enumeration = "super::GatewayStakingMode", tag = "4")]
        //    pub staking_mode: i32,
        //    /// / the transmit gain value of the gateway in dbi x 10 For example 1 dbi = 10,
        //    /// 15 dbi = 150
        //    #[prost(int32, tag = "5")]
        //    pub gain: i32,
        //    /// / The region of the gateway's corresponding location
        //    #[prost(enumeration = "super::Region", tag = "6")]
        //    pub region: i32,
        //};
        // let resp = FollowerGatewayRespV1 {
        //     /// / The height for at which the ownership was looked up
        //     height: 1,
        //     // result: Some(follower_gateway_resp_v1::Result::Info(gw)),
        //     result: None,
        // };
        // let resps = FollowerGatewayStreamRespV1 {
        //     gateways: vec![resp],
        // };
        // std::task::Poll::Ready(Some(Ok(resps)))
        std::task::Poll::Ready(None)
    }
}

#[tonic::async_trait]
impl helium_proto::services::follower::follower_server::Follower for FollowerServerInstance {
    type txn_streamStream = TxnStream;

    async fn txn_stream(
        &self,
        req: Request<FollowerTxnStreamReqV1>,
    ) -> std::result::Result<tonic::Response<Self::txn_streamStream>, Status> {
        tracing::debug!("txn_stream. self:{self:?}, req:{req:?}");
        let r = SubnetworkReward {
            account: vec![0], // TODO pub key?
            amount: 0,
        };
        let rs = BlockchainTxnSubnetworkRewardsV1 {
            token_type: 0,
            start_epoch: 0,
            end_epoch: 0,
            reward_server_signature: vec![0],
            rewards: vec![r],
        };
        let tx0 = helium_proto::blockchain_txn::Txn::SubnetworkRewards(rs);
        let tx = BlockchainTxn { txn: Some(tx0) };
        let msg = TxnStream::new(tx);
        Ok(tonic::Response::new(msg))
    }

    async fn find_gateway(
        &self,
        req: Request<FollowerGatewayReqV1>,
    ) -> std::result::Result<tonic::Response<FollowerGatewayRespV1>, Status> {
        tracing::debug!("find_gateway. self:{self:?}, req:{req:?}");
        let msg = FollowerGatewayRespV1 {
            height: 0,
            result: None,
        };
        Ok(tonic::Response::new(msg))
    }

    type active_gatewaysStream = GwStream;

    async fn active_gateways(
        &self,
        req: Request<FollowerGatewayStreamReqV1>,
    ) -> std::result::Result<tonic::Response<Self::active_gatewaysStream>, Status> {
        tracing::debug!("active_gateways. self:{self:?}, req:{req:?}");
        let msg = GwStream::default();
        Ok(tonic::Response::new(msg))
    }

    async fn subnetwork_last_reward_height(
        &self,
        _request: Request<FollowerSubnetworkLastRewardHeightReqV1>,
    ) -> std::result::Result<tonic::Response<FollowerSubnetworkLastRewardHeightRespV1>, Status>
    {
        let msg = FollowerSubnetworkLastRewardHeightRespV1 {
            height: 0,
            reward_height: 0,
        };
        Ok(tonic::Response::new(msg))
    }
}

async fn txn_stream_start() -> Result<()> {
    let addr = "127.0.0.1:8080";
    let addr: std::net::SocketAddr = addr.parse()?;
    let follower_srv = FollowerServerInstance::default();
    tonic::transport::Server::builder()
        .add_service(helium_proto::services::follower::Server::new(follower_srv))
        .serve(addr)
        .await?;
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

    let endpoint = http::Uri::from_str(endpoint)
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

async fn ingestor_start(
    shutdown_listener: triggered::Listener,
    settings: poc_ingest::Settings,
) -> Result<()> {
    poc_ingest::server_5g::grpc_server(shutdown_listener, &settings)
        .await
        .map(|_| tracing::info!("START ingestor_start OK"))
        .map_err(|e| {
            tracing::error!("ingestor_start ERROR: {e:?}");
            e
        })
        .map_err(|e| anyhow!("ingestor failure: {e:?}"))
}

async fn verifier_start(settings: poc_mobile_verifier::Settings) -> Result<()> {
    // XXX While Cmd.run runs the migrations, it also calls things that
    //     fail without seeding, but seeding fails without migrations.
    let pool = settings.database.connect(10).await?;
    sqlx::migrate!("../poc_mobile_verifier/migrations")
        .run(&pool)
        .await
        .map_err(|e| {
            tracing::error!("START verifier_start migration: {e:?}");
            e
        })?;
    verifier_seed_db(&settings).await;
    poc_mobile_verifier::cli::server::Cmd {}
        .run(&settings)
        .await
        .map(|_| tracing::info!("START verifier_start OK"))
        .map_err(|e| {
            tracing::error!("verifier_start ERROR: {e:?}");
            e
        })
        .map_err(|e| anyhow!("verifier failure: {e:?}"))
}

async fn rewarder_start(
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
            tracing::error!("START rewarder_start migration: {e:?}");
            e
        })?;
    let mut reward_server = mobile_rewards::server::Server::new(&settings)
        .await
        .map_err(|e| {
            tracing::error!("START rewarder_start server construct: {e:?}");
            e
        })?;
    reward_server.run(shutdown_listener).await.map_err(|e| {
        tracing::error!("START rewarder_start server run: {e:?}");
        e
    })?;
    Ok(())
}

async fn follower_start(_settings: node_follower::Settings) -> Result<()> {
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
    let mut request = Request::new(heartbeat);
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

async fn verifier_seed_db(settings: &poc_mobile_verifier::Settings) {
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
