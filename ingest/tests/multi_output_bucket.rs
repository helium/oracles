//! End-to-end coverage for writing one ingested file to more than one bucket.
//!
//! Unlike `mobile_ingest.rs`, which swaps the file sinks for channels and stops
//! at the gRPC handler, this drives the whole production path:
//! `ingest::server_mobile::grpc_server` built from a real `Settings`, a real
//! gRPC submission, the real rolling file sink, and the real uploader writing
//! into the RustFS from `docker-compose.yml`. Nothing here is a stand-in, so it
//! is the only test that can show a submitted report actually reaching two
//! buckets.
//!
//! Buckets come from `AwsLocal`, which creates a randomly named one per handle
//! and deletes it on cleanup, so runs cannot collide or inherit junk from a
//! previous failure.
//!
//! The mirror is configured with its own endpoint, region and credentials
//! rather than inheriting the primary's — the arrangement an S3 primary with a
//! Cloudflare R2 mirror has.

use file_store::{aws_local::AwsLocal, BucketClient, FileInfo};
use futures::TryStreamExt;
use helium_crypto::{KeyTag, Keypair, Sign};
use helium_proto::services::poc_mobile::{
    CarrierIdV2, Client as PocMobileClient, DataTransferEvent, DataTransferRadioAccessTechnology,
    DataTransferSessionIngestReportV1, DataTransferSessionReqV1,
};
use ingest::Settings;
use prost::Message;
use rand::rngs::OsRng;
use std::{net::SocketAddr, path::Path, time::Duration};
use tokio::net::TcpListener;
use tonic::{metadata::MetadataValue, Request};

const FILE_PREFIX: &str = "data_transfer_session_ingest_report";
const TOKEN: &str = "api_token";
/// Any valid mainnet key: mobile mode refuses to start without one, though the
/// report this test submits is not carrier-signed.
const CARRIER_KEY: &str = "113HRxtzxFbFUjDEJJpyeMRZRtdAW38LAUnB5mshRwi6jt7uFbt";

#[tokio::test(flavor = "multi_thread")]
async fn an_ingested_report_lands_in_every_configured_bucket() -> anyhow::Result<()> {
    let primary_aws = AwsLocal::new().await;
    // A separately configured identity for the mirror: its own region, so it
    // resolves to its own S3 client rather than sharing the primary's.
    let mirror_aws = AwsLocal::builder()
        .region("auto".to_string())
        .credentials_same("admin")
        .build()
        .await;
    primary_aws.create_bucket().await?;
    mirror_aws.create_bucket().await?;

    let result = run_ingest(&primary_aws, &mirror_aws).await;

    // Clean up both buckets whatever the assertions did, or AwsLocal's drop
    // guard turns one failure into two. A cleanup error must not stand in for
    // the assertion failure that caused it.
    let cleanup = primary_aws
        .cleanup()
        .await
        .and(mirror_aws.cleanup().await)
        .map_err(anyhow::Error::from);
    result.and(cleanup)
}

async fn run_ingest(primary_aws: &AwsLocal, mirror_aws: &AwsLocal) -> anyhow::Result<()> {
    let primary = primary_aws.bucket_client();
    let mirror = mirror_aws.bucket_client();

    let cache = tempfile::tempdir()?;
    let listen_addr = free_port().await?;
    let (settings, _settings_dir) = settings(primary_aws, mirror_aws, cache.path(), listen_addr)?;

    // The real entry point, wiring the real sinks to the real uploader. It owns
    // its own shutdown (TaskManager waits on SIGTERM), so the test aborts it
    // once both buckets have the file rather than trying to signal it.
    let server = tokio::spawn(async move { ingest::server_mobile::grpc_server(&settings).await });

    let mut client = connect(listen_addr).await?;
    let keypair = Keypair::generate(KeyTag::default(), &mut OsRng);
    let submitted = submit_data_transfer(&mut client, &keypair).await?;

    let primary_file = await_file(&primary).await?;
    let mirror_file = await_file(&mirror).await?;
    server.abort();

    // Same key in both: the mirror holds a copy, not a separately named
    // artifact.
    assert_eq!(primary_file.key, mirror_file.key);

    let primary_bytes = raw_bytes(&primary, &primary_file.key).await?;
    let mirror_bytes = raw_bytes(&mirror, &mirror_file.key).await?;
    assert!(!primary_bytes.is_empty(), "uploaded an empty file");
    assert_eq!(
        primary_bytes, mirror_bytes,
        "the mirror's copy differs from the primary's"
    );

    // ... and those bytes are the report that was submitted, not merely equal
    // rubbish.
    let stored = decode_only_report(&primary, primary_file)
        .await?
        .report
        .expect("inner report");
    assert_eq!(submitted.pub_key, stored.pub_key);
    assert_eq!(
        submitted.data_transfer_usage.expect("usage").event_id,
        stored.data_transfer_usage.expect("stored usage").event_id
    );

    Ok(())
}

async fn free_port() -> anyhow::Result<SocketAddr> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    Ok(listener.local_addr()?)
}

/// Builds settings the way a deployment does — parsed from a settings file — so
/// the test covers the `[additional_output_buckets.<name>]` config shape and not
/// just the types behind it.
fn settings(
    primary: &AwsLocal,
    mirror: &AwsLocal,
    cache: &Path,
    listen_addr: SocketAddr,
) -> anyhow::Result<(Settings, tempfile::TempDir)> {
    let dir = tempfile::tempdir()?;
    let path = dir.path().join("settings.toml");
    std::fs::write(
        &path,
        format!(
            r#"
mode = "mobile"
listen_addr = "{listen_addr}"
cache = "{cache}"
token = "{TOKEN}"
carrier_authorized_keys = "{CARRIER_KEY}"
# Short so the sink rolls and uploads within the test's patience. Production
# runs 15 minutes.
roll_time = "1s"
output_bucket = "{primary_bucket}"

# The mirror carries its own endpoint, region and key pair and inherits nothing
# from [file_store] -- what makes a different provider possible.
[additional_output_buckets.mirror]
bucket = "{mirror_bucket}"
endpoint = "{mirror_endpoint}"
region = "auto"
access_key_id = "admin"
secret_access_key = "admin"

[file_store]
endpoint = "{primary_endpoint}"
region = "us-east-1"
access_key_id = "admin"
secret_access_key = "admin"
"#,
            cache = cache.display(),
            primary_bucket = primary.bucket(),
            primary_endpoint = primary.endpoint(),
            mirror_bucket = mirror.bucket(),
            mirror_endpoint = mirror.endpoint(),
        ),
    )?;

    Ok((Settings::new(Some(&path))?, dir))
}

/// Polls until the bucket holds a file for our prefix, or gives up. The file
/// is due about a second after it is written (`roll_time`), so this is mostly
/// headroom for a slow CI worker.
async fn await_file(bucket: &BucketClient) -> anyhow::Result<FileInfo> {
    for _ in 0..300 {
        if let Some(file) = bucket
            .list_all_files(FILE_PREFIX, None, None)
            .await?
            .into_iter()
            .next()
        {
            return Ok(file);
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    anyhow::bail!("no {FILE_PREFIX} file appeared in {}", bucket.bucket)
}

async fn raw_bytes(bucket: &BucketClient, key: &str) -> anyhow::Result<Vec<u8>> {
    Ok(bucket.get_raw_file(key).await?.collect().await?.to_vec())
}

/// Waits for the gRPC listener. Generous, because startup builds 17 file sinks
/// and an S3 client first, and `aws_config` resolution can stall for seconds on
/// a host with no instance metadata service.
async fn connect(addr: SocketAddr) -> anyhow::Result<PocMobileClient<tonic::transport::Channel>> {
    let endpoint = format!("http://{addr}");
    for _ in 0..300 {
        if let Ok(client) = PocMobileClient::connect(endpoint.clone()).await {
            return Ok(client);
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    anyhow::bail!("ingest grpc never came up on {endpoint}")
}

async fn submit_data_transfer(
    client: &mut PocMobileClient<tonic::transport::Channel>,
    keypair: &Keypair,
) -> anyhow::Result<DataTransferSessionReqV1> {
    let mut req = DataTransferSessionReqV1 {
        data_transfer_usage: Some(DataTransferEvent {
            pub_key: keypair.public_key().into(),
            upload_bytes: 1024,
            download_bytes: 2048,
            radio_access_technology: DataTransferRadioAccessTechnology::Wlan as i32,
            event_id: "multi-bucket-event".to_string(),
            payer: vec![1, 2, 3, 4],
            timestamp: chrono::Utc::now().timestamp() as u64,
            signature: vec![],
        }),
        reward_cancelled: false,
        pub_key: keypair.public_key().into(),
        signature: vec![],
        rewardable_bytes: 3072,
        carrier_id_v2: CarrierIdV2::Carrier9 as i32,
        sampling: false,
    };
    req.signature = keypair.sign(&req.encode_to_vec())?;

    let mut request = Request::new(req.clone());
    request.metadata_mut().insert(
        "authorization",
        format!("Bearer {TOKEN}").parse::<MetadataValue<_>>()?,
    );
    client.submit_data_transfer_session(request).await?;

    Ok(req)
}

/// Reads the single report back out of the gzipped, length-framed file.
async fn decode_only_report(
    bucket: &BucketClient,
    file: FileInfo,
) -> anyhow::Result<DataTransferSessionIngestReportV1> {
    let frames: Vec<_> = bucket.stream_single_file(file).await?.try_collect().await?;
    let [frame] = frames.as_slice() else {
        anyhow::bail!("expected exactly one report, got {}", frames.len());
    };
    Ok(DataTransferSessionIngestReportV1::decode(frame.clone())?)
}
