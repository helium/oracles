use file_store::{
    aws_local::AwsLocal,
    file_upload::{FileUpload, UPLOAD_METRIC},
    BucketClient,
};
use metrics_util::debugging::{DebugValue, DebuggingRecorder, Snapshotter};
use std::{
    collections::HashMap,
    path::{Path, PathBuf},
    sync::OnceLock,
    time::Duration,
};
use task_manager::ManagedTask;

#[tokio::test]
async fn uploads_the_same_file_to_every_bucket() {
    let primary = AwsLocal::new().await;
    let mirror = AwsLocal::new().await;
    primary.create_bucket().await.expect("create primary");
    mirror.create_bucket().await.expect("create mirror");

    let (file_upload, server) =
        FileUpload::with_additional_buckets(primary.bucket_client(), vec![mirror.bucket_client()])
            .await;

    let (trigger, listener) = triggered::trigger();
    let handle = Box::new(server).start_task(listener);

    let cache = tempfile::tempdir().expect("tempdir");
    let contents = b"the exact same bytes".to_vec();
    let path = write_file(cache.path(), "test_report.1700000000000.gz", &contents).await;

    file_upload.upload_file(&path).await.expect("queue upload");
    file_upload.wait_for_uploads_at_least(1).await;

    assert_eq!(
        contents,
        file_contents(&primary.bucket_client(), "test_report.1700000000000.gz").await
    );
    assert_eq!(
        contents,
        file_contents(&mirror.bucket_client(), "test_report.1700000000000.gz").await
    );
    // Every bucket has it, so the cached copy is gone.
    assert!(!path.exists());

    trigger.trigger();
    handle.await.expect("uploader task");

    primary.cleanup().await.expect("cleanup primary");
    mirror.cleanup().await.expect("cleanup mirror");
}

#[tokio::test]
async fn keeps_the_local_file_when_a_bucket_never_gets_it() {
    let primary = AwsLocal::new().await;
    primary.create_bucket().await.expect("create primary");

    // A bucket that was never created: every put against it fails, so the
    // fan-out can never complete.
    let missing = BucketClient {
        client: primary.aws_client(),
        bucket: "bucket-that-does-not-exist".to_string(),
    };

    let (file_upload, server) =
        FileUpload::with_additional_buckets(primary.bucket_client(), vec![missing]).await;
    // Give up on the missing bucket immediately rather than sitting through
    // the production backoff.
    let server = server.with_retry_policy(0, Duration::from_millis(1));

    let (trigger, listener) = triggered::trigger();
    let handle = Box::new(server).start_task(listener);

    let cache = tempfile::tempdir().expect("tempdir");
    let contents = b"kept on disk".to_vec();
    let path = write_file(cache.path(), "test_kept.1700000000000.gz", &contents).await;

    file_upload.upload_file(&path).await.expect("queue upload");
    file_upload.wait_for_uploads_at_least(1).await;

    // The bucket that worked has the file, but the fan-out never completed, so
    // the local copy stays put instead of being lost with it.
    assert_eq!(
        contents,
        file_contents(&primary.bucket_client(), "test_kept.1700000000000.gz").await
    );
    assert!(path.exists());

    trigger.trigger();
    handle.await.expect("uploader task");

    primary.cleanup().await.expect("cleanup primary");
}

/// The shape ingest ships for: a primary bucket and a mirror reached through a
/// different endpoint under a *different* key pair, the way an S3 primary and a
/// Cloudflare R2 mirror are. The clients share nothing — separate credentials
/// provider, separate cache entry — and both still get the identical file.
#[tokio::test]
async fn mirrors_across_independent_credentials() {
    let primary = AwsLocal::builder().credentials_same("admin").build().await;
    // A second identity against the same local S3: different key pair, so this
    // exercises the two-credential path rather than reusing one cached client.
    let mirror = AwsLocal::builder()
        .access_key_id("admin")
        .secret_access_key("admin")
        .region("auto".to_string())
        .build()
        .await;

    primary.create_bucket().await.expect("create primary");
    mirror.create_bucket().await.expect("create mirror");
    assert_ne!(primary.bucket(), mirror.bucket());

    let (file_upload, server) =
        FileUpload::with_additional_buckets(primary.bucket_client(), vec![mirror.bucket_client()])
            .await;

    let (trigger, listener) = triggered::trigger();
    let handle = Box::new(server).start_task(listener);

    let cache = tempfile::tempdir().expect("tempdir");
    let contents = b"same bytes, two providers".to_vec();
    let path = write_file(cache.path(), "test_mirror.1700000000000.gz", &contents).await;

    file_upload.upload_file(&path).await.expect("queue upload");
    file_upload.wait_for_uploads_at_least(1).await;

    assert_eq!(
        contents,
        file_contents(&primary.bucket_client(), "test_mirror.1700000000000.gz").await
    );
    assert_eq!(
        contents,
        file_contents(&mirror.bucket_client(), "test_mirror.1700000000000.gz").await
    );
    assert!(!path.exists());

    trigger.trigger();
    handle.await.expect("uploader task");

    primary.cleanup().await.expect("cleanup primary");
    mirror.cleanup().await.expect("cleanup mirror");
}

/// The upload counter is what an operator alerts on, so a break in it is
/// silent by construction — worth pinning the label shape down.
#[tokio::test]
async fn records_upload_outcome_per_bucket() {
    let snapshotter = snapshotter();

    let primary = AwsLocal::new().await;
    primary.create_bucket().await.expect("create primary");
    let missing_bucket = format!("{}-never-created", primary.bucket());
    let missing = BucketClient {
        client: primary.aws_client(),
        bucket: missing_bucket.clone(),
    };

    let (file_upload, server) =
        FileUpload::with_additional_buckets(primary.bucket_client(), vec![missing]).await;
    let server = server.with_retry_policy(0, Duration::from_millis(1));

    let (trigger, listener) = triggered::trigger();
    let handle = Box::new(server).start_task(listener);

    let cache = tempfile::tempdir().expect("tempdir");
    let path = write_file(cache.path(), "test_metric.1700000000000.gz", b"counted").await;
    file_upload.upload_file(&path).await.expect("queue upload");
    file_upload.wait_for_uploads_at_least(1).await;

    trigger.trigger();
    handle.await.expect("uploader task");

    // `snapshot()` drains, so collect once and assert against that.
    let counts = upload_counts(&snapshotter);
    let count = |bucket: &str, status: &str| counts.get(&(bucket.to_string(), status.into()));

    // The bucket that took the file, and the one that never could, are each
    // counted under their own name.
    assert_eq!(Some(&1), count(primary.bucket(), "ok"));
    assert_eq!(Some(&0), count(primary.bucket(), "error"));
    assert_eq!(Some(&1), count(&missing_bucket, "error"));
    // Seeded at startup, so "no failures" is reported rather than absent.
    assert_eq!(Some(&0), count(&missing_bucket, "ok"));

    primary.cleanup().await.expect("cleanup primary");
}

/// Writes a file into `dir` and returns its path.
async fn write_file(dir: &Path, name: &str, contents: &[u8]) -> PathBuf {
    let path = dir.join(name);
    tokio::fs::write(&path, contents).await.expect("write file");
    path
}

async fn file_contents(bucket: &BucketClient, key: &str) -> Vec<u8> {
    bucket
        .get_raw_file(key)
        .await
        .expect("get file")
        .collect()
        .await
        .expect("collect bytes")
        .to_vec()
}

/// Installs the debugging recorder once per test binary. The recorder is
/// process-global, so every test in this file shares it; assertions filter by
/// bucket name, which `AwsLocal` randomizes per handle.
fn snapshotter() -> Snapshotter {
    static SNAPSHOTTER: OnceLock<Snapshotter> = OnceLock::new();
    SNAPSHOTTER
        .get_or_init(|| {
            let recorder = DebuggingRecorder::new();
            let snapshotter = recorder.snapshotter();
            recorder.install().expect("install recorder");
            snapshotter
        })
        .clone()
}

/// Collects every `UPLOAD_METRIC` counter in one drain of the snapshotter,
/// keyed by its `bucket` and `status` labels.
fn upload_counts(snapshotter: &Snapshotter) -> HashMap<(String, String), u64> {
    snapshotter
        .snapshot()
        .into_vec()
        .into_iter()
        .filter_map(|(key, _, _, value)| {
            let key = key.key();
            if key.name() != UPLOAD_METRIC {
                return None;
            }
            let label = |name| {
                key.labels()
                    .find(|l| l.key() == name)
                    .map(|l| l.value().to_string())
            };
            match value {
                DebugValue::Counter(v) => Some(((label("bucket")?, label("status")?), v)),
                _ => None,
            }
        })
        .collect()
}
