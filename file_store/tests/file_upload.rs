use file_store::{
    aws_local::AwsLocal,
    file_upload::{FileUpload, FileUploadServer, FileUploader, MultiFileUpload, UPLOAD_METRIC},
    BucketClient,
};
use metrics_util::debugging::{DebugValue, DebuggingRecorder, Snapshotter};
use std::{
    collections::HashMap,
    os::unix::fs::MetadataExt,
    path::{Path, PathBuf},
    sync::OnceLock,
    time::Duration,
};
use task_manager::ManagedTask;

const KEY: &str = "test_report.1700000000000.gz";

#[tokio::test]
async fn uploads_the_same_file_to_every_bucket() {
    let primary = AwsLocal::new().await;
    let mirror = AwsLocal::new().await;
    primary.create_bucket().await.expect("create primary");
    mirror.create_bucket().await.expect("create mirror");

    let cache = tempfile::tempdir().expect("tempdir");
    let (primary_upload, primary_server) = upload_for(&primary, cache.path()).await;
    let (mirror_upload, mirror_server) = upload_for(&mirror, cache.path()).await;

    let (trigger, listener) = triggered::trigger();
    let handles = vec![
        Box::new(primary_server).start_task(listener.clone()),
        Box::new(mirror_server).start_task(listener),
    ];

    let contents = b"the exact same bytes".to_vec();
    let path = write_file(cache.path(), KEY, &contents).await;

    let uploader = MultiFileUpload::new(primary_upload.clone(), vec![mirror_upload.clone()])
        .expect("multi upload");
    uploader.upload_file(&path).await.expect("fan out");

    primary_upload.wait_for_uploads_at_least(1).await;
    mirror_upload.wait_for_uploads_at_least(1).await;

    assert_eq!(contents, file_contents(&primary.bucket_client(), KEY).await);
    assert_eq!(contents, file_contents(&mirror.bucket_client(), KEY).await);

    // Nothing is left on disk: the sink's link went when it was handed over,
    // and each uploader dropped its own once the bucket had the file.
    assert!(!path.exists());
    assert!(!primary_upload.dir().expect("staged dir").join(KEY).exists());
    assert!(!mirror_upload.dir().expect("staged dir").join(KEY).exists());

    trigger.trigger();
    for handle in handles {
        handle.await.expect("uploader task");
    }

    primary.cleanup().await.expect("cleanup primary");
    mirror.cleanup().await.expect("cleanup mirror");
}

/// The point of the fan-out: three buckets cost one copy of the bytes, not
/// three. Servers are deliberately never started, so the staged links are still
/// there to inspect.
#[tokio::test]
async fn hardlinks_into_each_bucket_rather_than_copying() {
    let cache = tempfile::tempdir().expect("tempdir");

    let mut uploads = Vec::new();
    // The servers are never started, but they hold the receiving end of each
    // queue: dropping them would close the channel out from under the fan-out.
    let mut _servers = Vec::new();
    for bucket in ["bucket-a", "bucket-b", "bucket-c"] {
        let (upload, server) =
            FileUpload::staged_in(offline_bucket(bucket), cache.path().join(bucket))
                .await
                .expect("file upload");
        uploads.push(upload);
        _servers.push(server);
    }

    let contents = b"one inode, three names".to_vec();
    let path = write_file(cache.path(), KEY, &contents).await;
    let source_ino = std::fs::metadata(&path).expect("source metadata").ino();

    let (first, rest) = uploads.split_first().expect("at least one upload");
    let uploader = MultiFileUpload::new(first.clone(), rest.to_vec()).expect("multi upload");
    uploader.upload_file(&path).await.expect("fan out");

    // The sink's link is spent, but the bytes live on behind the staged ones.
    assert!(!path.exists());

    for upload in &uploads {
        let link = upload.dir().expect("staged dir").join(KEY);
        let meta = std::fs::metadata(&link).expect("staged link metadata");
        assert_eq!(
            source_ino,
            meta.ino(),
            "{} is a copy, not a link to the original",
            link.display()
        );
        assert_eq!(3, meta.nlink(), "expected one link per bucket");
        assert_eq!(contents, std::fs::read(&link).expect("read staged link"));
    }
}

/// Each bucket owns its own link, so one that is failing neither holds nor
/// deletes a copy any other bucket cares about.
#[tokio::test]
async fn a_failing_bucket_keeps_only_its_own_copy() {
    let primary = AwsLocal::new().await;
    primary.create_bucket().await.expect("create primary");

    let cache = tempfile::tempdir().expect("tempdir");
    let (primary_upload, primary_server) = upload_for(&primary, cache.path()).await;
    // A bucket that was never created: every put against it fails.
    let (missing_upload, missing_server) = FileUpload::staged_in(
        BucketClient {
            client: primary.aws_client(),
            bucket: "bucket-that-does-not-exist".to_string(),
        },
        cache.path().join("missing"),
    )
    .await
    .expect("file upload");

    let (trigger, listener) = triggered::trigger();
    let handles = vec![
        Box::new(primary_server).start_task(listener.clone()),
        // Give up immediately rather than sitting through the production
        // backoff.
        Box::new(missing_server.with_retry_policy(0, Duration::from_millis(1)))
            .start_task(listener),
    ];

    let contents = b"kept on disk".to_vec();
    let path = write_file(cache.path(), KEY, &contents).await;

    let uploader = MultiFileUpload::new(primary_upload.clone(), vec![missing_upload.clone()])
        .expect("multi upload");
    uploader.upload_file(&path).await.expect("fan out");

    primary_upload.wait_for_uploads_at_least(1).await;
    missing_upload.wait_for_uploads_at_least(1).await;

    // The working bucket stored the file and dropped its link...
    assert_eq!(contents, file_contents(&primary.bucket_client(), KEY).await);
    assert!(!primary_upload.dir().expect("staged dir").join(KEY).exists());
    // ...while the failing one still holds the bytes for its own retry.
    let kept = missing_upload.dir().expect("staged dir").join(KEY);
    assert!(kept.exists());
    assert_eq!(contents, std::fs::read(&kept).expect("read kept link"));

    trigger.trigger();
    for handle in handles {
        handle.await.expect("uploader task");
    }

    primary.cleanup().await.expect("cleanup primary");
}

/// What makes a bucket that was down recoverable: whatever is still staged in
/// its directory is picked up when it next starts.
#[tokio::test]
async fn resumes_files_left_in_its_directory() {
    let bucket = AwsLocal::new().await;
    bucket.create_bucket().await.expect("create bucket");

    let cache = tempfile::tempdir().expect("tempdir");
    let (upload, server) = upload_for(&bucket, cache.path()).await;

    // Staged by a previous run that ended before the bucket had it. Nothing is
    // queued through the channel here — the startup scan is the only thing that
    // can find it.
    let contents = b"left over from last time".to_vec();
    write_file(upload.dir().expect("staged dir"), KEY, &contents).await;

    let (trigger, listener) = triggered::trigger();
    let handle = Box::new(server).start_task(listener);

    upload.wait_for_uploads_at_least(1).await;

    assert_eq!(contents, file_contents(&bucket.bucket_client(), KEY).await);
    assert!(!upload.dir().expect("staged dir").join(KEY).exists());

    trigger.trigger();
    handle.await.expect("uploader task");

    bucket.cleanup().await.expect("cleanup");
}

/// The upload counter is what an operator alerts on, so a break in it is silent
/// by construction — worth pinning the label shape down.
#[tokio::test]
async fn records_upload_outcome_per_bucket() {
    let snapshotter = snapshotter();

    let good = AwsLocal::new().await;
    good.create_bucket().await.expect("create bucket");
    let missing_bucket = format!("{}-never-created", good.bucket());

    let cache = tempfile::tempdir().expect("tempdir");
    let (good_upload, good_server) = upload_for(&good, cache.path()).await;
    let (missing_upload, missing_server) = FileUpload::staged_in(
        BucketClient {
            client: good.aws_client(),
            bucket: missing_bucket.clone(),
        },
        cache.path().join("missing"),
    )
    .await
    .expect("file upload");

    let (trigger, listener) = triggered::trigger();
    let handles = vec![
        Box::new(good_server).start_task(listener.clone()),
        Box::new(missing_server.with_retry_policy(0, Duration::from_millis(1)))
            .start_task(listener),
    ];

    let path = write_file(cache.path(), KEY, b"counted").await;
    let uploader = MultiFileUpload::new(good_upload.clone(), vec![missing_upload.clone()])
        .expect("multi upload");
    uploader.upload_file(&path).await.expect("fan out");

    good_upload.wait_for_uploads_at_least(1).await;
    missing_upload.wait_for_uploads_at_least(1).await;

    trigger.trigger();
    for handle in handles {
        handle.await.expect("uploader task");
    }

    // `snapshot()` drains, so collect once and assert against that.
    let counts = upload_counts(&snapshotter);
    let count = |bucket: &str, status: &str| counts.get(&(bucket.to_string(), status.into()));

    assert_eq!(Some(&1), count(good.bucket(), "ok"));
    assert_eq!(Some(&0), count(good.bucket(), "error"));
    assert_eq!(Some(&1), count(&missing_bucket, "error"));
    // Seeded at startup, so "no failures" is reported rather than absent.
    assert_eq!(Some(&0), count(&missing_bucket, "ok"));

    good.cleanup().await.expect("cleanup");
}

/// A `FileUpload` for one of `AwsLocal`'s buckets, staging under
/// `cache/<bucket>` the way ingest lays it out.
async fn upload_for(aws: &AwsLocal, cache: &Path) -> (FileUpload, FileUploadServer) {
    FileUpload::staged_in(aws.bucket_client(), cache.join(aws.bucket()))
        .await
        .expect("file upload")
}

/// A bucket client that is never talked to — for tests that stop before any
/// upload is attempted.
fn offline_bucket(bucket: &str) -> BucketClient {
    BucketClient {
        client: aws_sdk_s3::Client::from_conf(
            aws_sdk_s3::Config::builder()
                .behavior_version_latest()
                .build(),
        ),
        bucket: bucket.to_string(),
    }
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
