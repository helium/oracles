use file_store::{
    aws_local::AwsLocal,
    file_upload::{FileUpload, FileUploadServer, UPLOAD_METRIC},
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
    let (uploader, servers) = FileUpload::new(
        vec![primary.bucket_client(), mirror.bucket_client()],
        cache.path(),
    )
    .await
    .expect("file upload");

    let (trigger, listener) = triggered::trigger();
    let handles = start(servers, &listener);

    let contents = b"the exact same bytes".to_vec();
    let path = write_file(cache.path(), KEY, &contents).await;
    uploader.upload_file(&path).await.expect("fan out");

    uploader.wait_for_uploads_at_least(1).await;

    assert_eq!(contents, file_contents(&primary.bucket_client(), KEY).await);
    assert_eq!(contents, file_contents(&mirror.bucket_client(), KEY).await);

    // Nothing is left on disk: the caller's link went at handover, and each
    // uploader dropped its own once the bucket had the file.
    assert!(!path.exists());
    for upload in uploader.uploads() {
        assert!(!upload.dir().join(KEY).exists());
    }

    trigger.trigger();
    join(handles).await;

    primary.cleanup().await.expect("cleanup primary");
    mirror.cleanup().await.expect("cleanup mirror");
}

/// One bucket takes the same path as several: the file is staged in that
/// bucket's own directory rather than uploaded where it was handed over.
#[tokio::test]
async fn a_single_bucket_stages_in_its_own_directory() {
    let cache = tempfile::tempdir().expect("tempdir");

    // Servers are never started; they just hold the queues open.
    let (uploader, _servers) = FileUpload::new(vec![offline_bucket("solo")], cache.path())
        .await
        .expect("file upload");

    let dir = uploader.uploads()[0].dir().to_path_buf();
    assert_eq!(cache.path().join("solo"), dir);
    assert!(dir.is_dir(), "the directory is created up front");

    let contents = b"staged by one bucket".to_vec();
    let path = write_file(cache.path(), KEY, &contents).await;
    uploader.upload_file(&path).await.expect("hand over");

    assert!(!path.exists());
    assert_eq!(
        contents,
        std::fs::read(dir.join(KEY)).expect("read staged file")
    );
}

/// The point of the fan-out: three buckets cost one copy of the bytes, not
/// three. Servers are deliberately never started, so the staged links are still
/// there to inspect.
#[tokio::test]
async fn hardlinks_into_each_bucket_rather_than_copying() {
    let cache = tempfile::tempdir().expect("tempdir");
    let buckets = ["bucket-a", "bucket-b", "bucket-c"].map(offline_bucket);

    let (uploader, _servers) = FileUpload::new(buckets.to_vec(), cache.path())
        .await
        .expect("file upload");

    let contents = b"one inode, three names".to_vec();
    let path = write_file(cache.path(), KEY, &contents).await;
    let source_ino = std::fs::metadata(&path).expect("source metadata").ino();

    uploader.upload_file(&path).await.expect("fan out");

    // The caller's link is spent, but the bytes live on behind the staged ones.
    assert!(!path.exists());

    for upload in uploader.uploads() {
        let link = upload.dir().join(KEY);
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
    let good = AwsLocal::new().await;
    good.create_bucket().await.expect("create bucket");

    let cache = tempfile::tempdir().expect("tempdir");
    // A bucket that was never created: every put against it fails.
    let missing = BucketClient {
        client: good.aws_client(),
        bucket: "bucket-that-does-not-exist".to_string(),
    };
    let (uploader, mut servers) =
        FileUpload::new(vec![good.bucket_client(), missing], cache.path())
            .await
            .expect("file upload");

    // Give up on the missing bucket immediately rather than sitting through the
    // production backoff.
    let failing = servers.pop().expect("missing bucket server");
    servers.push(failing.with_retry_policy(0, Duration::from_millis(1)));

    let (trigger, listener) = triggered::trigger();
    let handles = start(servers, &listener);

    let contents = b"kept on disk".to_vec();
    let path = write_file(cache.path(), KEY, &contents).await;
    uploader.upload_file(&path).await.expect("fan out");

    uploader.wait_for_uploads_at_least(1).await;

    // The working bucket stored the file and dropped its link...
    assert_eq!(contents, file_contents(&good.bucket_client(), KEY).await);
    assert!(!uploader.uploads()[0].dir().join(KEY).exists());
    // ...while the failing one still holds the bytes for its own retry.
    let kept = uploader.uploads()[1].dir().join(KEY);
    assert!(kept.exists());
    assert_eq!(contents, std::fs::read(&kept).expect("read kept link"));

    trigger.trigger();
    join(handles).await;

    good.cleanup().await.expect("cleanup");
}

/// What makes a bucket that was down recoverable: whatever is still staged in
/// its directory when the uploader is built is picked up.
#[tokio::test]
async fn resumes_files_left_in_its_directory() {
    let bucket = AwsLocal::new().await;
    bucket.create_bucket().await.expect("create bucket");

    let cache = tempfile::tempdir().expect("tempdir");

    // Staged by a previous run that ended before the bucket had it, so it is
    // already there when the uploader is built. Nothing is queued through the
    // channel here — the scan in `new` is the only thing that can find it.
    let dir = cache.path().join(bucket.bucket());
    tokio::fs::create_dir_all(&dir).await.expect("create dir");
    let contents = b"left over from last time".to_vec();
    write_file(&dir, KEY, &contents).await;

    let (uploader, servers) = FileUpload::new(vec![bucket.bucket_client()], cache.path())
        .await
        .expect("file upload");

    let (trigger, listener) = triggered::trigger();
    let handles = start(servers, &listener);

    uploader.wait_for_uploads_at_least(1).await;

    assert_eq!(contents, file_contents(&bucket.bucket_client(), KEY).await);
    assert!(!dir.join(KEY).exists());

    trigger.trigger();
    join(handles).await;

    bucket.cleanup().await.expect("cleanup");
}

/// Reproduces the ordering every service has: `connect` builds the uploaders,
/// then every sink is constructed — and `FileSink::init` hands over whatever it
/// finds left in the root — and only then does the TaskManager start the
/// servers. Scanning at startup rather than at construction would find the link
/// init had just staged and queue the same file a second time.
#[tokio::test]
async fn a_file_handed_over_before_startup_is_not_uploaded_twice() {
    let bucket = AwsLocal::new().await;
    bucket.create_bucket().await.expect("create bucket");

    let cache = tempfile::tempdir().expect("tempdir");
    let (uploader, servers) = FileUpload::new(vec![bucket.bucket_client()], cache.path())
        .await
        .expect("file upload");

    // A file a sink deposited but never handed over — what a crash between the
    // rename and the handover leaves behind.
    let contents = b"handed over once".to_vec();
    let path = write_file(cache.path(), KEY, &contents).await;

    // FileSink::init hands it over, while the servers are still only built.
    uploader.upload_file(&path).await.expect("hand over");

    // Only now does the TaskManager start them.
    let (trigger, listener) = triggered::trigger();
    let handles = start(servers, &listener);

    uploader.wait_for_uploads_at_least(1).await;

    // One file, one trip through the queue. A second completion means it was
    // pulled twice: uploaded twice, and unlinked twice.
    let duplicated = tokio::time::timeout(
        Duration::from_secs(2),
        uploader.wait_for_uploads_at_least(2),
    )
    .await;
    assert!(
        duplicated.is_err(),
        "the same file was queued and processed twice"
    );

    assert_eq!(contents, file_contents(&bucket.bucket_client(), KEY).await);

    trigger.trigger();
    join(handles).await;

    bucket.cleanup().await.expect("cleanup");
}

/// The other half of the crash window: a run that staged the link but died
/// before releasing the source leaves both behind. The scan in `new` queues the
/// link, so when the sink hands the source over and `stage` finds the link
/// already there, queueing it again would upload and unlink the same file
/// twice.
#[tokio::test]
async fn a_leftover_link_and_its_source_are_uploaded_once() {
    let bucket = AwsLocal::new().await;
    bucket.create_bucket().await.expect("create bucket");

    let cache = tempfile::tempdir().expect("tempdir");
    let dir = cache.path().join(bucket.bucket());
    tokio::fs::create_dir_all(&dir).await.expect("create dir");

    // Both names for one inode, exactly as a crash mid-handover leaves them.
    let contents = b"staged but not released".to_vec();
    let source = write_file(cache.path(), KEY, &contents).await;
    std::fs::hard_link(&source, dir.join(KEY)).expect("stage the leftover link");

    let (uploader, servers) = FileUpload::new(vec![bucket.bucket_client()], cache.path())
        .await
        .expect("file upload");

    // FileSink::init finds the source still sitting in the root and hands it
    // over, the way it does on every start.
    uploader.upload_file(&source).await.expect("hand over");

    let (trigger, listener) = triggered::trigger();
    let handles = start(servers, &listener);

    uploader.wait_for_uploads_at_least(1).await;

    let duplicated = tokio::time::timeout(
        Duration::from_secs(2),
        uploader.wait_for_uploads_at_least(2),
    )
    .await;
    assert!(
        duplicated.is_err(),
        "the leftover link was queued by both the scan and the handover"
    );

    assert_eq!(contents, file_contents(&bucket.bucket_client(), KEY).await);
    assert!(!source.exists());
    assert!(!dir.join(KEY).exists());

    trigger.trigger();
    join(handles).await;

    bucket.cleanup().await.expect("cleanup");
}

/// With no buckets, `upload_file` would release the caller's only copy having
/// stored it nowhere.
#[tokio::test]
async fn no_buckets_is_an_error() {
    let cache = tempfile::tempdir().expect("tempdir");
    assert!(FileUpload::new(vec![], cache.path()).await.is_err());
}

/// Directories are named for their bucket, so two buckets with one name would
/// upload and delete each other's files.
#[tokio::test]
async fn two_buckets_sharing_a_name_is_an_error() {
    let cache = tempfile::tempdir().expect("tempdir");
    let buckets = vec![offline_bucket("same-name"), offline_bucket("same-name")];
    assert!(FileUpload::new(buckets, cache.path()).await.is_err());
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
    let missing = BucketClient {
        client: good.aws_client(),
        bucket: missing_bucket.clone(),
    };
    let (uploader, mut servers) =
        FileUpload::new(vec![good.bucket_client(), missing], cache.path())
            .await
            .expect("file upload");

    let failing = servers.pop().expect("missing bucket server");
    servers.push(failing.with_retry_policy(0, Duration::from_millis(1)));

    let (trigger, listener) = triggered::trigger();
    let handles = start(servers, &listener);

    let path = write_file(cache.path(), KEY, b"counted").await;
    uploader.upload_file(&path).await.expect("fan out");
    uploader.wait_for_uploads_at_least(1).await;

    trigger.trigger();
    join(handles).await;

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

fn start(servers: Vec<FileUploadServer>, listener: &triggered::Listener) -> Vec<TaskHandle> {
    servers
        .into_iter()
        .map(|server| Box::new(server).start_task(listener.clone()))
        .collect()
}

type TaskHandle = task_manager::TaskFuture;

async fn join(handles: Vec<TaskHandle>) {
    for handle in handles {
        handle.await.expect("uploader task");
    }
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
