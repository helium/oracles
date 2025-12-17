use aws_local::AwsLocal;
use chrono::DateTime;
use file_store::file_source;
use sqlx::{Executor, PgPool};

// We would like this test to be moved to src/file_info_poller.rs. With
// aws-local currently being it's own workspace, the circular dependency on
// file-store fails to resolve to the same types. Until we can pull that
// functionality into file-store it will live here.
#[sqlx::test]
async fn poller_filters_files_by_exact_prefix(
    pool: PgPool,
) -> Result<(), Box<dyn std::error::Error>> {
    // There is no auto-migration for tests in this lib workspace.
    pool.execute(
        r#"
        CREATE TABLE files_processed (
            process_name TEXT NOT NULL DEFAULT 'default',
            file_name VARCHAR PRIMARY KEY,
            file_type VARCHAR NOT NULL,
            file_timestamp TIMESTAMPTZ NOT NULL,
            processed_at TIMESTAMPTZ NOT NULL
        );
        "#,
    )
    .await?;

    let awsl = AwsLocal::new().await;
    awsl.create_bucket().await?;

    #[derive(Clone, prost::Message)]
    struct TestV1 {}

    #[derive(Clone, prost::Message)]
    struct TestV2 {}

    // Put 1 file of each type with overlapping prefixes
    awsl.put_protos("file_type", vec![TestV1 {}]).await?;
    awsl.put_protos("file_type_v2", vec![TestV2 {}]).await?;

    let (receiver_v1, server_v1) = file_source::Continuous::prost_source::<TestV1, _, _>()
        .state(pool.clone())
        .bucket_client(awsl.bucket_client())
        .lookback_start_after(DateTime::UNIX_EPOCH)
        .prefix("file_type")
        .create()
        .await?;

    let (receiver_v2, server_v2) = file_source::Continuous::prost_source::<TestV2, _, _>()
        .state(pool.clone())
        .bucket_client(awsl.bucket_client())
        .lookback_start_after(DateTime::UNIX_EPOCH)
        .prefix("file_type_v2")
        .create()
        .await?;

    let (trigger, listener) = triggered::trigger();
    let _handle_v1 = tokio::spawn(server_v1.run(listener.clone()));
    let _handle_v2 = tokio::spawn(server_v2.run(listener.clone()));

    let mut v1 = consume_msgs(receiver_v1).await.into_iter();
    let mut v2 = consume_msgs(receiver_v2).await.into_iter();

    assert!(
        v1.all(|f| !f.file_info.key.starts_with("file_type_v2")),
        "Expected no files with prefix 'file_type_v2'"
    );
    assert!(
        v2.all(|f| f.file_info.key.starts_with("file_type_v2")),
        "Expected all files with prefix 'file_type_v2'"
    );

    trigger.trigger();
    awsl.cleanup().await?;

    Ok(())
}

async fn consume_msgs<T>(mut receiver: tokio::sync::mpsc::Receiver<T>) -> Vec<T> {
    use std::time::Duration;
    use tokio::time::timeout;

    let mut msgs = Vec::with_capacity(10);

    // FileInfoPoller puts a single file into the channel at a time. It's easier
    // to loop here with a timeout than sleep some arbitrary amount hoping it
    // will have processed all it's files by then.
    while let Ok(Some(msg)) = timeout(Duration::from_millis(100), receiver.recv()).await {
        msgs.push(msg);
    }

    msgs
}
