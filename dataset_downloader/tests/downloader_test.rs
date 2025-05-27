use std::path::PathBuf;
use std::str::FromStr;

use aws_local::{gen_bucket_name, AwsLocal, AWSLOCAL_DEFAULT_ENDPOINT};
use dataset_downloader::DataSetDownloader;
use sqlx::PgPool;
use tempfile::TempDir;

use hex_assignments::HexBoostData;
use uuid::Uuid;

pub async fn create_data_set_downloader(
    pool: PgPool,
    file_paths: Vec<PathBuf>,
    tmp_dir: &TempDir,
) -> (DataSetDownloader, HexBoostData, String) {
    let bucket_name = gen_bucket_name();

    let awsl = AwsLocal::new(AWSLOCAL_DEFAULT_ENDPOINT, &bucket_name).await;

    for file_path in file_paths {
        awsl.put_file_to_aws(&file_path).await.unwrap();
    }

    let uuid: Uuid = Uuid::new_v4();
    let data_set_directory = tmp_dir.path().join(uuid.to_string());
    tokio::fs::create_dir_all(data_set_directory.clone())
        .await
        .unwrap();

    let file_store = awsl.file_store.clone();

    let mut dsd = DataSetDownloader::new(pool, file_store, data_set_directory);

    let mut hbd = HexBoostData::default();

    hbd = dsd.fetch_first_datasets(hbd).await.unwrap();
    hbd = dsd.check_for_new_data_sets(None, hbd).await.unwrap();

    (dsd, hbd, bucket_name)
}

pub async fn hex_assignment_file_exist(pool: &PgPool, filename: &str) -> bool {
    sqlx::query_scalar::<_, bool>(
        r#"
            SELECT EXISTS(SELECT 1 FROM hex_assignment_data_set_status WHERE filename = $1)
        "#,
    )
    .bind(filename)
    .fetch_one(pool)
    .await
    .unwrap()
}

#[sqlx::test(migrations = "../mobile_verifier/migrations")]
async fn test_dataset_downloader(pool: PgPool) {
    // Scenario:
    // 1. DataSetDownloader downloads initial files
    // 2. Upload a new file
    // 3. DataSetDownloader downloads new file

    let paths = [
        "footfall.1722895200000.gz",
        "urbanization.1722895200000.gz",
        "landtype.1722895200000.gz",
        "service_provider_override.1739404800000.gz",
    ];

    let file_paths: Vec<PathBuf> = paths
        .iter()
        .map(|f| PathBuf::from(format!("./tests/fixtures/{}", f)))
        .collect();

    let tmp_dir = TempDir::new().expect("Unable to create temp dir");
    let (mut data_set_downloader, data_sets, bucket_name) =
        create_data_set_downloader(pool.clone(), file_paths, &tmp_dir).await;
    assert!(hex_assignment_file_exist(&pool, "footfall.1722895200000.gz").await);
    assert!(hex_assignment_file_exist(&pool, "urbanization.1722895200000.gz").await);
    assert!(hex_assignment_file_exist(&pool, "landtype.1722895200000.gz").await);
    assert!(hex_assignment_file_exist(&pool, "service_provider_override.1739404800000.gz").await);

    let awsl = AwsLocal::new(AWSLOCAL_DEFAULT_ENDPOINT, &bucket_name).await;
    awsl.put_file_to_aws(&PathBuf::from_str("./tests/fixtures/footfall.1732895200000.gz").unwrap())
        .await
        .unwrap();
    data_set_downloader
        .check_for_new_data_sets(None, data_sets)
        .await
        .unwrap();
    assert!(hex_assignment_file_exist(&pool, "footfall.1732895200000.gz").await);
}
