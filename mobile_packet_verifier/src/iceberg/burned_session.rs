use chrono::{DateTime, FixedOffset};
use file_store::FileInfo;
use file_store_oracles::mobile_transfer::ValidDataTransferSession;
use helium_iceberg::{FieldDefinition, PartitionDefinition, TableDefinition};
use serde::{Deserialize, Serialize};
use trino_rust_client::Trino;

pub use super::NAMESPACE;
pub const TABLE_NAME: &str = "burned_sessions";

#[derive(Debug, Clone, Trino, Serialize, Deserialize, PartialEq)]
pub struct IcebergBurnedDataTransferSession {
    pub_key: String,
    payer: String,
    upload_bytes: u64,
    download_bytes: u64,
    rewardable_bytes: u64,
    num_dcs: u64,

    /// Timestamp of the first ingest file we found a data transfer session in
    first_timestamp: DateTime<FixedOffset>,
    /// Timestamp of the last ingest file we found a data transfer session in
    last_timestamp: DateTime<FixedOffset>,
    /// Timestamp of when the burn transaction was confirmed
    burn_timestamp: DateTime<FixedOffset>,
}

pub fn table_definition() -> helium_iceberg::Result<TableDefinition> {
    TableDefinition::builder(NAMESPACE, TABLE_NAME)
        .with_fields([
            FieldDefinition::required_string("pub_key"),
            FieldDefinition::required_string("payer"),
            FieldDefinition::required_long("upload_bytes"),
            FieldDefinition::required_long("download_bytes"),
            FieldDefinition::required_long("rewardable_bytes"),
            FieldDefinition::required_long("num_dcs"),
            FieldDefinition::required_timestamptz("first_timestamp"),
            FieldDefinition::required_timestamptz("last_timestamp"),
            FieldDefinition::required_timestamptz("burn_timestamp"),
        ])
        .with_partition(PartitionDefinition::day(
            "burn_timestamp",
            "burn_timestamp_day",
        ))
        .build()
}

pub async fn get_all(
    trino: &trino_rust_client::Client,
) -> anyhow::Result<Vec<IcebergBurnedDataTransferSession>> {
    let all = match trino
        .get_all(format!("SELECT * from {NAMESPACE}.{TABLE_NAME}"))
        .await
    {
        Ok(all) => all.into_vec(),
        Err(trino_rust_client::error::Error::EmptyData) => vec![],
        Err(err) => return Err(err.into()),
    };
    Ok(all)
}

impl From<ValidDataTransferSession> for IcebergBurnedDataTransferSession {
    fn from(value: ValidDataTransferSession) -> Self {
        IcebergBurnedDataTransferSession {
            pub_key: value.pub_key.to_string(),
            payer: value.payer.to_string(),
            upload_bytes: value.upload_bytes,
            download_bytes: value.download_bytes,
            rewardable_bytes: value.rewardable_bytes,
            num_dcs: value.num_dcs,
            first_timestamp: value.first_timestamp.into(),
            last_timestamp: value.last_timestamp.into(),
            burn_timestamp: value.burn_timestamp.into(),
        }
    }
}

impl IcebergBurnedDataTransferSession {
    /// Creates an IcebergBurnedDataTransferSession from a ValidDataTransferSession,
    /// using the file timestamp as a fallback if burn_timestamp is epoch (0).
    ///
    /// The burn_timestamp field was added about a year ago, so older data will have
    /// burn_timestamp set to 0. In those cases, we use the file's timestamp instead.
    pub fn from_session(session: ValidDataTransferSession, file_info: &FileInfo) -> Self {
        let burn_timestamp = if session.burn_timestamp.timestamp_millis() == 0 {
            file_info.timestamp.into()
        } else {
            session.burn_timestamp.into()
        };

        IcebergBurnedDataTransferSession {
            pub_key: session.pub_key.to_string(),
            payer: session.payer.to_string(),
            upload_bytes: session.upload_bytes,
            download_bytes: session.download_bytes,
            rewardable_bytes: session.rewardable_bytes,
            num_dcs: session.num_dcs,
            first_timestamp: session.first_timestamp.into(),
            last_timestamp: session.last_timestamp.into(),
            burn_timestamp,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{TimeZone, Utc};
    use helium_crypto::PublicKeyBinary;

    fn make_session(burn_timestamp: DateTime<chrono::Utc>) -> ValidDataTransferSession {
        ValidDataTransferSession {
            pub_key: PublicKeyBinary::from(vec![1]),
            payer: PublicKeyBinary::from(vec![2]),
            upload_bytes: 100,
            download_bytes: 200,
            rewardable_bytes: 300,
            num_dcs: 10,
            first_timestamp: Utc.with_ymd_and_hms(2024, 1, 1, 0, 0, 0).unwrap(),
            last_timestamp: Utc.with_ymd_and_hms(2024, 1, 1, 1, 0, 0).unwrap(),
            burn_timestamp,
        }
    }

    fn make_file_info(timestamp: DateTime<chrono::Utc>) -> FileInfo {
        FileInfo {
            key: "test-file".to_string(),
            prefix: "test".to_string(),
            timestamp,
            size: 0,
        }
    }

    #[test]
    fn from_session_uses_burn_timestamp_when_set() {
        let burn_time = Utc.with_ymd_and_hms(2024, 6, 15, 12, 0, 0).unwrap();
        let file_time = Utc.with_ymd_and_hms(2024, 6, 15, 13, 0, 0).unwrap();
        let session = make_session(burn_time);
        let file_info = make_file_info(file_time);

        let iceberg_session = IcebergBurnedDataTransferSession::from_session(session, &file_info);

        let expected_burn: DateTime<FixedOffset> = burn_time.into();
        assert_eq!(iceberg_session.burn_timestamp, expected_burn);
    }

    #[test]
    fn from_session_uses_file_timestamp_when_burn_is_epoch() {
        let epoch = Utc.with_ymd_and_hms(1970, 1, 1, 0, 0, 0).unwrap();
        let file_time = Utc.with_ymd_and_hms(2024, 6, 15, 13, 0, 0).unwrap();
        let session = make_session(epoch);
        let file_info = make_file_info(file_time);

        let iceberg_session = IcebergBurnedDataTransferSession::from_session(session, &file_info);

        let expected_burn: DateTime<FixedOffset> = file_time.into();
        assert_eq!(iceberg_session.burn_timestamp, expected_burn);
    }
}
