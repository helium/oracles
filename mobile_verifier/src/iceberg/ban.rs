use chrono::{DateTime, FixedOffset};
use file_store_oracles::mobile_ban::{BanAction, VerifiedBanReport};
use helium_iceberg::{FieldDefinition, PartitionDefinition, SortFieldDefinition, TableDefinition};
use serde::{Deserialize, Serialize};
use trino_rust_client::Trino;

pub use super::NAMESPACE;
pub const TABLE_NAME: &str = "bans";

#[derive(Default, Debug, Clone, Trino, Serialize, Deserialize, PartialEq)]
pub struct IcebergBan {
    pub(crate) pubkey: String,
    pub(crate) ban_pubkey: String,
    pub(crate) report_timestamp: DateTime<FixedOffset>,
    pub(crate) received_timestamp: DateTime<FixedOffset>,
    pub(crate) is_ban: bool,
    pub(crate) hotspot_serial: Option<String>,
    pub(crate) message: Option<String>,
    pub(crate) ban_reason: Option<String>,
    pub(crate) ban_type: Option<String>,
    pub(crate) expiration_timestamp: Option<DateTime<FixedOffset>>,
}

pub fn table_definition() -> helium_iceberg::Result<TableDefinition> {
    TableDefinition::builder(NAMESPACE, TABLE_NAME)
        .with_fields([
            FieldDefinition::required_string("pubkey"),
            FieldDefinition::required_string("ban_pubkey"),
            FieldDefinition::required_timestamptz("report_timestamp"),
            FieldDefinition::required_timestamptz("received_timestamp"),
            FieldDefinition::required_boolean("is_ban"),
            FieldDefinition::optional_string("hotspot_serial"),
            FieldDefinition::optional_string("message"),
            FieldDefinition::optional_string("ban_reason"),
            FieldDefinition::optional_string("ban_type"),
            FieldDefinition::optional_timestamptz("expiration_timestamp"),
        ])
        .with_partition(PartitionDefinition::day(
            "received_timestamp",
            "received_timestamp_day",
        ))
        .with_sort_fields([
            SortFieldDefinition::ascending("pubkey"),
            SortFieldDefinition::ascending("received_timestamp"),
        ])
        .build()
}

pub async fn get_all(trino: &trino_rust_client::Client) -> anyhow::Result<Vec<IcebergBan>> {
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

impl From<&VerifiedBanReport> for IcebergBan {
    fn from(value: &VerifiedBanReport) -> Self {
        let req = &value.report.report;

        let (is_ban, hotspot_serial, message, ban_reason, ban_type, expiration_timestamp) =
            match &req.ban_action {
                BanAction::Ban(details) => (
                    true,
                    Some(details.hotspot_serial.clone()),
                    Some(details.message.clone()),
                    Some(details.reason.as_str_name().to_string()),
                    Some(details.ban_type.as_str_name().to_string()),
                    details.expiration_timestamp.map(|ts| ts.into()),
                ),
                BanAction::Unban(_) => (false, None, None, None, None, None),
            };
        Self {
            pubkey: req.hotspot_pubkey.to_string(),
            ban_pubkey: req.ban_pubkey.to_string(),
            report_timestamp: req.timestamp.into(),
            received_timestamp: value.report.received_timestamp.into(),
            is_ban,
            hotspot_serial,
            message,
            ban_reason,
            ban_type,
            expiration_timestamp,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;
    use file_store_oracles::mobile_ban::{
        proto::{BanReason, VerifiedBanIngestReportStatus},
        BanDetails, BanReport, BanRequest, BanType, UnbanDetails,
    };
    use helium_crypto::PublicKeyBinary;

    fn make_ban_report() -> VerifiedBanReport {
        let hotspot_pubkey = PublicKeyBinary::from(vec![1, 2, 3]);
        let ban_pubkey = PublicKeyBinary::from(vec![4, 5, 6]);
        let now = Utc::now();
        let report_time = now - chrono::Duration::minutes(5);

        VerifiedBanReport {
            verified_timestamp: now,
            report: BanReport {
                received_timestamp: now,
                report: BanRequest {
                    hotspot_pubkey,
                    timestamp: report_time,
                    ban_pubkey,
                    signature: vec![],
                    ban_action: BanAction::Ban(BanDetails {
                        hotspot_serial: "SN-12345".to_string(),
                        message: "test ban".to_string(),
                        reason: BanReason::LocationGaming,
                        ban_type: BanType::Poc,
                        expiration_timestamp: None,
                    }),
                },
            },
            status: VerifiedBanIngestReportStatus::Valid,
        }
    }

    fn make_unban_report() -> VerifiedBanReport {
        let hotspot_pubkey = PublicKeyBinary::from(vec![1, 2, 3]);
        let ban_pubkey = PublicKeyBinary::from(vec![4, 5, 6]);
        let now = Utc::now();

        VerifiedBanReport {
            verified_timestamp: now,
            report: BanReport {
                received_timestamp: now,
                report: BanRequest {
                    hotspot_pubkey,
                    timestamp: now - chrono::Duration::minutes(5),
                    ban_pubkey,
                    signature: vec![],
                    ban_action: BanAction::Unban(UnbanDetails {
                        hotspot_serial: "SN-12345".to_string(),
                        message: "lifting ban".to_string(),
                    }),
                },
            },
            status: VerifiedBanIngestReportStatus::Valid,
        }
    }

    #[test]
    fn table_definition_builds_successfully() {
        let def = table_definition().expect("table definition should build");
        assert_eq!(def.name(), TABLE_NAME);
        assert_eq!(def.namespace(), NAMESPACE);
    }

    #[test]
    fn convert_ban_report() {
        let report = make_ban_report();
        let iceberg = IcebergBan::from(&report);

        assert_eq!(
            iceberg.pubkey,
            report.report.report.hotspot_pubkey.to_string()
        );
        assert_eq!(
            iceberg.ban_pubkey,
            report.report.report.ban_pubkey.to_string()
        );
        assert!(iceberg.is_ban);
        assert_eq!(iceberg.hotspot_serial.as_deref(), Some("SN-12345"));
        assert_eq!(iceberg.message.as_deref(), Some("test ban"));
        assert!(iceberg.ban_reason.is_some());
        assert!(iceberg.ban_type.is_some());
        assert!(iceberg.expiration_timestamp.is_none());
        assert_ne!(
            iceberg.report_timestamp, iceberg.received_timestamp,
            "report_timestamp and received_timestamp should differ"
        );
    }

    #[test]
    fn convert_unban_report() {
        let report = make_unban_report();
        let iceberg = IcebergBan::from(&report);

        assert!(!iceberg.is_ban);
        assert!(iceberg.hotspot_serial.is_none());
        assert!(iceberg.message.is_none());
        assert!(iceberg.ban_reason.is_none());
        assert!(iceberg.ban_type.is_none());
        assert!(iceberg.expiration_timestamp.is_none());
    }
}
