//! The route's middleware: every decision about a report, in one place.
//!
//! The input hands over each report untouched. This turns it into a row of
//! `chain.mobile_hotspot_history`, or rejects it with a reason:
//!
//! 1. **Validate.** A report with no report, change, metadata, `pub_key` or
//!    `asset`, or with a timestamp out of range, is rejected as a
//!    [`Rejected`]. glacon counts rejections by
//!    [`reason`](GlaconRecordError::reason) in `glacon_middleware_errors_total`
//!    and logs them; the output writes nothing for them.
//! 2. **Convert**, as `glacon-rs` did for `network.chain.mobile_hotspot_history`:
//!    timestamps, `pub_key` and `asset` to their string forms, `animal_name`
//!    from `pub_key`, `device_type` to its name, empty strings to `None`.
//! 3. **Locate.** Add the center of the asserted H3 cell as `asserted_lat` and
//!    `asserted_lon`. This is the only place H3 math happens, since Trino has
//!    none. With no `asserted_hex`, or one that is not a valid H3 cell, both
//!    are `None` and the row is still written, as the network table keeps it.
//!
//! One result per report, in order. The middleware holds no state, so a
//! replayed batch gives the same results.

use super::{MobileHotspotChangeReport, MobileHotspotHistory};
use angry_purple_tiger::AnimalName;
use glacon_helium::{
    async_trait,
    file_store::traits::{TimestampDecode, TimestampDecodeError},
    BatchId, BoxError, GlaconCoreMiddleware, GlaconRecordError, RecordResult,
};
use h3o::{error::InvalidCellIndex, CellIndex, LatLng};
use helium_crypto::PublicKeyBinary;
use helium_proto::services::chain_rewardable_entities::MobileHotspotDeviceType;

/// Prefix prost's enum names carry, stripped so the column holds `CBRS`,
/// `WIFI_INDOOR`, ... as the network table does.
const DEVICE_TYPE_PREFIX: &str = "mobile_hotspot_device_type_";

#[derive(Clone, Copy, Debug, Default)]
pub struct MobileHotspotHistoryMiddleware;

#[async_trait]
impl GlaconCoreMiddleware<MobileHotspotChangeReport, MobileHotspotHistory>
    for MobileHotspotHistoryMiddleware
{
    type Error = Rejected;

    async fn handle_data(
        &mut self,
        batch: &BatchId,
        records: Vec<MobileHotspotChangeReport>,
    ) -> Result<Vec<RecordResult<MobileHotspotHistory, Rejected>>, BoxError> {
        Ok(records
            .into_iter()
            .map(|record| -> RecordResult<MobileHotspotHistory, Rejected> {
                let MobileHotspotChangeReport {
                    file_ts,
                    record_index,
                    report: proto,
                } = record;

                // 1. Validate.
                let report = proto
                    .report
                    .ok_or(Rejected::MissingReport { record_index })?;
                let change = report
                    .change
                    .ok_or(Rejected::MissingChange { record_index })?;
                let metadata = change
                    .metadata
                    .ok_or(Rejected::MissingMetadata { record_index })?;
                let pub_key = change
                    .pub_key
                    .ok_or(Rejected::MissingPubKey { record_index })?;
                let asset = change
                    .asset
                    .ok_or(Rejected::MissingAsset { record_index })?;

                let received_timestamp = proto
                    .received_timestamp_ms
                    .to_timestamp_millis()
                    .map_err(|source| Rejected::InvalidTimestamp {
                        record_index,
                        field: "received_timestamp_ms",
                        source,
                    })?
                    .fixed_offset();

                let timestamp = change
                    .timestamp_seconds
                    .to_timestamp()
                    .map_err(|source| Rejected::InvalidTimestamp {
                        record_index,
                        field: "timestamp_seconds",
                        source,
                    })?
                    .fixed_offset();

                // 2. Convert.
                let pub_key = PublicKeyBinary::from(pub_key.value).to_string();

                let animal_name = pub_key
                    .parse::<AnimalName>()
                    .ok()
                    .map(|name| name.to_string());

                let asset = bs58::encode(asset.value).into_string();

                let asserted_hex = non_empty(metadata.asserted_hex);

                // 3. Locate.
                let center = asserted_hex
                    .as_deref()
                    .and_then(|hex| match cell_center(hex) {
                        Ok(center) => Some(center),
                        Err(err) => {
                            tracing::warn!(
                                input = %batch.input,
                                file = %batch.key,
                                record_index,
                                %pub_key,
                                asserted_hex = hex,
                                %err,
                                "asserted_hex is not an H3 cell, leaving asserted_lat/asserted_lon empty"
                            );
                            None
                        }
                    });

                Ok(MobileHotspotHistory {
                    file_ts,
                    record_index,
                    received_timestamp,
                    block: change.block,
                    timestamp,
                    pub_key,
                    animal_name,
                    asset,
                    serial_number: non_empty(metadata.serial_number),
                    device_type: device_type_name(metadata.device_type),
                    asserted_hex,
                    asserted_lat: center.map(|center| center.lat()),
                    asserted_lon: center.map(|center| center.lng()),
                    azimuth: metadata.azimuth,
                    signer: report.signer,
                })
            })
            .collect())
    }
}

/// Why a report was not written.
#[derive(Debug, thiserror::Error)]
pub enum Rejected {
    #[error("record {record_index}: missing report")]
    MissingReport { record_index: u64 },

    #[error("record {record_index}: missing change")]
    MissingChange { record_index: u64 },

    #[error("record {record_index}: missing metadata")]
    MissingMetadata { record_index: u64 },

    #[error("record {record_index}: missing pub_key")]
    MissingPubKey { record_index: u64 },

    #[error("record {record_index}: missing asset")]
    MissingAsset { record_index: u64 },

    #[error("record {record_index}: invalid {field}")]
    InvalidTimestamp {
        record_index: u64,
        field: &'static str,
        #[source]
        source: TimestampDecodeError,
    },
}

impl GlaconRecordError for Rejected {
    fn reason(&self) -> &'static str {
        match self {
            Self::MissingReport { .. } => "missing_report",
            Self::MissingChange { .. } => "missing_change",
            Self::MissingMetadata { .. } => "missing_metadata",
            Self::MissingPubKey { .. } => "missing_pub_key",
            Self::MissingAsset { .. } => "missing_asset",
            Self::InvalidTimestamp { .. } => "invalid_timestamp",
        }
    }
}

/// `CBRS`, `WIFI_INDOOR`, `WIFI_OUTDOOR`, `WIFI_DATA_ONLY`, or `UNKNOWN` for
/// both the proto's own unknown value and a value it doesn't define.
fn device_type_name(value: i32) -> String {
    MobileHotspotDeviceType::try_from(value)
        .map(|device_type| {
            let name = device_type.as_str_name();
            name.strip_prefix(DEVICE_TYPE_PREFIX)
                .unwrap_or(name)
                .to_string()
        })
        .unwrap_or_else(|_| "UNKNOWN".to_string())
}

fn non_empty(value: String) -> Option<String> {
    (!value.is_empty()).then_some(value)
}

/// The center of the H3 cell written as `asserted_hex`, with or without a
/// `0x` prefix. h3o's own parse, whose error names the value and the reason.
pub fn cell_center(asserted_hex: &str) -> Result<LatLng, InvalidCellIndex> {
    let cell = asserted_hex.trim_start_matches("0x").parse::<CellIndex>()?;
    Ok(LatLng::from(cell))
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::DateTime;
    use helium_proto::services::chain_rewardable_entities::{
        HeliumPubKey, MobileHotspotChangeReportV1, MobileHotspotChangeReqV1, MobileHotspotChangeV1,
        MobileHotspotMetadata, SolanaPubKey,
    };

    const CELL: &str = "8c2681a3064d9ff";
    const FILE_TS_MS: u64 = 1_700_000_123_456;

    // ------------------------------------------------------------- helpers

    /// A complete, valid report: what every test starts from and breaks one
    /// piece of.
    fn valid() -> MobileHotspotChangeReportV1 {
        MobileHotspotChangeReportV1 {
            received_timestamp_ms: 1_700_000_001_000,
            report: Some(MobileHotspotChangeReqV1 {
                change: Some(MobileHotspotChangeV1 {
                    block: 42,
                    timestamp_seconds: 1_700_000_000,
                    pub_key: Some(HeliumPubKey { value: vec![1; 33] }),
                    asset: Some(SolanaPubKey {
                        value: vec![4, 5, 6],
                    }),
                    metadata: Some(MobileHotspotMetadata {
                        serial_number: "SN123".to_string(),
                        device_type: MobileHotspotDeviceType::WifiIndoor as i32,
                        asserted_hex: CELL.to_string(),
                        azimuth: 180,
                    }),
                }),
                signer: "signer1".to_string(),
                signature: vec![],
            }),
        }
    }

    fn change(report: &mut MobileHotspotChangeReportV1) -> &mut MobileHotspotChangeV1 {
        report.report.as_mut().unwrap().change.as_mut().unwrap()
    }

    fn metadata(report: &mut MobileHotspotChangeReportV1) -> &mut MobileHotspotMetadata {
        change(report).metadata.as_mut().unwrap()
    }

    fn with_hex(asserted_hex: &str) -> MobileHotspotChangeReportV1 {
        let mut report = valid();
        metadata(&mut report).asserted_hex = asserted_hex.to_string();
        report
    }

    /// Runs reports through the middleware as one batch, the way glacon does:
    /// each report's `record_index` is its position.
    async fn run(
        reports: Vec<MobileHotspotChangeReportV1>,
    ) -> Vec<Result<MobileHotspotHistory, Rejected>> {
        let batch = BatchId {
            input: "chain".into(),
            key: "mobile_hotspot_change_report.1700000123456.gz".to_string(),
            timestamp: DateTime::from_timestamp_millis(FILE_TS_MS as i64).unwrap(),
        };
        let records = reports
            .into_iter()
            .enumerate()
            .map(|(index, report)| MobileHotspotChangeReport {
                file_ts: FILE_TS_MS,
                record_index: index as u64,
                report,
            })
            .collect();
        MobileHotspotHistoryMiddleware
            .handle_data(&batch, records)
            .await
            .expect("the middleware itself never fails")
    }

    async fn one(report: MobileHotspotChangeReportV1) -> Result<MobileHotspotHistory, Rejected> {
        run(vec![report]).await.remove(0)
    }

    async fn rejected(report: MobileHotspotChangeReportV1) -> Rejected {
        one(report).await.expect_err("must be rejected")
    }

    async fn accepted(report: MobileHotspotChangeReportV1) -> MobileHotspotHistory {
        one(report).await.expect("must be accepted")
    }

    fn center_of(hex: &str) -> LatLng {
        LatLng::from(hex.parse::<CellIndex>().unwrap())
    }

    // ------------------------------------------- rejected: one per reason

    #[tokio::test]
    async fn rejects_a_missing_report_as_missing_report() {
        let mut report = valid();
        report.report = None;

        let err = rejected(report).await;
        assert!(matches!(err, Rejected::MissingReport { record_index: 0 }));
        assert_eq!(err.reason(), "missing_report");
        assert_eq!(err.to_string(), "record 0: missing report");
    }

    #[tokio::test]
    async fn rejects_a_missing_change_as_missing_change() {
        let mut report = valid();
        report.report.as_mut().unwrap().change = None;

        let err = rejected(report).await;
        assert!(matches!(err, Rejected::MissingChange { record_index: 0 }));
        assert_eq!(err.reason(), "missing_change");
        assert_eq!(err.to_string(), "record 0: missing change");
    }

    #[tokio::test]
    async fn rejects_missing_metadata_as_missing_metadata() {
        let mut report = valid();
        change(&mut report).metadata = None;

        let err = rejected(report).await;
        assert!(matches!(err, Rejected::MissingMetadata { record_index: 0 }));
        assert_eq!(err.reason(), "missing_metadata");
        assert_eq!(err.to_string(), "record 0: missing metadata");
    }

    #[tokio::test]
    async fn rejects_a_missing_pub_key_as_missing_pub_key() {
        let mut report = valid();
        change(&mut report).pub_key = None;

        let err = rejected(report).await;
        assert!(matches!(err, Rejected::MissingPubKey { record_index: 0 }));
        assert_eq!(err.reason(), "missing_pub_key");
        assert_eq!(err.to_string(), "record 0: missing pub_key");
    }

    #[tokio::test]
    async fn rejects_a_missing_asset_as_missing_asset() {
        let mut report = valid();
        change(&mut report).asset = None;

        let err = rejected(report).await;
        assert!(matches!(err, Rejected::MissingAsset { record_index: 0 }));
        assert_eq!(err.reason(), "missing_asset");
        assert_eq!(err.to_string(), "record 0: missing asset");
    }

    /// Both timestamps share the reason; the message says which one.
    #[tokio::test]
    async fn rejects_an_out_of_range_timestamp_as_invalid_timestamp() {
        let mut bad_received = valid();
        bad_received.received_timestamp_ms = u64::MAX;
        let mut bad_change = valid();
        change(&mut bad_change).timestamp_seconds = u64::MAX;

        for (report, field) in [
            (bad_received, "received_timestamp_ms"),
            (bad_change, "timestamp_seconds"),
        ] {
            let err = rejected(report).await;
            assert!(
                matches!(err, Rejected::InvalidTimestamp { record_index: 0, field: f, .. } if f == field),
                "{err:?}"
            );
            assert_eq!(err.reason(), "invalid_timestamp");
            assert_eq!(err.to_string(), format!("record 0: invalid {field}"));
        }
    }

    // ------------------------------------------------------------- convert

    #[tokio::test]
    async fn converts_a_valid_report_field_for_field() {
        let row = accepted(valid()).await;

        let pub_key = PublicKeyBinary::from(vec![1u8; 33]).to_string();
        assert_eq!(row.file_ts, FILE_TS_MS);
        assert_eq!(row.record_index, 0);
        assert_eq!(row.received_timestamp.timestamp_millis(), 1_700_000_001_000);
        assert_eq!(row.block, 42);
        assert_eq!(row.timestamp.timestamp(), 1_700_000_000);
        assert_eq!(
            row.animal_name,
            Some(pub_key.parse::<AnimalName>().unwrap().to_string())
        );
        assert_eq!(row.pub_key, pub_key);
        assert_eq!(row.asset, bs58::encode([4, 5, 6]).into_string());
        assert_eq!(row.serial_number.as_deref(), Some("SN123"));
        assert_eq!(row.device_type, "WIFI_INDOOR");
        assert_eq!(row.asserted_hex.as_deref(), Some(CELL));
        assert_eq!(row.azimuth, 180);
        assert_eq!(row.signer, "signer1");
    }

    /// The names the network table holds, and `UNKNOWN` for a value the proto
    /// doesn't define.
    #[test]
    fn names_device_types_as_the_network_table_does() {
        for (value, name) in [
            (MobileHotspotDeviceType::Unknown as i32, "UNKNOWN"),
            (MobileHotspotDeviceType::Cbrs as i32, "CBRS"),
            (MobileHotspotDeviceType::WifiIndoor as i32, "WIFI_INDOOR"),
            (MobileHotspotDeviceType::WifiOutdoor as i32, "WIFI_OUTDOOR"),
            (
                MobileHotspotDeviceType::WifiDataOnly as i32,
                "WIFI_DATA_ONLY",
            ),
            (9999, "UNKNOWN"),
        ] {
            assert_eq!(device_type_name(value), name);
        }
    }

    #[tokio::test]
    async fn turns_an_empty_serial_into_none() {
        let mut report = valid();
        metadata(&mut report).serial_number = String::new();
        assert_eq!(accepted(report).await.serial_number, None);
    }

    // ------------------------------------------------------------------ h3

    #[tokio::test]
    async fn asserted_lat_and_lon_are_the_cells_center() {
        let row = accepted(with_hex(CELL)).await;
        assert_eq!(row.asserted_lat, Some(center_of(CELL).lat()));
        assert_eq!(row.asserted_lon, Some(center_of(CELL).lng()));
    }

    /// Independent of h3o's own answer above: start from a known place, take
    /// its resolution-12 cell, and the center must land back there. A swapped
    /// lat/lon would put it on the other side of the planet.
    #[tokio::test]
    async fn the_center_is_where_the_cell_is() {
        let (lat, lon) = (37.7749, -122.4194);
        let cell = LatLng::new(lat, lon)
            .unwrap()
            .to_cell(h3o::Resolution::Twelve);

        let row = accepted(with_hex(&format!("{:x}", u64::from(cell)))).await;
        // A resolution-12 cell is about 10 m across; 1e-3 degrees is ~100 m.
        assert!((row.asserted_lat.unwrap() - lat).abs() < 1e-3);
        assert!((row.asserted_lon.unwrap() - lon).abs() < 1e-3);
    }

    #[tokio::test]
    async fn a_0x_prefixed_hex_gives_the_same_center() {
        let row = accepted(with_hex(&format!("0x{CELL}"))).await;
        assert_eq!(row.asserted_lat, Some(center_of(CELL).lat()));
        assert_eq!(row.asserted_lon, Some(center_of(CELL).lng()));
    }

    /// An unasserted hotspot is a normal row with no location.
    #[tokio::test]
    async fn no_asserted_hex_is_kept_with_no_location() {
        let row = accepted(with_hex("")).await;
        assert_eq!(row.asserted_hex, None);
        assert_eq!(row.asserted_lat, None);
        assert_eq!(row.asserted_lon, None);
    }

    /// A hex that isn't a cell is not a rejection: the row is kept, as the
    /// network table keeps it, with the hex as sent and no location.
    #[tokio::test]
    async fn an_invalid_asserted_hex_is_kept_with_no_location() {
        for hex in ["not-hex", "ffffffffffffffff", "0"] {
            let row = accepted(with_hex(hex)).await;
            assert_eq!(row.asserted_hex.as_deref(), Some(hex));
            assert_eq!(row.asserted_lat, None, "{hex}");
            assert_eq!(row.asserted_lon, None, "{hex}");
        }
        assert_eq!(cell_center("zz").unwrap_err().value, None, "not hex");
        assert_eq!(cell_center("0").unwrap_err().value, Some(0), "not a cell");
    }

    // --------------------------------------------------------------- batch

    /// glacon fails the route if the result count differs from the record
    /// count, and pairs results with records by position.
    #[tokio::test]
    async fn returns_one_result_per_report_in_order() {
        let mut missing_change = valid();
        missing_change.report.as_mut().unwrap().change = None;

        let results = run(vec![with_hex(CELL), missing_change, with_hex("")]).await;

        assert_eq!(results.len(), 3);
        assert_eq!(results[0].as_ref().unwrap().record_index, 0);
        assert!(matches!(
            results[1],
            Err(Rejected::MissingChange { record_index: 1 })
        ));
        assert_eq!(results[2].as_ref().unwrap().record_index, 2);
    }
}
