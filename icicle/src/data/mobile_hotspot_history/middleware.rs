//! The route's middleware: every decision about a report, in one place.
//!
//! The input hands over each report untouched. This turns it into a row of
//! `chain.mobile_hotspot_history`, or rejects it with a reason:
//!
//! 1. **Validate.** A report with no report, change or metadata, or with a
//!    timestamp out of range, is rejected as a [`Rejected`]. glacon counts
//!    rejections by [`reason`](GlaconRecordError::reason) in
//!    `glacon_middleware_errors_total` and logs them; the output writes
//!    nothing for them.
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
use h3o::{CellIndex, LatLng};
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
                let pub_key = change
                    .pub_key
                    .map(|key| PublicKeyBinary::from(key.value).to_string())
                    .unwrap_or_default();

                let animal_name = pub_key
                    .parse::<AnimalName>()
                    .ok()
                    .map(|name| name.to_string());

                let asset = change
                    .asset
                    .map(|key| bs58::encode(key.value).into_string())
                    .unwrap_or_default();

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

#[derive(Debug, PartialEq, Eq)]
pub enum AssertedHexError {
    NotHex,
    NotACell,
}

impl std::fmt::Display for AssertedHexError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NotHex => f.write_str("not a hexadecimal u64"),
            Self::NotACell => f.write_str("not a valid H3 cell index"),
        }
    }
}

/// The center of the H3 cell written as `asserted_hex`, with or without a
/// `0x` prefix. The same parse the mobile config's change stream uses.
pub fn cell_center(asserted_hex: &str) -> Result<LatLng, AssertedHexError> {
    let digits = asserted_hex.trim_start_matches("0x");
    let index = u64::from_str_radix(digits, 16).map_err(|_| AssertedHexError::NotHex)?;
    let cell = CellIndex::try_from(index).map_err(|_| AssertedHexError::NotACell)?;
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

    fn change() -> MobileHotspotChangeV1 {
        MobileHotspotChangeV1 {
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
        }
    }

    fn report(change: Option<MobileHotspotChangeV1>) -> MobileHotspotChangeReportV1 {
        MobileHotspotChangeReportV1 {
            received_timestamp_ms: 1_700_000_001_000,
            report: Some(MobileHotspotChangeReqV1 {
                change,
                signer: "signer1".to_string(),
                signature: vec![],
            }),
        }
    }

    fn with_hex(asserted_hex: &str) -> MobileHotspotChangeReportV1 {
        let mut change = change();
        change.metadata.as_mut().unwrap().asserted_hex = asserted_hex.to_string();
        report(Some(change))
    }

    fn record(report: MobileHotspotChangeReportV1, record_index: u64) -> MobileHotspotChangeReport {
        MobileHotspotChangeReport {
            file_ts: FILE_TS_MS,
            record_index,
            report,
        }
    }

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
            .map(|(index, report)| record(report, index as u64))
            .collect();
        MobileHotspotHistoryMiddleware
            .handle_data(&batch, records)
            .await
            .expect("the middleware itself never fails")
    }

    async fn one(report: MobileHotspotChangeReportV1) -> Result<MobileHotspotHistory, Rejected> {
        run(vec![report]).await.remove(0)
    }

    fn expected_center() -> LatLng {
        LatLng::from(CELL.parse::<CellIndex>().unwrap())
    }

    // ------------------------------------------------------------ validate

    #[tokio::test]
    async fn a_missing_report_is_rejected() {
        let proto = MobileHotspotChangeReportV1 {
            received_timestamp_ms: 1000,
            report: None,
        };
        let err = one(proto).await.unwrap_err();
        assert_eq!(err.reason(), "missing_report");
    }

    #[tokio::test]
    async fn a_missing_change_is_rejected() {
        let err = one(report(None)).await.unwrap_err();
        assert_eq!(err.reason(), "missing_change");
    }

    #[tokio::test]
    async fn missing_metadata_is_rejected() {
        let mut change = change();
        change.metadata = None;
        let err = one(report(Some(change))).await.unwrap_err();
        assert_eq!(err.reason(), "missing_metadata");
    }

    #[tokio::test]
    async fn an_out_of_range_timestamp_is_rejected() {
        let mut change = change();
        change.timestamp_seconds = u64::MAX;
        let err = one(report(Some(change))).await.unwrap_err();
        assert_eq!(err.reason(), "invalid_timestamp");
        assert!(err.to_string().contains("timestamp_seconds"), "{err}");

        let mut proto = report(Some(self::change()));
        proto.received_timestamp_ms = u64::MAX;
        let err = one(proto).await.unwrap_err();
        assert_eq!(err.reason(), "invalid_timestamp");
        assert!(err.to_string().contains("received_timestamp_ms"), "{err}");
    }

    /// The message names the record, so the example glacon logs for a batch
    /// can be found in its file.
    #[tokio::test]
    async fn a_rejection_names_its_record() {
        let results = run(vec![report(Some(change())), report(None)]).await;
        let err = results[1].as_ref().unwrap_err();
        assert!(err.to_string().starts_with("record 1:"), "{err}");
    }

    // ------------------------------------------------------------- convert

    #[tokio::test]
    async fn a_valid_report_converts_field_for_field() {
        let row = one(report(Some(change()))).await.expect("valid");

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

    #[test]
    fn every_device_type_is_named_as_in_the_network_table() {
        for (value, name) in [
            (MobileHotspotDeviceType::Unknown, "UNKNOWN"),
            (MobileHotspotDeviceType::Cbrs, "CBRS"),
            (MobileHotspotDeviceType::WifiIndoor, "WIFI_INDOOR"),
            (MobileHotspotDeviceType::WifiOutdoor, "WIFI_OUTDOOR"),
            (MobileHotspotDeviceType::WifiDataOnly, "WIFI_DATA_ONLY"),
        ] {
            assert_eq!(device_type_name(value as i32), name);
        }
    }

    #[tokio::test]
    async fn an_undefined_device_type_is_unknown() {
        let mut change = change();
        change.metadata.as_mut().unwrap().device_type = 9999;
        let row = one(report(Some(change))).await.expect("valid");
        assert_eq!(row.device_type, "UNKNOWN");
    }

    #[tokio::test]
    async fn empty_serial_and_hex_are_none() {
        let mut change = change();
        let metadata = change.metadata.as_mut().unwrap();
        metadata.serial_number = String::new();
        metadata.asserted_hex = String::new();

        let row = one(report(Some(change))).await.expect("valid");
        assert_eq!(row.serial_number, None);
        assert_eq!(row.asserted_hex, None);
    }

    // -------------------------------------------------------------- locate

    #[tokio::test]
    async fn asserted_lat_is_the_cell_centers_latitude() {
        let row = one(with_hex(CELL)).await.expect("valid");
        assert_eq!(row.asserted_lat, Some(expected_center().lat()));
    }

    #[tokio::test]
    async fn asserted_lon_is_the_cell_centers_longitude() {
        let row = one(with_hex(CELL)).await.expect("valid");
        assert_eq!(row.asserted_lon, Some(expected_center().lng()));
    }

    /// Independently of `expected_center`: start from a known place, take its
    /// resolution-12 cell as hex, and the center must land back there. A
    /// swapped lat/lon would put it on the other side of the planet.
    #[tokio::test]
    async fn the_center_is_where_the_cell_is() {
        let (lat, lon) = (37.7749, -122.4194);
        let cell = LatLng::new(lat, lon)
            .unwrap()
            .to_cell(h3o::Resolution::Twelve);
        let hex = format!("{:x}", u64::from(cell));

        let row = one(with_hex(&hex)).await.expect("valid");
        let (got_lat, got_lon) = (row.asserted_lat.unwrap(), row.asserted_lon.unwrap());
        // A resolution-12 cell is about 10 m across; 1e-3 degrees is ~100 m.
        assert!((got_lat - lat).abs() < 1e-3, "lat {got_lat}, want ~{lat}");
        assert!((got_lon - lon).abs() < 1e-3, "lon {got_lon}, want ~{lon}");
    }

    #[test]
    fn a_0x_prefix_is_accepted() {
        assert_eq!(cell_center(&format!("0x{CELL}")), Ok(expected_center()));
    }

    #[tokio::test]
    async fn no_asserted_hex_leaves_both_columns_empty() {
        let row = one(with_hex("")).await.expect("valid");
        assert_eq!(row.asserted_hex, None);
        assert_eq!(row.asserted_lat, None);
        assert_eq!(row.asserted_lon, None);
    }

    #[tokio::test]
    async fn an_invalid_asserted_hex_keeps_the_row_with_both_columns_empty() {
        for hex in ["not-hex", "ffffffffffffffff", "0"] {
            let row = one(with_hex(hex)).await.expect("the row must be kept");
            assert_eq!(row.asserted_hex.as_deref(), Some(hex));
            assert_eq!(row.asserted_lat, None, "{hex}");
            assert_eq!(row.asserted_lon, None, "{hex}");
        }
    }

    #[test]
    fn invalid_hex_says_why() {
        assert_eq!(cell_center("zz"), Err(AssertedHexError::NotHex));
        assert_eq!(cell_center("0"), Err(AssertedHexError::NotACell));
    }

    // --------------------------------------------------------------- shape

    /// glacon fails the route if the result count differs from the record
    /// count, and pairs results to records by position.
    #[tokio::test]
    async fn one_result_per_report_in_order() {
        let results = run(vec![
            with_hex(CELL),
            report(None),
            with_hex(""),
            with_hex("garbage"),
        ])
        .await;

        assert_eq!(results.len(), 4);
        assert_eq!(results[0].as_ref().unwrap().record_index, 0);
        assert_eq!(results[1].as_ref().unwrap_err().reason(), "missing_change");
        assert_eq!(results[2].as_ref().unwrap().record_index, 2);
        assert_eq!(results[3].as_ref().unwrap().record_index, 3);
    }
}
