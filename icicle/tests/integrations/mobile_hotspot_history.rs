//! The mobile hotspot history route end to end: one file in the bucket, the
//! real route over the deployed settings shape, rows read back from Trino.
//!
//! The decisions themselves are tested in the middleware's unit tests. This
//! proves the wiring: the input reads the file, the middleware's rows reach
//! the table and its rejections don't, and the values survive the trip
//! through Arrow, Parquet and Iceberg.

use crate::common::{self, run_test, Harness};
use h3o::{CellIndex, LatLng};
use helium_crypto::PublicKeyBinary;

const CELL: &str = "8c2681a3064d9ff";

#[test]
fn a_file_of_changes_becomes_rows_with_their_asserted_center() -> anyhow::Result<()> {
    run_test(async {
        let h = Harness::new().await?;

        let at = common::minutes_ago(30);
        let file_time = common::minutes_ago(20);

        let mut rejected = common::report(3, CELL, at);
        if let Some(change) = rejected.report.as_mut().and_then(|r| r.change.as_mut()) {
            change.metadata = None;
        }

        h.put_file(
            file_time,
            vec![
                common::report(1, CELL, at), // asserted
                common::report(2, "", at),   // unasserted
                rejected,                    // missing metadata
            ],
        )
        .await?;

        h.run_route().await?;

        let rows = common::rows(&h).await?;
        assert_eq!(
            rows.iter().map(|r| r.record_index).collect::<Vec<_>>(),
            [0, 1],
            "the two accepted reports are written, the rejected one is not"
        );

        for (row, key_byte) in rows.iter().zip([1u8, 2]) {
            assert_eq!(row.file_ts, file_time.timestamp_millis());
            assert_eq!(row.received_ms, at.timestamp_millis());
            assert_eq!(row.timestamp_s, at.timestamp());
            assert_eq!(
                row.pub_key,
                PublicKeyBinary::from(vec![key_byte; 33]).to_string()
            );
            assert!(row.animal_name.is_some());
            assert_eq!(row.device_type, "WIFI_INDOOR");
            assert_eq!(row.signer, "test-signer");
        }

        let center = LatLng::from(CELL.parse::<CellIndex>()?);
        let asserted = &rows[0];
        assert_eq!(asserted.asserted_hex.as_deref(), Some(CELL));
        assert_close(asserted.asserted_lat, center.lat(), "asserted_lat");
        assert_close(asserted.asserted_lon, center.lng(), "asserted_lon");

        let unasserted = &rows[1];
        assert_eq!(unasserted.asserted_hex, None);
        assert_eq!(unasserted.asserted_lat, None);
        assert_eq!(unasserted.asserted_lon, None);

        h.cleanup().await
    })
}

/// Parquet stores doubles exactly; Trino hands them back as JSON numbers, so
/// allow for the last bit of the decimal round trip.
fn assert_close(got: Option<f64>, want: f64, what: &str) {
    let got = got.unwrap_or_else(|| panic!("{what} is empty"));
    assert!((got - want).abs() < 1e-9, "{what}: got {got}, want {want}");
}
