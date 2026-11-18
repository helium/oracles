//! The mobile hotspot history route end to end: change reports in a bucket
//! become rows of `chain.mobile_hotspot_history`, with the asserted cell's
//! center in `asserted_lat` and `asserted_lon`.
//!
//! Every test runs the real [`mobile_hotspot_history::route`] over the
//! deployed settings shape, so the wiring under test -- input, middleware,
//! output, fresh-state guard -- is the wiring that ships.

use crate::common::{self, run_test, Harness};
use angry_purple_tiger::AnimalName;
use h3o::{CellIndex, LatLng};
use helium_crypto::PublicKeyBinary;
use icicle::data::mobile_hotspot_history;
use trino_rust_client::Trino;

const CELL: &str = "8c2681a3064d9ff";

/// Parquet stores doubles exactly; Trino hands them back as JSON numbers, so
/// allow for the last bit of the decimal round trip.
fn assert_close(got: Option<f64>, want: f64, what: &str) {
    let got = got.unwrap_or_else(|| panic!("{what} is empty"));
    assert!((got - want).abs() < 1e-9, "{what}: got {got}, want {want}");
}

#[test]
fn every_change_becomes_a_row_with_its_asserted_center() -> anyhow::Result<()> {
    run_test(async {
        let h = Harness::new().await?;

        let at = common::minutes_ago(30);
        let file_time = common::minutes_ago(20);
        h.put_file(
            file_time,
            vec![
                common::report(1, CELL, at),
                // Unasserted: no hex, so no center.
                common::report(2, "", at),
                // Not a cell: kept, with no center.
                common::report(3, "not-a-cell", at),
            ],
        )
        .await?;

        h.run_route().await?;

        let rows = common::rows(&h).await?;
        assert_eq!(rows.len(), 3, "every change must be written");

        let center = LatLng::from(CELL.parse::<CellIndex>()?);
        let asserted = &rows[0];
        assert_eq!(asserted.asserted_hex.as_deref(), Some(CELL));
        assert_close(asserted.asserted_lat, center.lat(), "asserted_lat");
        assert_close(asserted.asserted_lon, center.lng(), "asserted_lon");

        let unasserted = &rows[1];
        assert_eq!(unasserted.asserted_hex, None);
        assert_eq!(unasserted.asserted_lat, None);
        assert_eq!(unasserted.asserted_lon, None);

        let invalid = &rows[2];
        assert_eq!(invalid.asserted_hex.as_deref(), Some("not-a-cell"));
        assert_eq!(invalid.asserted_lat, None);
        assert_eq!(invalid.asserted_lon, None);

        h.cleanup().await
    })
}

/// The shared columns must come out exactly as the network table has them.
/// The unit tests cover the conversions; this proves they survive the round
/// trip through Arrow, Parquet and Iceberg.
#[test]
fn the_row_written_matches_the_file_and_the_report() -> anyhow::Result<()> {
    run_test(async {
        let h = Harness::new().await?;

        let at = common::minutes_ago(30);
        let file_time = common::minutes_ago(20);
        h.put_file(
            file_time,
            vec![common::report(1, CELL, at), common::report(2, CELL, at)],
        )
        .await?;

        h.run_route().await?;

        let rows = common::rows(&h).await?;
        assert_eq!(rows.len(), 2);

        for (index, row) in rows.iter().enumerate() {
            let key_byte = index as u8 + 1;
            let pub_key = PublicKeyBinary::from(vec![key_byte; 33]).to_string();

            assert_eq!(
                row.file_ts,
                file_time.timestamp_millis(),
                "file_ts must be the file name's epoch MILLISECONDS"
            );
            assert_eq!(row.record_index, index as i64);
            assert_eq!(row.received_ms, at.timestamp_millis());
            assert_eq!(row.block, 42);
            assert_eq!(row.timestamp_s, at.timestamp());
            assert_eq!(
                row.animal_name,
                Some(pub_key.parse::<AnimalName>()?.to_string())
            );
            assert_eq!(row.pub_key, pub_key);
            assert_eq!(row.asset, bs58::encode(vec![key_byte; 32]).into_string());
            assert_eq!(row.serial_number, Some(format!("SN-{key_byte}")));
            assert_eq!(row.device_type, "WIFI_INDOOR");
            assert_eq!(row.azimuth, Some(180));
            assert_eq!(row.signer, "test-signer");
        }

        h.cleanup().await
    })
}

/// A report the middleware rejects is not written; the rest of its file is.
#[test]
fn a_rejected_report_is_not_written_and_the_rest_is() -> anyhow::Result<()> {
    run_test(async {
        let h = Harness::new().await?;

        let at = common::minutes_ago(30);
        let mut no_metadata = common::report(9, CELL, at);
        if let Some(change) = no_metadata
            .report
            .as_mut()
            .and_then(|report| report.change.as_mut())
        {
            change.metadata = None;
        }

        h.put_file(
            common::minutes_ago(20),
            vec![common::report(1, CELL, at), no_metadata],
        )
        .await?;

        h.run_route().await?;

        let rows = common::rows(&h).await?;
        assert_eq!(rows.len(), 1);
        assert_eq!(
            rows[0].pub_key,
            PublicKeyBinary::from(vec![1u8; 33]).to_string()
        );

        h.cleanup().await
    })
}

/// A restart on the same state directory must neither lose nor duplicate.
#[test]
fn a_restart_skips_files_already_read() -> anyhow::Result<()> {
    run_test(async {
        let h = Harness::new().await?;

        for i in 0..3 {
            h.put_file(
                common::minutes_ago(40 - i),
                vec![common::report(i as u8 + 1, CELL, common::minutes_ago(45))],
            )
            .await?;
        }
        h.run_route().await?;
        assert_eq!(common::count(&h).await?, 3);

        for i in 0..2 {
            h.put_file(
                common::minutes_ago(10 - i),
                vec![common::report(i as u8 + 10, CELL, common::minutes_ago(15))],
            )
            .await?;
        }
        // Same harness, so the same state dir and spool: a restart.
        h.run_route().await?;

        assert_eq!(
            common::count(&h).await?,
            5,
            "a restart lost or duplicated rows"
        );

        h.cleanup().await
    })
}

/// The table must have the network table's columns, in its order, plus
/// `asserted_lat` and `asserted_lon` as two separate double columns.
#[test]
fn the_table_has_the_network_columns_plus_asserted_lat_and_lon() -> anyhow::Result<()> {
    run_test(async {
        let h = Harness::new().await?;

        h.put_file(
            common::minutes_ago(20),
            vec![common::report(1, CELL, common::minutes_ago(30))],
        )
        .await?;
        h.run_route().await?;

        #[derive(Debug, serde::Deserialize, serde::Serialize, Trino)]
        struct Column {
            column_name: String,
            data_type: String,
        }

        let columns = h
            .iceberg
            .trino()
            .get_all::<Column>(format!(
                "SELECT column_name, data_type FROM information_schema.columns \
                 WHERE table_schema = '{}' AND table_name = '{}' \
                 ORDER BY ordinal_position",
                mobile_hotspot_history::DEFAULT_NAMESPACE,
                mobile_hotspot_history::DEFAULT_TABLE_NAME,
            ))
            .await?
            .into_vec();

        let got: Vec<_> = columns
            .iter()
            .map(|c| (c.column_name.as_str(), c.data_type.as_str()))
            .collect();

        assert_eq!(
            got,
            [
                ("file_ts", "bigint"),
                ("record_index", "bigint"),
                ("received_timestamp", "timestamp(6) with time zone"),
                ("block", "bigint"),
                ("timestamp", "timestamp(6) with time zone"),
                ("pub_key", "varchar"),
                ("animal_name", "varchar"),
                ("asset", "varchar"),
                ("serial_number", "varchar"),
                ("device_type", "varchar"),
                ("asserted_hex", "varchar"),
                ("asserted_lat", "double"),
                ("asserted_lon", "double"),
                ("azimuth", "integer"),
                ("signer", "varchar"),
            ]
        );

        h.cleanup().await
    })
}
