//! Reading `mobile_hotspot_change_report.<epoch_ms>.gz` files.
//!
//! The input makes no decisions: every message becomes a
//! [`MobileHotspotChangeReport`], the report untouched plus where it came
//! from. Validating and converting it is the middleware's job.

use super::{MobileHotspotChangeReport, FILE_PREFIX};
use glacon_helium::{FileMeta, GlaconInput};
use helium_proto::services::chain_rewardable_entities::MobileHotspotChangeReportV1;

#[derive(Clone, Copy, Debug, Default)]
pub struct MobileHotspotChangeInput;

impl GlaconInput for MobileHotspotChangeInput {
    type Proto = MobileHotspotChangeReportV1;
    type Record = MobileHotspotChangeReport;

    fn prefix(&self) -> &str {
        FILE_PREFIX
    }

    fn decode(
        &self,
        report: MobileHotspotChangeReportV1,
        file: FileMeta<'_>,
    ) -> Option<MobileHotspotChangeReport> {
        Some(MobileHotspotChangeReport {
            file_ts: file.timestamp.timestamp_millis() as u64,
            record_index: file.index as u64,
            report,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::DateTime;

    #[test]
    fn the_prefix_is_the_ingest_file_type() {
        assert_eq!(
            MobileHotspotChangeInput.prefix(),
            "mobile_hotspot_change_report"
        );
    }

    /// The file name carries epoch MILLISECONDS, and the index is the
    /// message's position in the file.
    #[test]
    fn every_message_is_kept_with_its_file_position() {
        let file_ts_ms = 1_700_000_123_456;
        let report = MobileHotspotChangeReportV1 {
            received_timestamp_ms: 1,
            report: None,
        };

        let record = MobileHotspotChangeInput
            .decode(
                report.clone(),
                FileMeta {
                    key: "mobile_hotspot_change_report.1700000123456.gz",
                    timestamp: DateTime::from_timestamp_millis(file_ts_ms).unwrap(),
                    index: 7,
                },
            )
            .expect("the input never drops a message");

        assert_eq!(record.file_ts, file_ts_ms as u64);
        assert_eq!(record.record_index, 7);
        assert_eq!(record.report, report);
    }
}
