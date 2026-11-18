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
