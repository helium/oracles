use file_store_oracles::{validated_heartbeat::ValidatedHeartbeat, FileType};
use helium_proto::services::poc_mobile::HeartbeatValidity;
use mobile_config::gateway::service::info::DeviceType;

use crate::{
    backfill::{Backfiller, IcebergBackfill},
    iceberg::{self, IcebergHeartbeat},
};

pub struct HeartbeatConverter;

impl IcebergBackfill for HeartbeatConverter {
    type FileRecord = ValidatedHeartbeat;
    type IcebergRow = IcebergHeartbeat;
    const FILE_TYPE: FileType = FileType::ValidatedHeartbeat;

    fn convert(record: ValidatedHeartbeat) -> Option<IcebergHeartbeat> {
        if record.validity != HeartbeatValidity::Valid {
            return None;
        }
        if iceberg::heartbeat::cell_type_to_device_type(record.cell_type) == Some(DeviceType::Cbrs)
        {
            return None;
        }
        Some(record.into())
    }
}

pub type HeartbeatBackfiller = Backfiller<HeartbeatConverter>;

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;
    use helium_crypto::PublicKeyBinary;
    use helium_proto::services::poc_mobile::{CellType, LocationSource};
    use uuid::Uuid;

    fn make_record(validity: HeartbeatValidity) -> ValidatedHeartbeat {
        make_record_with_cell_type(validity, CellType::NovaGenericWifiIndoor)
    }

    fn make_record_with_cell_type(
        validity: HeartbeatValidity,
        cell_type: CellType,
    ) -> ValidatedHeartbeat {
        ValidatedHeartbeat {
            pubkey: PublicKeyBinary::from(vec![1, 2, 3]),
            cbsd_id: String::new(),
            cell_type,
            validity,
            received_timestamp: Utc::now(),
            lat: 37.7749,
            lon: -122.4194,
            coverage_object: Uuid::new_v4().as_bytes().to_vec(),
            location_validation_timestamp: Some(Utc::now()),
            distance_to_asserted: 250,
            location_trust_score_multiplier: rust_decimal::Decimal::new(750, 3),
            location_source: LocationSource::Skyhook,
        }
    }

    #[test]
    fn invalid_heartbeats_are_filtered() {
        let invalid = make_record(HeartbeatValidity::HeartbeatOutsideRange);
        assert!(HeartbeatConverter::convert(invalid).is_none());
    }

    #[test]
    fn valid_heartbeats_pass_through() {
        let record = make_record(HeartbeatValidity::Valid);
        assert!(HeartbeatConverter::convert(record).is_some());
    }

    #[test]
    fn cbrs_heartbeats_are_filtered() {
        for cell_type in [
            CellType::Nova436h,
            CellType::Nova430i,
            CellType::Neutrino430,
            CellType::SercommIndoor,
            CellType::SercommOutdoor,
        ] {
            let record = make_record_with_cell_type(HeartbeatValidity::Valid, cell_type);
            assert!(
                HeartbeatConverter::convert(record).is_none(),
                "expected {cell_type:?} to be skipped"
            );
        }
    }

    #[test]
    fn wifi_outdoor_heartbeats_pass_through() {
        let record =
            make_record_with_cell_type(HeartbeatValidity::Valid, CellType::NovaGenericWifiOutdoor);
        assert!(HeartbeatConverter::convert(record).is_some());
    }
}
