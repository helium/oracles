use file_store_oracles::{validated_heartbeat::ValidatedHeartbeat, FileType};
use helium_proto::services::poc_mobile::HeartbeatValidity;

use crate::{
    backfill::{Backfiller, IcebergBackfill},
    iceberg::IcebergHeartbeat,
};

pub struct HeartbeatConverter;

impl IcebergBackfill for HeartbeatConverter {
    type FileRecord = ValidatedHeartbeat;
    type IcebergRow = IcebergHeartbeat;
    const FILE_TYPE: FileType = FileType::ValidatedHeartbeat;

    fn convert(record: ValidatedHeartbeat) -> Option<IcebergHeartbeat> {
        (record.validity == HeartbeatValidity::Valid).then(|| record.into())
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
        ValidatedHeartbeat {
            pubkey: PublicKeyBinary::from(vec![1, 2, 3]),
            cbsd_id: String::new(),
            cell_type: CellType::NovaGenericWifiIndoor,
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
}
