use chrono::{DateTime, Utc};
use file_store::traits::{MsgDecode, TimestampDecode, TimestampDecodeError, TimestampDecodeResult};
use helium_crypto::PublicKeyBinary;
use helium_proto::services::poc_mobile::{
    CellType, Heartbeat as HeartbeatProto, HeartbeatValidity, LocationSource,
};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::{prost_enum, traits::MsgTimestamp};

#[derive(thiserror::Error, Debug)]
pub enum ValidatedHeartbeatError {
    #[error("invalid timestamp: {0}")]
    Timestamp(#[from] TimestampDecodeError),

    #[error("unsupported cell type: {0}")]
    CellType(prost::UnknownEnumValue),

    #[error("unsupported validity: {0}")]
    Validity(prost::UnknownEnumValue),

    #[error("unsupported location source: {0}")]
    LocationSource(prost::UnknownEnumValue),
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ValidatedHeartbeat {
    pub pubkey: PublicKeyBinary,
    pub cbsd_id: String,
    pub cell_type: CellType,
    pub validity: HeartbeatValidity,
    pub received_timestamp: DateTime<Utc>,
    pub lat: f64,
    pub lon: f64,
    pub coverage_object: Vec<u8>,
    pub location_validation_timestamp: Option<DateTime<Utc>>,
    pub distance_to_asserted: u64,
    /// 0.0..=1.0 score. On the wire this is encoded as a `u32` scaled by
    /// 1000 (see `helium_proto::services::poc_mobile::Heartbeat`); we
    /// unscale on decode so callers don't have to remember the encoding.
    pub location_trust_score_multiplier: Decimal,
    pub location_source: LocationSource,
}

impl ValidatedHeartbeat {
    pub fn coverage_object(&self) -> Option<Uuid> {
        Uuid::from_slice(&self.coverage_object).ok()
    }
}

impl MsgDecode for ValidatedHeartbeat {
    type Msg = HeartbeatProto;
}

impl MsgTimestamp<TimestampDecodeResult> for HeartbeatProto {
    fn timestamp(&self) -> TimestampDecodeResult {
        self.timestamp.to_timestamp()
    }
}

impl TryFrom<HeartbeatProto> for ValidatedHeartbeat {
    type Error = ValidatedHeartbeatError;

    fn try_from(v: HeartbeatProto) -> Result<Self, Self::Error> {
        let location_validation_timestamp = if v.location_validation_timestamp == 0 {
            None
        } else {
            v.location_validation_timestamp.to_timestamp().ok()
        };

        // Wire encoding is `score * 1000` packed in a `u32`. `Decimal::new`
        // with scale=3 reverses that without going through f64.
        let location_trust_score_multiplier =
            Decimal::new(v.location_trust_score_multiplier as i64, 3);

        Ok(Self {
            pubkey: v.pub_key.into(),
            cbsd_id: v.cbsd_id,
            cell_type: prost_enum(v.cell_type, ValidatedHeartbeatError::CellType)?,
            validity: prost_enum(v.validity, ValidatedHeartbeatError::Validity)?,
            received_timestamp: v.timestamp.to_timestamp()?,
            lat: v.lat,
            lon: v.lon,
            coverage_object: v.coverage_object,
            location_validation_timestamp,
            distance_to_asserted: v.distance_to_asserted,
            location_trust_score_multiplier,
            location_source: prost_enum(
                v.location_source,
                ValidatedHeartbeatError::LocationSource,
            )?,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use prost::Message;

    #[test]
    fn decode_validated_heartbeat_proto() {
        let pub_key = vec![1u8, 2, 3, 4];
        let now = Utc::now().timestamp() as u64;
        let proto = HeartbeatProto {
            pub_key: pub_key.clone(),
            timestamp: now,
            cell_type: CellType::NovaGenericWifiIndoor as i32,
            validity: HeartbeatValidity::Valid as i32,
            lat: 37.7749,
            lon: -122.4194,
            coverage_object: Uuid::new_v4().as_bytes().to_vec(),
            location_validation_timestamp: now,
            distance_to_asserted: 42,
            location_trust_score_multiplier: 750,
            location_source: LocationSource::Skyhook as i32,
            ..Default::default()
        };

        let buf = proto.encode_to_vec();
        let decoded = ValidatedHeartbeat::decode(buf.as_slice()).unwrap();

        assert_eq!(decoded.pubkey, PublicKeyBinary::from(pub_key));
        assert_eq!(decoded.validity, HeartbeatValidity::Valid);
        assert_eq!(decoded.distance_to_asserted, 42);
        // Wire value 750 (u32, scaled by 1000) decodes to Decimal 0.750.
        assert_eq!(
            decoded.location_trust_score_multiplier,
            Decimal::new(750, 3)
        );
        assert!(decoded.location_validation_timestamp.is_some());
        assert!(decoded.coverage_object().is_some());
    }

    #[test]
    fn zero_location_validation_timestamp_decodes_to_none() {
        let proto = HeartbeatProto {
            pub_key: vec![1],
            timestamp: Utc::now().timestamp() as u64,
            cell_type: CellType::NovaGenericWifiIndoor as i32,
            validity: HeartbeatValidity::Valid as i32,
            location_trust_score_multiplier: 1000,
            location_source: LocationSource::Gps as i32,
            ..Default::default()
        };

        let buf = proto.encode_to_vec();
        let decoded = ValidatedHeartbeat::decode(buf.as_slice()).unwrap();

        assert!(decoded.location_validation_timestamp.is_none());
        assert_eq!(decoded.distance_to_asserted, 0);
    }
}
