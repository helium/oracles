use crate::{
    traits::{MsgDecode, MsgTimestamp, TimestampDecode},
    Error, Result,
};
use chrono::{DateTime, Utc};
use helium_crypto::PublicKeyBinary;
use helium_proto::services::poc_mobile::{WifiHeartbeatIngestReportV1, WifiHeartbeatReqV1};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct WifiHeartbeat {
    pub pubkey: PublicKeyBinary,
    pub lat: f64,
    pub lon: f64,
    pub operation_mode: bool,
    pub location_validation_timestamp: Option<DateTime<Utc>>,
    pub coverage_object: Vec<u8>,
    pub timestamp: DateTime<Utc>,
}

impl WifiHeartbeat {
    pub fn coverage_object(&self) -> Option<Uuid> {
        Uuid::from_slice(&self.coverage_object).ok()
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct WifiHeartbeatIngestReport {
    pub received_timestamp: DateTime<Utc>,
    pub report: WifiHeartbeat,
}

impl MsgDecode for WifiHeartbeat {
    type Msg = WifiHeartbeatReqV1;
}

impl MsgDecode for WifiHeartbeatIngestReport {
    type Msg = WifiHeartbeatIngestReportV1;
}

impl TryFrom<WifiHeartbeatReqV1> for WifiHeartbeat {
    type Error = Error;
    fn try_from(v: WifiHeartbeatReqV1) -> Result<Self> {
        let location_validation_timestamp = if v.location_validation_timestamp == 0 {
            None
        } else {
            v.location_validation_timestamp.to_timestamp().ok()
        };
        Ok(Self {
            pubkey: v.pub_key.into(),
            lat: v.lat,
            lon: v.lon,
            operation_mode: v.operation_mode,
            coverage_object: v.coverage_object,
            timestamp: v.timestamp.to_timestamp()?,
            location_validation_timestamp,
        })
    }
}

impl MsgTimestamp<Result<DateTime<Utc>>> for WifiHeartbeatReqV1 {
    fn timestamp(&self) -> Result<DateTime<Utc>> {
        self.timestamp.to_timestamp()
    }
}

impl TryFrom<WifiHeartbeatIngestReportV1> for WifiHeartbeatIngestReport {
    type Error = Error;
    fn try_from(v: WifiHeartbeatIngestReportV1) -> Result<Self> {
        Ok(Self {
            received_timestamp: v.timestamp()?,
            report: v
                .report
                .ok_or_else(|| Error::not_found("ingest wifi heartbeat report"))?
                .try_into()?,
        })
    }
}

impl MsgTimestamp<Result<DateTime<Utc>>> for WifiHeartbeatIngestReportV1 {
    fn timestamp(&self) -> Result<DateTime<Utc>> {
        self.received_timestamp.to_timestamp_millis()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;
    use hex_literal::hex;
    use prost::Message;

    const PK_BYTES: [u8; 33] =
        hex!("008f23e96ab6bbff48c8923cac831dc97111bcf33dba9f5a8539c00f9d93551af1");

    #[test]
    fn decode_proto_heartbeat_ingest_report_to_internal_struct() {
        let now = Utc::now().timestamp_millis();
        let report1 = WifiHeartbeatIngestReportV1 {
            received_timestamp: now as u64,
            report: Some(WifiHeartbeatReqV1 {
                pub_key: PK_BYTES.to_vec(),
                lat: 72.63,
                lon: 72.53,
                operation_mode: true,
                coverage_object: vec![],
                timestamp: Utc::now().timestamp() as u64,
                location_validation_timestamp: now as u64,
                signature: vec![],
            }),
        };
        let report2 = WifiHeartbeatIngestReportV1 {
            received_timestamp: now as u64,
            report: Some(WifiHeartbeatReqV1 {
                pub_key: PK_BYTES.to_vec(),
                lat: 72.63,
                lon: 72.53,
                operation_mode: true,
                coverage_object: vec![],
                timestamp: Utc::now().timestamp() as u64,
                location_validation_timestamp: 0,
                signature: vec![],
            }),
        };

        let buffer1 = report1.encode_to_vec();
        let buffer2 = report2.encode_to_vec();

        let wifiheartbeatreport1 = WifiHeartbeatIngestReport::decode(buffer1.as_slice())
            .expect("unable to decode into WifiHeartbeat");

        assert_eq!(
            wifiheartbeatreport1.received_timestamp,
            Utc.timestamp_millis_opt(now).unwrap()
        );
        assert_eq!(
            report1.timestamp().expect("timestamp"),
            wifiheartbeatreport1.received_timestamp
        );
        assert!(wifiheartbeatreport1
            .report
            .location_validation_timestamp
            .is_some());

        let wifiheartbeatreport2 = WifiHeartbeatIngestReport::decode(buffer2.as_slice())
            .expect("unable to decode into WifiHeartbeat");

        assert!(wifiheartbeatreport2
            .report
            .location_validation_timestamp
            .is_none());
    }
}
