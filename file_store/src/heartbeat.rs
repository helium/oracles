use crate::{
    traits::{MsgDecode, MsgTimestamp, TimestampDecode},
    Error, Result,
};
use chrono::{DateTime, Utc};
use helium_crypto::PublicKeyBinary;
use helium_proto::services::poc_mobile::{CellHeartbeatIngestReportV1, CellHeartbeatReqV1};
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct CbrsHeartbeat {
    pub pubkey: PublicKeyBinary,
    pub hotspot_type: String,
    pub cell_id: u32,
    pub timestamp: DateTime<Utc>,
    pub lon: f64,
    pub lat: f64,
    pub operation_mode: bool,
    pub cbsd_category: String,
    pub cbsd_id: String,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct CbrsHeartbeatIngestReport {
    pub received_timestamp: DateTime<Utc>,
    pub report: CbrsHeartbeat,
}

impl MsgDecode for CbrsHeartbeat {
    type Msg = CellHeartbeatReqV1;
}

impl MsgDecode for CbrsHeartbeatIngestReport {
    type Msg = CellHeartbeatIngestReportV1;
}

impl TryFrom<CellHeartbeatReqV1> for CbrsHeartbeat {
    type Error = Error;
    fn try_from(v: CellHeartbeatReqV1) -> Result<Self> {
        Ok(Self {
            timestamp: v.timestamp.to_timestamp()?,
            pubkey: v.pub_key.into(),
            hotspot_type: v.hotspot_type,
            cell_id: v.cell_id,
            lon: v.lon,
            lat: v.lat,
            operation_mode: v.operation_mode,
            cbsd_category: v.cbsd_category,
            cbsd_id: v.cbsd_id,
        })
    }
}

impl MsgTimestamp<Result<DateTime<Utc>>> for CellHeartbeatReqV1 {
    fn timestamp(&self) -> Result<DateTime<Utc>> {
        self.timestamp.to_timestamp()
    }
}

impl TryFrom<CellHeartbeatIngestReportV1> for CbrsHeartbeatIngestReport {
    type Error = Error;
    fn try_from(v: CellHeartbeatIngestReportV1) -> Result<Self> {
        Ok(Self {
            received_timestamp: v.timestamp()?,
            report: v
                .report
                .ok_or_else(|| Error::not_found("ingest heartbeat report"))?
                .try_into()?,
        })
    }
}

impl MsgTimestamp<Result<DateTime<Utc>>> for CellHeartbeatIngestReportV1 {
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
        let report = CellHeartbeatIngestReportV1 {
            received_timestamp: now as u64,
            report: Some(CellHeartbeatReqV1 {
                pub_key: PK_BYTES.to_vec(),
                hotspot_type: "hotspot".to_string(),
                cell_id: 123,
                timestamp: Utc::now().timestamp() as u64,
                lat: 72.63,
                lon: 72.53,
                operation_mode: true,
                cbsd_category: "category".to_string(),
                cbsd_id: "id".to_string(),
                signature: vec![],
                coverage_object: vec![],
            }),
        };

        let buffer = report.encode_to_vec();

        let heartbeatreport = CbrsHeartbeatIngestReport::decode(buffer.as_slice())
            .expect("unable to decode into CbrsHeartbeat");

        assert_eq!(
            heartbeatreport.received_timestamp,
            Utc.timestamp_millis_opt(now).unwrap()
        );
        assert_eq!(
            report.timestamp().expect("timestamp"),
            heartbeatreport.received_timestamp
        );
        assert_eq!(heartbeatreport.report.cell_id, 123);
    }
}
