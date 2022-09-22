use crate::{datetime_from_epoch, traits::MsgDecode, Error, Result};
use chrono::{DateTime, TimeZone, Utc};
use helium_crypto::PublicKey;
use helium_proto::services::poc_mobile::{CellHeartbeatIngestReportV1, CellHeartbeatReqV1};
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug)]
pub struct CellHeartbeat {
    #[serde(alias = "pubKey")]
    pub pubkey: PublicKey,
    pub hotspot_type: String,
    pub cell_id: u32,
    pub timestamp: DateTime<Utc>,
    #[serde(alias = "longitude")]
    pub lon: f64,
    #[serde(alias = "latitude")]
    pub lat: f64,
    pub operation_mode: bool,
    pub cbsd_category: String,
    pub cbsd_id: String,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct CellHeartbeatIngestReport {
    pub received_timestamp: DateTime<Utc>,
    pub report: CellHeartbeat,
}

impl MsgDecode for CellHeartbeat {
    type Msg = CellHeartbeatReqV1;
}

impl MsgDecode for CellHeartbeatIngestReport {
    type Msg = CellHeartbeatIngestReportV1;
}

impl TryFrom<CellHeartbeatReqV1> for CellHeartbeat {
    type Error = Error;
    fn try_from(v: CellHeartbeatReqV1) -> Result<Self> {
        Ok(Self {
            pubkey: PublicKey::try_from(v.pub_key)?,
            hotspot_type: v.hotspot_type,
            cell_id: v.cell_id,
            timestamp: datetime_from_epoch(v.timestamp),
            lon: v.lon,
            lat: v.lat,
            operation_mode: v.operation_mode,
            cbsd_category: v.cbsd_category,
            cbsd_id: v.cbsd_id,
        })
    }
}

impl TryFrom<CellHeartbeatIngestReportV1> for CellHeartbeatIngestReport {
    type Error = Error;
    fn try_from(v: CellHeartbeatIngestReportV1) -> Result<Self> {
        Ok(Self {
            received_timestamp: Utc.timestamp_millis(v.received_timestamp as i64),
            report: TryFrom::try_from(v.report.unwrap())?,
        })
    }
}

impl From<CellHeartbeat> for CellHeartbeatReqV1 {
    fn from(v: CellHeartbeat) -> Self {
        Self {
            pub_key: v.pubkey.to_vec(),
            hotspot_type: v.hotspot_type,
            cell_id: v.cell_id,
            timestamp: v.timestamp.timestamp() as u64,
            lon: v.lon,
            lat: v.lat,
            operation_mode: v.operation_mode,
            cbsd_category: v.cbsd_category,
            cbsd_id: v.cbsd_id,
            signature: vec![],
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
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
            }),
        };

        let buffer = report.encode_to_vec();

        let cellheartbeatreport = CellHeartbeatIngestReport::decode(buffer.as_slice())
            .expect("unable to decode into CellHeartbeat");

        assert_eq!(
            cellheartbeatreport.received_timestamp,
            Utc.timestamp_millis(now)
        );
        assert_eq!(cellheartbeatreport.report.cell_id, 123);
    }
}
