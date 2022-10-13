use crate::{
    traits::{MsgDecode, MsgTimestamp, TimestampDecode, TimestampEncode},
    Error, Result,
};
use chrono::{DateTime, Utc};
use helium_crypto::PublicKey;
use helium_proto::services::poc_mobile::{SpeedtestIngestReportV1, SpeedtestReqV1};
use serde::{Deserialize, Serialize};

#[derive(Deserialize, Serialize, Debug)]
pub struct CellSpeedtest {
    #[serde(alias = "pubKey")]
    pub pubkey: PublicKey,
    pub serial: String,
    pub timestamp: DateTime<Utc>,
    #[serde(alias = "uploadSpeed")]
    pub upload_speed: u64,
    #[serde(alias = "downloadSpeed")]
    pub download_speed: u64,
    pub latency: u32,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct CellSpeedtestIngestReport {
    pub received_timestamp: DateTime<Utc>,
    pub report: CellSpeedtest,
}

impl MsgDecode for CellSpeedtest {
    type Msg = SpeedtestReqV1;
}

impl MsgDecode for CellSpeedtestIngestReport {
    type Msg = SpeedtestIngestReportV1;
}

impl MsgTimestamp<Result<DateTime<Utc>>> for SpeedtestReqV1 {
    fn timestamp(&self) -> Result<DateTime<Utc>> {
        self.timestamp.to_timestamp()
    }
}

impl MsgTimestamp<u64> for CellSpeedtest {
    fn timestamp(&self) -> u64 {
        self.timestamp.encode_timestamp()
    }
}

impl MsgTimestamp<Result<DateTime<Utc>>> for SpeedtestIngestReportV1 {
    fn timestamp(&self) -> Result<DateTime<Utc>> {
        self.received_timestamp.to_timestamp_millis()
    }
}

impl MsgTimestamp<u64> for CellSpeedtestIngestReport {
    fn timestamp(&self) -> u64 {
        self.received_timestamp.encode_timestamp_millis()
    }
}

impl From<CellSpeedtest> for SpeedtestReqV1 {
    fn from(v: CellSpeedtest) -> Self {
        let timestamp = v.timestamp();
        SpeedtestReqV1 {
            pub_key: v.pubkey.to_vec(),
            serial: v.serial,
            timestamp,
            upload_speed: v.upload_speed,
            download_speed: v.download_speed,
            latency: v.latency,
            signature: vec![],
        }
    }
}

impl TryFrom<SpeedtestReqV1> for CellSpeedtest {
    type Error = Error;
    fn try_from(value: SpeedtestReqV1) -> Result<Self> {
        let timestamp = value.timestamp()?;
        Ok(Self {
            pubkey: PublicKey::try_from(value.pub_key)?,
            serial: value.serial,
            timestamp,
            upload_speed: value.upload_speed,
            download_speed: value.download_speed,
            latency: value.latency,
        })
    }
}

impl TryFrom<SpeedtestIngestReportV1> for CellSpeedtestIngestReport {
    type Error = Error;
    fn try_from(v: SpeedtestIngestReportV1) -> Result<Self> {
        Ok(Self {
            received_timestamp: v.timestamp()?,
            report: v
                .report
                .ok_or_else(|| Error::not_found("ingest speedtest report"))?
                .try_into()?,
        })
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
    fn decode_proto_speed_test_ingest_report_to_internal_struct() {
        let now = Utc::now().timestamp_millis();
        let report = SpeedtestIngestReportV1 {
            received_timestamp: now as u64,
            report: Some(SpeedtestReqV1 {
                pub_key: PK_BYTES.to_vec(),
                serial: "serial".to_string(),
                timestamp: now as u64,
                upload_speed: 6,
                download_speed: 2,
                latency: 1,
                signature: vec![],
            }),
        };

        let buffer = report.encode_to_vec();

        let speedtest_report = CellSpeedtestIngestReport::decode(buffer.as_slice())
            .expect("unable to decode in CellSpeedtestIngestReport");

        assert_eq!(
            speedtest_report.received_timestamp,
            Utc.timestamp_millis(now)
        );
        assert_eq!(speedtest_report.report.serial, "serial");
    }
}
