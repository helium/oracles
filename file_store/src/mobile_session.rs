use crate::{
    traits::{MsgDecode, MsgTimestamp, TimestampDecode},
    Error, Result,
};
use chrono::{DateTime, Utc};
use helium_crypto::PublicKeyBinary;
use helium_proto::services::poc_mobile::{
    DataTransferEvent as DataTransferEventProto, DataTransferRadioAccessTechnology,
    DataTransferSessionIngestReportV1, DataTransferSessionReqV1,
};
use serde::Serialize;

impl MsgTimestamp<Result<DateTime<Utc>>> for DataTransferSessionIngestReportV1 {
    fn timestamp(&self) -> Result<DateTime<Utc>> {
        self.received_timestamp.to_timestamp_millis()
    }
}

#[derive(Serialize, Clone, Debug)]
pub struct DataTransferSessionIngestReport {
    pub received_timestamp: DateTime<Utc>,
    pub report: DataTransferSessionReq,
}

impl MsgDecode for DataTransferSessionIngestReport {
    type Msg = DataTransferSessionIngestReportV1;
}

impl TryFrom<DataTransferSessionIngestReportV1> for DataTransferSessionIngestReport {
    type Error = Error;

    fn try_from(v: DataTransferSessionIngestReportV1) -> Result<Self> {
        Ok(Self {
            received_timestamp: v.timestamp()?,
            report: v
                .report
                .ok_or_else(|| Error::not_found("data transfer session report"))?
                .try_into()?,
        })
    }
}

#[derive(Serialize, Clone, Debug)]
pub struct DataTransferEvent {
    pub pub_key: PublicKeyBinary,
    pub upload_bytes: u64,
    pub download_bytes: u64,
    pub radio_access_technology: DataTransferRadioAccessTechnology,
    pub event_id: String,
    pub payer: PublicKeyBinary,
    pub timestamp: DateTime<Utc>,
    pub signature: Vec<u8>,
}

impl MsgTimestamp<Result<DateTime<Utc>>> for DataTransferEventProto {
    fn timestamp(&self) -> Result<DateTime<Utc>> {
        self.timestamp.to_timestamp_millis()
    }
}

impl MsgDecode for DataTransferEvent {
    type Msg = DataTransferEventProto;
}

impl TryFrom<DataTransferEventProto> for DataTransferEvent {
    type Error = Error;

    fn try_from(v: DataTransferEventProto) -> Result<Self> {
        let timestamp = v.timestamp()?;
        let radio_access_technology = v.radio_access_technology();
        Ok(Self {
            pub_key: v.pub_key.into(),
            upload_bytes: v.upload_bytes,
            download_bytes: v.download_bytes,
            radio_access_technology,
            event_id: v.event_id,
            payer: v.payer.into(),
            timestamp,
            signature: v.signature,
        })
    }
}

#[derive(Serialize, Clone, Debug)]
pub struct DataTransferSessionReq {
    pub data_transfer_usage: DataTransferEvent,
    pub reward_cancelled: bool,
    pub pub_key: PublicKeyBinary,
    pub signature: Vec<u8>,
}

impl MsgDecode for DataTransferSessionReq {
    type Msg = DataTransferSessionReqV1;
}

impl TryFrom<DataTransferSessionReqV1> for DataTransferSessionReq {
    type Error = Error;

    fn try_from(v: DataTransferSessionReqV1) -> Result<Self> {
        Ok(Self {
            reward_cancelled: v.reward_cancelled,
            signature: v.signature,
            data_transfer_usage: v
                .data_transfer_usage
                .ok_or_else(|| Error::not_found("data transfer usage"))?
                .try_into()?,
            pub_key: v.pub_key.into(),
        })
    }
}
