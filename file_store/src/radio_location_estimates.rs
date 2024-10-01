use crate::{
    traits::{MsgDecode, MsgTimestamp, TimestampDecode, TimestampEncode},
    Error, Result,
};
use chrono::{DateTime, Utc};
use helium_crypto::PublicKeyBinary;
use helium_proto::services::poc_mobile::{
    RadioLocationEstimateV1, RadioLocationEstimatesReqV1, RleEventV1,
};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

#[derive(Clone, Deserialize, Serialize, Debug, PartialEq)]
pub struct RadioLocationEstimatesReq {
    pub radio_id: String,
    pub estimates: Vec<RadioLocationEstimate>,
    pub timestamp: DateTime<Utc>,
    pub carrier_key: PublicKeyBinary,
}

impl MsgDecode for RadioLocationEstimatesReq {
    type Msg = RadioLocationEstimatesReqV1;
}

impl MsgTimestamp<Result<DateTime<Utc>>> for RadioLocationEstimatesReqV1 {
    fn timestamp(&self) -> Result<DateTime<Utc>> {
        self.timestamp.to_timestamp()
    }
}

impl MsgTimestamp<u64> for RadioLocationEstimatesReq {
    fn timestamp(&self) -> u64 {
        self.timestamp.encode_timestamp()
    }
}

impl From<RadioLocationEstimatesReq> for RadioLocationEstimatesReqV1 {
    fn from(rle: RadioLocationEstimatesReq) -> Self {
        let timestamp = rle.timestamp();
        RadioLocationEstimatesReqV1 {
            radio_id: rle.radio_id,
            estimates: rle
                .estimates
                .into_iter()
                .map(|e| e.try_into().unwrap())
                .collect(),
            timestamp,
            carrier_key: rle.carrier_key.into(),
            signature: vec![],
        }
    }
}

impl TryFrom<RadioLocationEstimatesReqV1> for RadioLocationEstimatesReq {
    type Error = Error;
    fn try_from(req: RadioLocationEstimatesReqV1) -> Result<Self> {
        let timestamp = req.timestamp()?;
        Ok(Self {
            radio_id: req.radio_id,
            estimates: req
                .estimates
                .into_iter()
                .map(|e| e.try_into().unwrap())
                .collect(),
            timestamp,
            carrier_key: req.carrier_key.into(),
        })
    }
}

#[derive(Clone, Deserialize, Serialize, Debug, PartialEq)]
pub struct RadioLocationEstimate {
    pub radius: Decimal,
    pub confidence: Decimal,
    pub events: Vec<RadioLocationEstimateEvent>,
}

impl From<RadioLocationEstimate> for RadioLocationEstimateV1 {
    fn from(rle: RadioLocationEstimate) -> Self {
        RadioLocationEstimateV1 {
            radius: Some(to_proto_decimal(rle.radius)),
            confidence: Some(to_proto_decimal(rle.confidence)),
            events: rle
                .events
                .into_iter()
                .map(|e| e.try_into().unwrap())
                .collect(),
        }
    }
}

impl TryFrom<RadioLocationEstimateV1> for RadioLocationEstimate {
    type Error = Error;
    fn try_from(estimate: RadioLocationEstimateV1) -> Result<Self> {
        Ok(Self {
            radius: to_rust_decimal(estimate.radius.unwrap()),
            confidence: to_rust_decimal(estimate.confidence.unwrap()),
            events: estimate
                .events
                .into_iter()
                .map(|e| e.try_into().unwrap())
                .collect(),
        })
    }
}

#[derive(Clone, Deserialize, Serialize, Debug, PartialEq)]
pub struct RadioLocationEstimateEvent {
    pub id: String,
    pub timestamp: DateTime<Utc>,
}

impl MsgTimestamp<Result<DateTime<Utc>>> for RleEventV1 {
    fn timestamp(&self) -> Result<DateTime<Utc>> {
        self.timestamp.to_timestamp()
    }
}

impl MsgTimestamp<u64> for RadioLocationEstimateEvent {
    fn timestamp(&self) -> u64 {
        self.timestamp.encode_timestamp()
    }
}

impl From<RadioLocationEstimateEvent> for RleEventV1 {
    fn from(event: RadioLocationEstimateEvent) -> Self {
        let timestamp = event.timestamp();
        RleEventV1 {
            id: event.id,
            timestamp,
        }
    }
}

impl TryFrom<RleEventV1> for RadioLocationEstimateEvent {
    type Error = Error;
    fn try_from(event: RleEventV1) -> Result<Self> {
        let timestamp = event.timestamp()?;
        Ok(Self {
            id: event.id,
            timestamp,
        })
    }
}

fn to_rust_decimal(x: helium_proto::Decimal) -> rust_decimal::Decimal {
    let str = x.value.as_str();
    rust_decimal::Decimal::from_str_exact(str).unwrap()
}

fn to_proto_decimal(x: rust_decimal::Decimal) -> helium_proto::Decimal {
    helium_proto::Decimal {
        value: x.to_string(),
    }
}
