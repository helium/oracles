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
pub struct RadioLocationEstimates {
    pub radio_id: String,
    pub estimates: Vec<RadioLocationEstimate>,
    pub timestamp: DateTime<Utc>,
    pub signer: PublicKeyBinary,
}

impl MsgDecode for RadioLocationEstimates {
    type Msg = RadioLocationEstimatesReqV1;
}

impl MsgTimestamp<Result<DateTime<Utc>>> for RadioLocationEstimatesReqV1 {
    fn timestamp(&self) -> Result<DateTime<Utc>> {
        self.timestamp.to_timestamp()
    }
}

impl MsgTimestamp<u64> for RadioLocationEstimates {
    fn timestamp(&self) -> u64 {
        self.timestamp.encode_timestamp()
    }
}

impl TryFrom<RadioLocationEstimatesReqV1> for RadioLocationEstimates {
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
            signer: req.signer.into(),
        })
    }
}

#[derive(Clone, Deserialize, Serialize, Debug, PartialEq)]
pub struct RadioLocationEstimate {
    pub radius: Decimal,
    pub confidence: Decimal,
    pub events: Vec<RadioLocationEstimateEvent>,
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
