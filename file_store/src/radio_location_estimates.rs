use crate::{
    traits::{MsgDecode, MsgTimestamp, TimestampDecode, TimestampEncode},
    Error, Result,
};
use chrono::{DateTime, Utc};
use h3o::CellIndex;
use helium_crypto::PublicKeyBinary;
use helium_proto::services::poc_mobile::{
    self as proto, RadioLocationCorrelationV1, RadioLocationEstimateV1, RadioLocationEstimatesReqV1,
};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use std::fmt;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum Entity {
    CbrsId(String),
    WifiPubKey(PublicKeyBinary),
}

impl fmt::Display for Entity {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Entity::CbrsId(id) => write!(f, "{}", id),
            Entity::WifiPubKey(pub_key) => write!(f, "{}", pub_key),
        }
    }
}

impl From<proto::radio_location_estimates_req_v1::Entity> for Entity {
    fn from(entity: proto::radio_location_estimates_req_v1::Entity) -> Self {
        match entity {
            proto::radio_location_estimates_req_v1::Entity::CbrsId(v) => Entity::CbrsId(v),
            proto::radio_location_estimates_req_v1::Entity::WifiPubKey(k) => {
                Entity::WifiPubKey(k.into())
            }
        }
    }
}

impl From<Entity> for proto::radio_location_estimates_req_v1::Entity {
    fn from(entity: Entity) -> Self {
        match entity {
            Entity::CbrsId(v) => proto::radio_location_estimates_req_v1::Entity::CbrsId(v),
            Entity::WifiPubKey(k) => {
                proto::radio_location_estimates_req_v1::Entity::WifiPubKey(k.into())
            }
        }
    }
}

#[derive(Clone, Deserialize, Serialize, Debug, PartialEq)]
pub struct RadioLocationEstimatesReq {
    pub entity: Entity,
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
            entity: Some(rle.entity.into()),
            estimates: rle.estimates.into_iter().map(|e| e.into()).collect(),
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
            entity: if let Some(entity) = req.entity {
                entity.into()
            } else {
                return Err(Error::NotFound("entity".to_string()));
            },
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
    pub hex: CellIndex,
    pub grid_distance: u32,
    pub confidence: Decimal,
    pub radio_location_correlations: Vec<RadioLocationCorrelation>,
}

impl From<RadioLocationEstimate> for RadioLocationEstimateV1 {
    fn from(rle: RadioLocationEstimate) -> Self {
        RadioLocationEstimateV1 {
            hex: rle.hex.into(),
            grid_distance: rle.grid_distance,
            confidence: Some(to_proto_decimal(rle.confidence)),
            radio_location_correlations: rle
                .radio_location_correlations
                .into_iter()
                .map(|e| e.into())
                .collect(),
        }
    }
}

impl TryFrom<RadioLocationEstimateV1> for RadioLocationEstimate {
    type Error = Error;
    fn try_from(estimate: RadioLocationEstimateV1) -> Result<Self> {
        let hex = CellIndex::try_from(estimate.hex)
            .map_err(crate::error::DecodeError::InvalidCellIndexError)?;

        Ok(Self {
            hex,
            grid_distance: estimate.grid_distance,
            confidence: to_rust_decimal(estimate.confidence)?,
            radio_location_correlations: estimate
                .radio_location_correlations
                .into_iter()
                .flat_map(|rlc| rlc.try_into())
                .collect(),
        })
    }
}

#[derive(Clone, Deserialize, Serialize, Debug, PartialEq)]
pub struct RadioLocationCorrelation {
    pub id: String,
    pub timestamp: DateTime<Utc>,
}

impl MsgTimestamp<Result<DateTime<Utc>>> for RadioLocationCorrelationV1 {
    fn timestamp(&self) -> Result<DateTime<Utc>> {
        self.timestamp.to_timestamp()
    }
}

impl MsgTimestamp<u64> for RadioLocationCorrelation {
    fn timestamp(&self) -> u64 {
        self.timestamp.encode_timestamp()
    }
}

impl From<RadioLocationCorrelation> for RadioLocationCorrelationV1 {
    fn from(event: RadioLocationCorrelation) -> Self {
        let timestamp = event.timestamp();
        RadioLocationCorrelationV1 {
            id: event.id,
            timestamp,
        }
    }
}

impl TryFrom<RadioLocationCorrelationV1> for RadioLocationCorrelation {
    type Error = Error;
    fn try_from(event: RadioLocationCorrelationV1) -> Result<Self> {
        let timestamp = event.timestamp()?;
        Ok(Self {
            id: event.id,
            timestamp,
        })
    }
}

fn to_rust_decimal(x: Option<helium_proto::Decimal>) -> Result<rust_decimal::Decimal> {
    let x = x.ok_or(Error::NotFound("Decimal".to_string()))?;
    let str = x.value.as_str();
    Ok(rust_decimal::Decimal::from_str_exact(str)?)
}

fn to_proto_decimal(x: rust_decimal::Decimal) -> helium_proto::Decimal {
    helium_proto::Decimal {
        value: x.to_string(),
    }
}
