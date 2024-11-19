use crate::{
    traits::{MsgDecode, MsgTimestamp, TimestampDecode, TimestampEncode},
    Error, Result,
};
use chrono::{DateTime, Utc};
use h3o::CellIndex;
use helium_crypto::PublicKeyBinary;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use std::fmt;

pub mod proto {
    pub use helium_proto::services::poc_mobile::{
        radio_location_estimates_req_v1::Entity, RadioLocationEstimateV1,
        RadioLocationEstimatesReqV1,
    };
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Hash, Eq)]
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

impl From<proto::Entity> for Entity {
    fn from(entity: proto::Entity) -> Self {
        match entity {
            proto::Entity::CbsdId(v) => Entity::CbrsId(v),
            proto::Entity::WifiPubKey(k) => Entity::WifiPubKey(k.into()),
        }
    }
}

impl From<Entity> for proto::Entity {
    fn from(entity: Entity) -> Self {
        match entity {
            Entity::CbrsId(v) => proto::Entity::CbsdId(v),
            Entity::WifiPubKey(k) => proto::Entity::WifiPubKey(k.into()),
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
    type Msg = proto::RadioLocationEstimatesReqV1;
}

impl MsgTimestamp<Result<DateTime<Utc>>> for proto::RadioLocationEstimatesReqV1 {
    fn timestamp(&self) -> Result<DateTime<Utc>> {
        self.timestamp.to_timestamp()
    }
}

impl MsgTimestamp<u64> for RadioLocationEstimatesReq {
    fn timestamp(&self) -> u64 {
        self.timestamp.encode_timestamp()
    }
}

impl From<RadioLocationEstimatesReq> for proto::RadioLocationEstimatesReqV1 {
    fn from(rle: RadioLocationEstimatesReq) -> Self {
        let timestamp = rle.timestamp();
        proto::RadioLocationEstimatesReqV1 {
            entity: Some(rle.entity.into()),
            estimates: rle.estimates.into_iter().map(|e| e.into()).collect(),
            timestamp,
            carrier_key: rle.carrier_key.into(),
            signature: vec![],
        }
    }
}

impl TryFrom<proto::RadioLocationEstimatesReqV1> for RadioLocationEstimatesReq {
    type Error = Error;
    fn try_from(req: proto::RadioLocationEstimatesReqV1) -> Result<Self> {
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
}

impl From<RadioLocationEstimate> for proto::RadioLocationEstimateV1 {
    fn from(rle: RadioLocationEstimate) -> Self {
        proto::RadioLocationEstimateV1 {
            hex: rle.hex.into(),
            grid_distance: rle.grid_distance,
            confidence: Some(to_proto_decimal(rle.confidence)),
        }
    }
}

impl TryFrom<proto::RadioLocationEstimateV1> for RadioLocationEstimate {
    type Error = Error;
    fn try_from(estimate: proto::RadioLocationEstimateV1) -> Result<Self> {
        let hex = CellIndex::try_from(estimate.hex)
            .map_err(crate::error::DecodeError::InvalidCellIndexError)?;

        Ok(Self {
            hex,
            grid_distance: estimate.grid_distance,
            confidence: to_rust_decimal(estimate.confidence)?,
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
