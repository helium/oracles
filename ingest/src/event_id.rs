use crate::{Error, Result};
use helium_proto::services::poc_mobile::{
    CellHeartbeatReqV1, CellHeartbeatRespV1, SpeedtestReqV1, SpeedtestRespV1,
};
use poc_store::{heartbeat::CellHeartbeat, speedtest::CellSpeedtest};
use serde::Serialize;
use sha2::{Digest, Sha256};

pub struct EventId(String);

impl Serialize for EventId {
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_str(&self.0)
    }
}

impl ToString for EventId {
    fn to_string(&self) -> String {
        self.0.to_string()
    }
}

impl<M: helium_proto::Message> From<&M> for EventId {
    fn from(event: &M) -> Self {
        Self(base64::encode(Sha256::digest(&event.encode_to_vec())))
    }
}

impl TryFrom<CellHeartbeat> for EventId {
    type Error = Error;
    fn try_from(event: CellHeartbeat) -> Result<Self> {
        let req = CellHeartbeatReqV1::from(event);
        Ok(Self::from(&req))
    }
}

impl TryFrom<CellSpeedtest> for EventId {
    type Error = Error;
    fn try_from(event: CellSpeedtest) -> Result<Self> {
        let req = SpeedtestReqV1::try_from(event)?;
        Ok(Self::from(&req))
    }
}

impl From<EventId> for CellHeartbeatRespV1 {
    fn from(v: EventId) -> Self {
        Self { id: v.0 }
    }
}

impl From<EventId> for SpeedtestRespV1 {
    fn from(v: EventId) -> Self {
        Self { id: v.0 }
    }
}
