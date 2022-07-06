use crate::{
    api::{heartbeat::CellHeartbeat, speedtest::CellSpeedtest},
    Error, Result,
};
use helium_proto::services::poc_mobile::{CellHeartbeatRespV1, SpeedtestRespV1};
use serde::Serialize;
use sha2::{Digest, Sha256};

pub struct EventId(String);

impl Serialize for EventId {
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        self.0.serialize(serializer)
    }
}

impl ToString for EventId {
    fn to_string(&self) -> String {
        self.0.to_string()
    }
}

impl<M: helium_proto::Message> From<M> for EventId {
    fn from(event: M) -> Self {
        Self(base64::encode(Sha256::digest(event.encode_to_vec())))
    }
}

impl TryFrom<CellHeartbeat> for EventId {
    type Error = Error;
    fn try_from(event: CellHeartbeat) -> Result<Self> {
        event.try_into()
    }
}

impl TryFrom<CellSpeedtest> for EventId {
    type Error = Error;
    fn try_from(event: CellSpeedtest) -> Result<Self> {
        event.try_into()
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
