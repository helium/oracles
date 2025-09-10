use base64::Engine;
use chrono::{DateTime, Duration, Utc};
use helium_crypto::{Keypair, PublicKey};
use helium_proto::services::mobile_config::AdminKeyRole as ProtoKeyRole;
use serde::{Deserialize, Serialize};
use std::{ops::Range, sync::Arc};
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Response, Status};

pub mod admin_service;
pub mod authorization_service;
pub mod boosted_hex_info;
pub mod carrier_service;
pub mod client;
pub mod entity_service;
pub mod gateway;
pub mod hex_boosting_service;
pub mod key_cache;
pub mod settings;
pub mod sub_dao_epoch_reward_info;
pub mod sub_dao_service;
pub mod telemetry;

pub use client::{GatewayClient, Settings as ClientSettings};

pub type GrpcResult<T> = Result<Response<T>, Status>;
pub type GrpcStreamResult<T> = ReceiverStream<Result<T, Status>>;

pub fn verify_public_key(bytes: &[u8]) -> Result<PublicKey, Status> {
    PublicKey::try_from(bytes).map_err(|_| Status::invalid_argument("invalid public key"))
}

#[derive(clap::ValueEnum, Clone, Copy, Debug, Eq, Hash, PartialEq, Serialize, sqlx::Type)]
#[sqlx(type_name = "key_role", rename_all = "snake_case")]
pub enum KeyRole {
    Administrator,
    Carrier,
    Oracle,
    Router,
    Pcs,
    Banning,
}

impl KeyRole {
    pub fn from_i32(v: i32) -> anyhow::Result<Self> {
        ProtoKeyRole::try_from(v)
            .map(|kr| kr.into())
            .map_err(|_| anyhow::anyhow!("unsupported key role {}", v))
    }
}

impl From<KeyRole> for i32 {
    fn from(value: KeyRole) -> Self {
        ProtoKeyRole::from(value) as i32
    }
}

impl From<KeyRole> for ProtoKeyRole {
    fn from(key_role: KeyRole) -> Self {
        Self::from(&key_role)
    }
}

impl From<&KeyRole> for ProtoKeyRole {
    fn from(skr: &KeyRole) -> Self {
        match skr {
            KeyRole::Administrator => Self::Administrator,
            KeyRole::Carrier => Self::Carrier,
            KeyRole::Oracle => Self::Oracle,
            KeyRole::Router => Self::Router,
            KeyRole::Pcs => Self::Pcs,
            KeyRole::Banning => Self::Banning,
        }
    }
}

impl From<ProtoKeyRole> for KeyRole {
    fn from(kt: ProtoKeyRole) -> Self {
        match kt {
            ProtoKeyRole::Administrator => Self::Administrator,
            ProtoKeyRole::Carrier => Self::Carrier,
            ProtoKeyRole::Oracle => Self::Oracle,
            ProtoKeyRole::Router => Self::Router,
            ProtoKeyRole::Pcs => Self::Pcs,
            ProtoKeyRole::Banning => Self::Banning,
        }
    }
}

impl std::fmt::Display for KeyRole {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let s = match self {
            Self::Administrator => "administrator",
            Self::Carrier => "carrier",
            Self::Oracle => "oracle",
            Self::Router => "router",
            Self::Pcs => "pcs",
            Self::Banning => "banning",
        };
        f.write_str(s)
    }
}

pub struct EpochInfo {
    pub period: Range<DateTime<Utc>>,
}

impl From<u64> for EpochInfo {
    fn from(next_reward_epoch: u64) -> Self {
        let start_time = DateTime::<Utc>::UNIX_EPOCH + Duration::days(next_reward_epoch as i64);
        let end_time = start_time + Duration::days(1);
        EpochInfo {
            period: start_time..end_time,
        }
    }
}

pub fn deserialize_helium_keypair<'a, D>(deserializer: D) -> Result<Arc<Keypair>, D::Error>
where
    D: serde::Deserializer<'a>,
{
    let string = String::deserialize(deserializer)?;
    let bytes = base64::engine::general_purpose::STANDARD
        .decode(&string)
        .map_err(serde::de::Error::custom)?;

    Keypair::try_from(bytes.as_slice())
        .map(Arc::new)
        .map_err(serde::de::Error::custom)
}
