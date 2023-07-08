use helium_crypto::PublicKey;
use helium_proto::services::mobile_config::AdminKeyRole as ProtoKeyRole;
use serde::Serialize;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Response, Status};

pub mod admin_service;
pub mod authorization_service;
pub mod client;
pub mod entity_service;
pub mod gateway_info;
pub mod gateway_service;
pub mod key_cache;
pub mod settings;
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
}

impl KeyRole {
    pub fn from_i32(v: i32) -> anyhow::Result<Self> {
        ProtoKeyRole::from_i32(v)
            .map(|kr| kr.into())
            .ok_or_else(|| anyhow::anyhow!("unsupported key role {}", v))
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
        }
    }
}

impl std::fmt::Display for KeyRole {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let s = match self {
            Self::Administrator => "Administrator",
            Self::Carrier => "Carrier",
            Self::Oracle => "Oracle",
            Self::Router => "Router",
            Self::Pcs => "PCS",
        };
        f.write_str(s)
    }
}
