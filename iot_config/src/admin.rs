use helium_crypto::PublicKey;
use helium_proto::services::iot_config::admin_add_key_req_v1::KeyTypeV1 as ProtoKeyType;
use serde::Serialize;

pub async fn get_admin_keys(
    key_type: KeyType,
    db: impl sqlx::PgExecutor<'_>,
) -> Result<Vec<PublicKey>, AdminKeysError> {
    Ok(
        sqlx::query_scalar::<_, PublicKey>(
            r#" select pubkey from admin_keys where key_type = $1 "#,
        )
        .bind(key_type)
        .fetch_all(db)
        .await?
        .into_iter()
        .map(PublicKey::try_from)
        .filter_map(|key| key.ok())
        .collect(),
    )
}

#[derive(Clone, Debug, Serialize, sqlx::Type)]
#[sqlx(type_name = "key_type", rename_all = "snake_case")]
pub enum KeyType {
    Administrator,
    PacketRouter,
}

#[derive(thiserror::Error, Debug)]
pub enum AdminKeysError {
    #[error("error retrieving saved admin keys: {0}")]
    DbError(#[from] sqlx::Error),
    #[error("unable to deserialize pubkey: {0}")]
    DecodeError(#[from] helium_crypto::Error),
}

#[derive(thiserror::Error, Debug)]
#[error("unsupported key type {0}")]
pub struct UnsupportedKeyTypeError(i32);

impl KeyType {
    pub fn from_i32(v: i32) -> Result<Self, UnsupportedKeyTypeError> {
        ProtoKeyType::from_i32(v)
            .map(|kt| kt.into())
            .ok_or(UnsupportedKeyTypeError(v))
    }
}

impl From<KeyType> for ProtoKeyType {
    fn from(key_type: KeyType) -> Self {
        ProtoKeyType::from(&key_type)
    }
}

impl From<&KeyType> for ProtoKeyType {
    fn from(skt: &KeyType) -> Self {
        match skt {
            KeyType::Administrator => ProtoKeyType::Administrator,
            KeyType::PacketRouter => ProtoKeyType::PacketRouter,
        }
    }
}

impl From<ProtoKeyType> for KeyType {
    fn from(kt: ProtoKeyType) -> Self {
        match kt {
            ProtoKeyType::Administrator => KeyType::Administrator,
            ProtoKeyType::PacketRouter => KeyType::PacketRouter,
        }
    }
}
