use crate::settings::Settings;
use file_store::traits::MsgVerify;
use helium_crypto::{PublicKey, PublicKeyBinary};
use helium_proto::services::iot_config::admin_add_key_req_v1::KeyTypeV1 as ProtoKeyType;
use serde::Serialize;
use sqlx::Row;
use std::{collections::HashSet, sync::Arc};
use tokio::sync::RwLock;

pub async fn get_admin_keys(
    key_type: &KeyType,
    db: impl sqlx::PgExecutor<'_>,
) -> Result<Vec<PublicKey>, AdminAuthError> {
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

pub async fn insert_key(
    pubkey: PublicKeyBinary,
    key_type: KeyType,
    db: impl sqlx::PgExecutor<'_>,
) -> Result<(), sqlx::Error> {
    sqlx::query(r#" insert into admin_keys (pubkey, key_type) values ($1, $2) "#)
        .bind(pubkey)
        .bind(key_type)
        .execute(db)
        .await
        .map(|_| ())
}

pub async fn remove_key(
    pubkey: PublicKeyBinary,
    db: impl sqlx::PgExecutor<'_>,
) -> Result<Option<(PublicKey, KeyType)>, sqlx::Error> {
    let result = sqlx::query(
        r#"
        delete from admin_keys
        where pubkey = $1
        returning (pubkey, key_type)
        "#,
    )
    .bind(pubkey)
    .fetch_optional(db)
    .await?
    .map(|row| {
        (
            row.get::<PublicKey, &str>("pubkey"),
            row.get::<KeyType, &str>("key_type"),
        )
    });

    Ok(result)
}

#[derive(Clone, Debug)]
pub struct AuthCache {
    admin_keys: Arc<RwLock<HashSet<PublicKey>>>,
    router_keys: Arc<RwLock<HashSet<PublicKey>>>,
}

impl AuthCache {
    pub async fn new(
        settings: &Settings,
        db: impl sqlx::PgExecutor<'_> + Copy,
    ) -> Result<Self, AdminAuthError> {
        let config_admin = settings.admin_pubkey()?;

        let mut admin_keys = get_admin_keys(&KeyType::Administrator, db)
            .await?
            .into_iter()
            .collect::<HashSet<PublicKey>>();
        _ = admin_keys.insert(config_admin);

        let router_keys = get_admin_keys(&KeyType::PacketRouter, db)
            .await?
            .into_iter()
            .collect::<HashSet<PublicKey>>();

        Ok(Self {
            admin_keys: Arc::new(RwLock::new(admin_keys)),
            router_keys: Arc::new(RwLock::new(router_keys)),
        })
    }

    pub async fn verify_signature<R>(
        &self,
        key_type: KeyType,
        request: &R,
    ) -> Result<(), AdminAuthError>
    where
        R: MsgVerify,
    {
        match key_type {
            KeyType::Administrator => {
                for key in self.admin_keys.read().await.iter() {
                    if request.verify(key).is_ok() {
                        tracing::debug!("request authorized by admin");
                        return Ok(());
                    }
                }
            }
            KeyType::PacketRouter => {
                for key in self.router_keys.read().await.iter() {
                    if request.verify(key).is_ok() {
                        tracing::debug!("request authorized by packet router {key}");
                        return Ok(());
                    }
                }
            }
        }
        Err(AdminAuthError::UnauthorizedRequest)
    }

    pub async fn insert_key(&self, key_type: KeyType, key: PublicKey) {
        match key_type {
            KeyType::Administrator => self.admin_keys.write(),
            KeyType::PacketRouter => self.router_keys.write(),
        }
        .await
        .insert(key);
    }

    pub async fn remove_key(&self, key_type: KeyType, key: &PublicKey) {
        match key_type {
            KeyType::Administrator => self.admin_keys.write(),
            KeyType::PacketRouter => self.router_keys.write(),
        }
        .await
        .remove(key);
    }

    pub async fn get_keys(&self, key_type: KeyType) -> Vec<PublicKey> {
        match key_type {
            KeyType::Administrator => self.admin_keys.read(),
            KeyType::PacketRouter => self.router_keys.read(),
        }
        .await
        .iter()
        .cloned()
        .collect::<Vec<PublicKey>>()
    }
}

#[derive(Clone, Copy, Debug, Serialize, sqlx::Type)]
#[sqlx(type_name = "key_type", rename_all = "snake_case")]
pub enum KeyType {
    Administrator,
    PacketRouter,
}

#[derive(thiserror::Error, Debug)]
pub enum AdminAuthError {
    #[error("unauthorized admin request signature")]
    UnauthorizedRequest,
    #[error("error deserializing pubkey: {0}")]
    DecodeKey(#[from] helium_crypto::Error),
    #[error("error retrieving saved admin keys: {0}")]
    DbStore(#[from] sqlx::Error),
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
