use crate::settings::Settings;
use anyhow::anyhow;
use file_store::traits::MsgVerify;
use helium_crypto::{PublicKey, PublicKeyBinary};
use helium_proto::services::mobile_config::admin_add_key_req_v1::KeyTypeV1 as ProtoKeyType;
use serde::Serialize;
use sqlx::{Pool, Postgres};
use std::{collections::HashSet, sync::Arc};
use tokio::sync::RwLock;

#[derive(Clone, Debug)]
pub struct KeyCache {
    admin_keys: Arc<RwLock<HashSet<PublicKey>>>,
    oracle_keys: Arc<RwLock<HashSet<PublicKey>>>,
    router_keys: Arc<RwLock<HashSet<PublicKey>>>,
    pool: Pool<Postgres>,
}

impl KeyCache {
    pub async fn new(settings: &Settings, pool: Pool<Postgres>) -> anyhow::Result<Self> {
        let config_admin = settings.admin_pubkey()?;

        let mut admin_keys = HashSet::<PublicKey>::new();
        admin_keys.insert(config_admin);

        let mut router_keys = HashSet::<PublicKey>::new();
        let mut oracle_keys = HashSet::<PublicKey>::new();

        db::fetch_stored_keys(&pool.clone())
            .await?
            .into_iter()
            .for_each(|(key_type, pubkey)| {
                match key_type {
                    KeyType::Administrator => admin_keys.insert(pubkey),
                    KeyType::Oracle => oracle_keys.insert(pubkey),
                    KeyType::PacketRouter => router_keys.insert(pubkey),
                };
            });

        Ok(Self {
            admin_keys: Arc::new(RwLock::new(admin_keys)),
            oracle_keys: Arc::new(RwLock::new(oracle_keys)),
            router_keys: Arc::new(RwLock::new(router_keys)),
            pool,
        })
    }

    pub async fn verify_signature<R>(&self, key_type: KeyType, request: &R) -> anyhow::Result<()>
    where
        R: MsgVerify,
    {
        let keys = match key_type {
            KeyType::Administrator => self.admin_keys.read(),
            KeyType::Oracle => self.oracle_keys.read(),
            KeyType::PacketRouter => self.router_keys.read(),
        };
        for key in keys.await.iter() {
            if request.verify(key).is_ok() {
                tracing::debug!(
                    pubkey = key.to_string(),
                    key_type = key_type.to_string(),
                    "request authorized"
                );
                return Ok(());
            }
        }
        Err(anyhow!("unauthorized request"))
    }

    pub async fn insert_key(&self, key_type: KeyType, key: PublicKey) -> anyhow::Result<()> {
        db::insert_key(key.clone().into(), key_type, &self.pool).await?;

        match key_type {
            KeyType::Administrator => self.admin_keys.write(),
            KeyType::Oracle => self.oracle_keys.write(),
            KeyType::PacketRouter => self.router_keys.write(),
        }
        .await
        .insert(key);

        Ok(())
    }

    pub async fn remove_key(&self, key: &PublicKey) -> anyhow::Result<()> {
        if let Some((pubkey, key_type)) = db::remove_key(key.clone().into(), &self.pool).await? {
            match key_type {
                KeyType::Administrator => self.admin_keys.write(),
                KeyType::Oracle => self.oracle_keys.write(),
                KeyType::PacketRouter => self.router_keys.write(),
            }
            .await
            .remove(&pubkey);
        };

        Ok(())
    }

    pub async fn has_key(&self, key: &PublicKey) -> bool {
        match self.router_keys.read().await.contains(key) {
            false => match self.admin_keys.read().await.contains(key) {
                false => self.oracle_keys.read().await.contains(key),
                true => true,
            },
            true => true,
        }
    }

    pub async fn list_keys(&self, key_type: KeyType) -> Vec<PublicKey> {
        match key_type {
            KeyType::Administrator => self.admin_keys.read(),
            KeyType::Oracle => self.oracle_keys.read(),
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
    Oracle,
    PacketRouter,
}

impl KeyType {
    pub fn from_i32(v: i32) -> anyhow::Result<Self> {
        ProtoKeyType::from_i32(v)
            .map(|kt| kt.into())
            .ok_or(anyhow!("unsupported key type {}", v))
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
            KeyType::Oracle => ProtoKeyType::Oracle,
            KeyType::PacketRouter => ProtoKeyType::PacketRouter,
        }
    }
}

impl From<ProtoKeyType> for KeyType {
    fn from(kt: ProtoKeyType) -> Self {
        match kt {
            ProtoKeyType::Administrator => KeyType::Administrator,
            ProtoKeyType::Oracle => KeyType::Oracle,
            ProtoKeyType::PacketRouter => KeyType::PacketRouter,
        }
    }
}

impl std::fmt::Display for KeyType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let s = match self {
            Self::Administrator => "administrator",
            Self::Oracle => "oracle",
            Self::PacketRouter => "packet_router",
        };
        f.write_str(s)
    }
}

pub(super) mod db {
    use super::{KeyType, PublicKey, PublicKeyBinary};
    use sqlx::Row;

    pub(super) async fn fetch_stored_keys(
        db: impl sqlx::PgExecutor<'_>,
    ) -> anyhow::Result<Vec<(KeyType, PublicKey)>> {
        Ok(
            sqlx::query(r#" select pubkey, key_type from registered_keys "#)
                .fetch_all(db)
                .await?
                .into_iter()
                .map(|row| (row.get("key_type"), row.get::<PublicKey, &str>("pubkey")))
                .collect(),
        )
    }

    pub(super) async fn insert_key(
        pubkey: PublicKeyBinary,
        key_type: KeyType,
        db: impl sqlx::PgExecutor<'_>,
    ) -> anyhow::Result<()> {
        Ok(
            sqlx::query(r#" insert into registered_keys (pubkey, key_type) values ($1, $2) "#)
                .bind(pubkey)
                .bind(key_type)
                .execute(db)
                .await
                .map(|_| ())?,
        )
    }

    pub(super) async fn remove_key(
        pubkey: PublicKeyBinary,
        db: impl sqlx::PgExecutor<'_>,
    ) -> anyhow::Result<Option<(PublicKey, KeyType)>> {
        Ok(sqlx::query(
            r#"
            delete from registered_keys
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
        }))
    }
}
