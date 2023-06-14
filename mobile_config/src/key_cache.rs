use crate::settings::Settings;
use anyhow::anyhow;
use file_store::traits::MsgVerify;
use helium_crypto::{PublicKey, PublicKeyBinary};
use helium_proto::services::mobile_config::AdminKeyRole as ProtoKeyRole;
use serde::Serialize;
use std::collections::HashSet;
use tokio::sync::watch;

pub type CacheKeys = HashSet<(PublicKey, KeyRole)>;

#[derive(Clone, Debug)]
pub struct KeyCache {
    cache_receiver: watch::Receiver<CacheKeys>,
}

impl KeyCache {
    pub async fn new(
        settings: &Settings,
        db: impl sqlx::PgExecutor<'_> + Copy,
    ) -> anyhow::Result<(watch::Sender<CacheKeys>, Self)> {
        let config_admin = settings.admin_pubkey()?;

        let mut stored_keys = db::fetch_stored_keys(db).await?;
        stored_keys.insert((config_admin, KeyRole::Administrator));

        let (cache_sender, cache_receiver) = watch::channel(stored_keys);

        Ok((cache_sender, Self { cache_receiver }))
    }

    pub fn verify_signature<R>(&self, signer: &PublicKey, request: &R) -> anyhow::Result<()>
    where
        R: MsgVerify,
    {
        let cached_keys = self.cache_receiver.borrow();
        if (cached_keys.contains(&(signer.clone(), KeyRole::Administrator)) || cached_keys.contains(&(signer.clone(), KeyRole::Oracle))) && request.verify(signer).is_ok() {
            tracing::debug!(pubkey = signer.to_string(), "request authorized");
            Ok(())
        } else {
            Err(anyhow!("unauthorized request"))
        }
    }

    pub fn verify_signature_with_role<R>(
        &self,
        key_role: KeyRole,
        signer: &PublicKey,
        request: &R,
    ) -> anyhow::Result<()>
    where
        R: MsgVerify,
    {
        let cached_signer = signer.clone();
        if self.cache_receiver.borrow().contains(&(cached_signer, key_role)) && request.verify(signer).is_ok()
        {
            tracing::debug!(pubkey = signer.to_string(), "request authorized");
            Ok(())
        } else {
            Err(anyhow!("unauthorized request"))
        }
    }

    pub fn get_keys(&self) -> Vec<(PublicKey, KeyRole)> {
        self.cache_receiver
            .borrow()
            .iter()
            .map(|(k, t)| (k.clone(), *t))
            .collect()
    }

    pub fn get_keys_by_role(&self, key_role: KeyRole) -> Vec<PublicKey> {
        self.cache_receiver
            .borrow()
            .iter()
            .filter_map(|(k, t)| {
                if t == &key_role {
                    Some(k.clone())
                } else {
                    None
                }
            })
            .collect()
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq, Serialize, sqlx::Type)]
#[sqlx(type_name = "key_role", rename_all = "snake_case")]
pub enum KeyRole {
    Administrator,
    Carrier,
    Oracle,
    Router,
}

impl KeyRole {
    pub fn from_i32(v: i32) -> anyhow::Result<Self> {
        ProtoKeyRole::from_i32(v)
            .map(|kr| kr.into())
            .ok_or_else(|| anyhow!("unsupported key role {}", v))
    }
}

impl From<KeyRole> for ProtoKeyRole {
    fn from(key_role: KeyRole) -> Self {
        ProtoKeyRole::from(&key_role)
    }
}

impl From<&KeyRole> for ProtoKeyRole {
    fn from(skr: &KeyRole) -> Self {
        match skr {
            KeyRole::Administrator => ProtoKeyRole::Administrator,
            KeyRole::Carrier => ProtoKeyRole::Carrier,
            KeyRole::Oracle => ProtoKeyRole::Oracle,
            KeyRole::Router => ProtoKeyRole::Router,
        }
    }
}

impl From<ProtoKeyRole> for KeyRole {
    fn from(kt: ProtoKeyRole) -> Self {
        match kt {
            ProtoKeyRole::Administrator => KeyRole::Administrator,
            ProtoKeyRole::Carrier => KeyRole::Carrier,
            ProtoKeyRole::Oracle => KeyRole::Oracle,
            ProtoKeyRole::Router => KeyRole::Router,
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
        };
        f.write_str(s)
    }
}

pub(crate) mod db {
    use super::{CacheKeys, KeyRole, PublicKey, PublicKeyBinary};
    use sqlx::Row;

    pub async fn fetch_stored_keys(db: impl sqlx::PgExecutor<'_>) -> anyhow::Result<CacheKeys> {
        Ok(
            sqlx::query(r#" select pubkey, key_role from registered_keys "#)
                .fetch_all(db)
                .await?
                .into_iter()
                .map(|row| (row.get::<PublicKey, &str>("pubkey"), row.get("key_role")))
                .collect(),
        )
    }

    pub async fn insert_key(
        pubkey: PublicKeyBinary,
        key_role: KeyRole,
        db: impl sqlx::PgExecutor<'_>,
    ) -> anyhow::Result<()> {
        Ok(
            sqlx::query(r#" insert into registered_keys (pubkey, key_role) values ($1, $2) "#)
                .bind(pubkey)
                .bind(key_role)
                .execute(db)
                .await
                .map(|_| ())?,
        )
    }

    pub async fn remove_key(
        pubkey: PublicKeyBinary,
        key_role: KeyRole,
        db: impl sqlx::PgExecutor<'_>,
    ) -> anyhow::Result<Option<(PublicKey, KeyRole)>> {
        Ok(sqlx::query(
            r#"
            delete from registered_keys
            where pubkey = $1 and key_role = $2
            returning (pubkey, key_role)
            "#,
        )
        .bind(pubkey)
        .bind(key_role)
        .fetch_optional(db)
        .await?
        .map(|row| {
            (
                row.get::<PublicKey, &str>("pubkey"),
                row.get::<KeyRole, &str>("key_role"),
            )
        }))
    }
}
