use anyhow::anyhow;
use file_store::traits::MsgVerify;
use helium_crypto::{PublicKey, PublicKeyBinary};
use helium_proto::services::iot_config::admin_add_key_req_v1::KeyTypeV1 as ProtoKeyType;
use serde::Serialize;
use sqlx::Row;
use std::collections::HashMap;
use tokio::sync::watch;

pub type CacheKeys = HashMap<PublicKey, KeyType>;

#[derive(Clone, Debug)]
pub struct AuthCache {
    cache_receiver: watch::Receiver<CacheKeys>,
}

impl AuthCache {
    pub async fn new(
        config_admin_key: PublicKey,
        db: impl sqlx::PgExecutor<'_> + Copy,
    ) -> anyhow::Result<(watch::Sender<CacheKeys>, Self)> {
        let mut stored_keys = fetch_stored_keys(db)
            .await?
            .into_iter()
            .collect::<CacheKeys>();
        stored_keys.insert(config_admin_key, KeyType::Administrator);

        let (cache_sender, cache_receiver) = watch::channel(stored_keys);

        Ok((cache_sender, Self { cache_receiver }))
    }

    pub fn verify_signature<R>(&self, signer: &PublicKey, request: &R) -> anyhow::Result<()>
    where
        R: MsgVerify,
    {
        if self.cache_receiver.borrow().contains_key(signer) && request.verify(signer).is_ok() {
            tracing::debug!(pubkey = signer.to_string(), "request authorized");
            Ok(())
        } else {
            Err(anyhow!("unauthorized request"))
        }
    }

    pub fn verify_signature_with_type<R>(
        &self,
        key_type: KeyType,
        signer: &PublicKey,
        request: &R,
    ) -> anyhow::Result<()>
    where
        R: MsgVerify,
    {
        if self.cache_receiver.borrow().get_key_value(signer) == Some((signer, &key_type))
            && request.verify(signer).is_ok()
        {
            tracing::debug!(pubkey = signer.to_string(), "request authorized");
            Ok(())
        } else {
            Err(anyhow!("unauthorized request"))
        }
    }

    pub fn get_keys(&self) -> Vec<(PublicKey, KeyType)> {
        self.cache_receiver
            .borrow()
            .iter()
            .map(|(k, t)| (k.clone(), *t))
            .collect()
    }

    pub fn get_keys_by_type(&self, key_type: KeyType) -> Vec<PublicKey> {
        self.cache_receiver
            .borrow()
            .iter()
            .filter_map(|(k, t)| {
                if t == &key_type {
                    Some(k.clone())
                } else {
                    None
                }
            })
            .collect()
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq, Serialize, sqlx::Type)]
#[sqlx(type_name = "key_type", rename_all = "snake_case")]
pub enum KeyType {
    Administrator,
    PacketRouter,
    Oracle,
}

impl KeyType {
    pub fn from_i32(v: i32) -> anyhow::Result<Self> {
        ProtoKeyType::try_from(v)
            .map(|kt| kt.into())
            .map_err(|_| anyhow!("unsupported key type {}", v))
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
            KeyType::Oracle => ProtoKeyType::Oracle,
        }
    }
}

impl From<ProtoKeyType> for KeyType {
    fn from(kt: ProtoKeyType) -> Self {
        match kt {
            ProtoKeyType::Administrator => KeyType::Administrator,
            ProtoKeyType::PacketRouter => KeyType::PacketRouter,
            ProtoKeyType::Oracle => KeyType::Oracle,
        }
    }
}

impl std::fmt::Display for KeyType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let s = match self {
            Self::Administrator => "administrator",
            Self::PacketRouter => "packet_router",
            Self::Oracle => "oracle",
        };
        f.write_str(s)
    }
}

pub async fn fetch_stored_keys(
    db: impl sqlx::PgExecutor<'_>,
) -> anyhow::Result<Vec<(PublicKey, KeyType)>> {
    Ok(sqlx::query(r#" select pubkey, key_type from admin_keys "#)
        .fetch_all(db)
        .await?
        .into_iter()
        .map(|row| (row.get::<PublicKey, &str>("pubkey"), row.get("key_type")))
        .collect())
}

pub async fn insert_key(
    pubkey: PublicKeyBinary,
    key_type: KeyType,
    db: impl sqlx::PgExecutor<'_>,
) -> anyhow::Result<()> {
    Ok(
        sqlx::query(r#" insert into admin_keys (pubkey, key_type) values ($1, $2) "#)
            .bind(pubkey)
            .bind(key_type)
            .execute(db)
            .await
            .map(|_| ())?,
    )
}

pub async fn remove_key(
    pubkey: PublicKeyBinary,
    db: impl sqlx::PgExecutor<'_>,
) -> anyhow::Result<Option<(PublicKey, KeyType)>> {
    Ok(sqlx::query(
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
    }))
}
