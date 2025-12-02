use crate::{settings::Settings, KeyRole};
use anyhow::anyhow;
use helium_crypto::{PublicKey, PublicKeyBinary};
use helium_proto_crypto::MsgVerify;
use std::collections::HashSet;
use tokio::sync::watch;

pub type CacheKeys = HashSet<(PublicKey, KeyRole)>;

#[derive(Clone, Debug)]
pub struct KeyCache {
    cache_receiver: watch::Receiver<CacheKeys>,
}

impl KeyCache {
    pub fn new(stored_keys: CacheKeys) -> (watch::Sender<CacheKeys>, Self) {
        let (cache_sender, cache_receiver) = watch::channel(stored_keys);

        (cache_sender, Self { cache_receiver })
    }

    pub async fn from_settings(
        settings: &Settings,
        db: impl sqlx::PgExecutor<'_> + Copy,
    ) -> anyhow::Result<(watch::Sender<CacheKeys>, Self)> {
        let config_admin = settings.admin_pubkey.clone();

        let mut stored_keys = db::fetch_stored_keys(db).await?;
        stored_keys.insert((config_admin, KeyRole::Administrator));

        Ok(Self::new(stored_keys))
    }

    pub fn verify_signature<R>(&self, signer: &PublicKey, request: &R) -> anyhow::Result<()>
    where
        R: MsgVerify,
    {
        let cached_keys = self.cache_receiver.borrow();
        if (cached_keys.contains(&(signer.clone(), KeyRole::Administrator))
            || cached_keys.contains(&(signer.clone(), KeyRole::Oracle))
            || cached_keys.contains(&(signer.clone(), KeyRole::Carrier)))
            && request.verify(signer).is_ok()
        {
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
        if self
            .cache_receiver
            .borrow()
            .contains(&(cached_signer, key_role))
            && request.verify(signer).is_ok()
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

    pub fn verify_key_by_role(&self, pubkey: &PublicKey, key_role: KeyRole) -> bool {
        self.cache_receiver
            .borrow()
            .contains(&(pubkey.clone(), key_role))
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

#[cfg(test)]
mod tests {
    use helium_crypto::{KeyTag, Keypair};
    use helium_proto_crypto::MsgVerifyError;
    use rand::rngs::OsRng;

    use super::*;

    struct VerifiedGood;

    impl MsgVerify for VerifiedGood {
        fn verify(&self, _verifier: &PublicKey) -> Result<(), MsgVerifyError> {
            Ok(())
        }
    }

    struct VerifiedBad;

    impl MsgVerify for VerifiedBad {
        fn verify(&self, _verifier: &PublicKey) -> Result<(), MsgVerifyError> {
            Err(helium_crypto::Error::InvalidNetwork.into())
        }
    }

    #[test]
    fn verify_signature_when_key_is_not_in_any_role() {
        let keypair = generate_keypair();

        let (_, key_cache) = KeyCache::new(HashSet::new());

        let result = key_cache.verify_signature(keypair.public_key(), &VerifiedGood);

        assert!(result.is_err());
    }

    #[test]
    fn verify_signature_for_administrator() {
        let keypair = generate_keypair();

        let mut keys = HashSet::new();
        keys.insert((keypair.public_key().to_owned(), KeyRole::Administrator));

        let (_, key_cache) = KeyCache::new(keys);

        let result = key_cache.verify_signature(keypair.public_key(), &VerifiedGood);
        assert!(result.is_ok());

        let result = key_cache.verify_signature(keypair.public_key(), &VerifiedBad);
        assert!(result.is_err());
    }

    #[test]
    fn verify_signature_for_oracle() {
        let keypair = generate_keypair();

        let mut keys = HashSet::new();
        keys.insert((keypair.public_key().to_owned(), KeyRole::Oracle));

        let (_, key_cache) = KeyCache::new(keys);

        let result = key_cache.verify_signature(keypair.public_key(), &VerifiedGood);
        assert!(result.is_ok());

        let result = key_cache.verify_signature(keypair.public_key(), &VerifiedBad);
        assert!(result.is_err());
    }

    #[test]
    fn verify_signature_for_carrier() {
        let keypair = generate_keypair();

        let mut keys = HashSet::new();
        keys.insert((keypair.public_key().to_owned(), KeyRole::Carrier));

        let (_, key_cache) = KeyCache::new(keys);

        let result = key_cache.verify_signature(keypair.public_key(), &VerifiedGood);
        assert!(result.is_ok());

        let result = key_cache.verify_signature(keypair.public_key(), &VerifiedBad);
        assert!(result.is_err());
    }

    fn generate_keypair() -> Keypair {
        Keypair::generate(KeyTag::default(), &mut OsRng)
    }
}
