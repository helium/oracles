use crate::Result;
use async_trait::async_trait;
use helium_crypto::PublicKey;

#[async_trait]
pub trait OwnerResolver {
    async fn resolve_owner(&mut self, address: &PublicKey) -> Result<Option<PublicKey>>;
}
