use crate::{PublicKey, Result};
use async_trait::async_trait;

#[async_trait]
pub trait OwnerResolver {
    async fn resolve_owner(&mut self, address: &PublicKey) -> Result<Option<PublicKey>>;
}
