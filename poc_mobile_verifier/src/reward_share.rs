use crate::Result;
use helium_crypto::PublicKey;
use helium_proto::services::{
    follower::{self, follower_gateway_resp_v1::Result as GatewayResult, FollowerGatewayReqV1},
    Channel,
};
use serde::Serialize;
use std::collections::HashMap;
use rust_decimal::Decimal;
use rust_decimal_macros::dec;

/// Map from gw_public_key to accumulated reward shares
pub type HotspotShares = HashMap<PublicKey, Decimal>;

/// Map from owner_public_key to accumulated reward shares
pub type OwnerShares = HashMap<PublicKey, Decimal>;

/// Map from gw_public_key (without owners) to accumulated_reward_weight (decimal)
pub type MissingOwnerShares = HashMap<PublicKey, Decimal>;

/// Map from owner_public_key to accumulated_rewards (mobile)
#[derive(Debug, Clone, Serialize)]
pub struct OwnerEmissions {
    emissions: HashMap<PublicKey, Decimal>,
}

impl OwnerEmissions {
    pub fn new(owner_shares: OwnerShares) -> Self {
        let mut emissions = HashMap::new();
        let total_shares: Decimal = owner_shares.values().sum();
        if total_shares > dec!(0.0) {
            for (owner, share) in owner_shares {
                emissions.insert(owner, share / total_shares);
            }
        }
        OwnerEmissions { emissions }
    }

    #[allow(dead_code)]
    pub fn total_emissions(&self) -> Decimal {
        self.emissions.values().sum()
    }

    pub fn into_iter(self) -> impl Iterator<Item = (PublicKey, Decimal)> {
        self.emissions.into_iter()
    }
}

#[async_trait::async_trait]
pub trait OwnerResolver: Send {
    async fn resolve_owner(&mut self, address: &PublicKey) -> Result<Option<PublicKey>>;

    async fn owner_shares(
        &mut self,
        hotspot_shares: HotspotShares,
    ) -> Result<(OwnerShares, MissingOwnerShares)> {
        let mut owner_shares = OwnerShares::new();
        let mut missing_owner_shares = MissingOwnerShares::new();
        for (hotspot, share) in hotspot_shares {
            if let Some(owner) = self.resolve_owner(&hotspot).await? {
                *owner_shares.entry(owner).or_default() += share;
            } else {
                *missing_owner_shares.entry(hotspot).or_default() += share;
            }
        }
        Ok((owner_shares, missing_owner_shares))
    }
}

#[async_trait::async_trait]
impl OwnerResolver for follower::Client<Channel> {
    async fn resolve_owner(&mut self, address: &PublicKey) -> Result<Option<PublicKey>> {
        let req = FollowerGatewayReqV1 {
            address: address.to_vec(),
        };
        let res = self.find_gateway(req).await?.into_inner();

        if let Some(GatewayResult::Info(gateway_info)) = res.result {
            if let Ok(pub_key) = PublicKey::try_from(gateway_info.owner) {
                return Ok(Some(pub_key));
            }
        }

        Ok(None)
    }
}

#[cfg(test)]
mod test {
    use crate::cell_type::CellType;
    use async_trait::async_trait;
    use helium_crypto::PublicKey;
    use std::str::FromStr;
    use rust_decimal_macros::dec;

    use super::*;

    struct FixedOwnerResolver {
        owner: PublicKey,
    }

    #[async_trait]
    impl OwnerResolver for FixedOwnerResolver {
        async fn resolve_owner(&mut self, _address: &PublicKey) -> Result<Option<PublicKey>> {
            Ok(Some(self.owner.clone()))
        }
    }

    #[tokio::test]
    async fn test_rewards() {
        // SercommIndoor
        let g1 = PublicKey::from_str("11eX55faMbqZB7jzN4p67m6w7ScPMH6ubnvCjCPLh72J49PaJEL")
            .expect("unable to construct pubkey");
        // Nova430I
        let g2 = PublicKey::from_str("118SPA16MX8WrUKcuXxsg6SH8u5dWszAySiUAJX6tTVoQVy7nWc")
            .expect("unable to construct pubkey");
        // SercommOutdoor
        let g3 = PublicKey::from_str("112qDCKek7fePg6wTpEnbLp3uD7TTn8MBH7PGKtmAaUcG1vKQ9eZ")
            .expect("unable to construct pubkey");
        // Nova436H
        let g4 = PublicKey::from_str("11k712d9dSb8CAujzS4PdC7Hi8EEBZWsSnt4Zr1hgke4e1Efiag")
            .expect("unable to construct pubkey");

        let c1 = "P27-SCE4255W2107CW5000014".to_string();
        let c2 = "2AG32PBS3101S1202000464223GY0153".to_string();
        let c3 = "P27-SCO4255PA102206DPT000207".to_string();
        let c4 = "2AG32MBS3100196N1202000240215KY0184".to_string();

        let ct1 = CellType::from_cbsd_id(&c1).expect("unable to get cell_type");
        let ct2 = CellType::from_cbsd_id(&c2).expect("unable to get cell_type");
        let ct3 = CellType::from_cbsd_id(&c3).expect("unable to get cell_type");
        let ct4 = CellType::from_cbsd_id(&c4).expect("unable to get cell_type");

        let mut shares = HotspotShares::new();
        shares.insert(g1, ct1.reward_weight());
        shares.insert(g2, ct2.reward_weight());
        shares.insert(g3, ct3.reward_weight());
        shares.insert(g4, ct4.reward_weight());

        let test_owner = PublicKey::from_str("1ay5TAKuQDjLS6VTpoWU51p3ik3Sif1b3DWRstErqkXFJ4zuG7r")
            .expect("unable to get test pubkey");
        let mut owner_resolver = FixedOwnerResolver { owner: test_owner };

        let (owner_shares, _missing_owner_shares) = owner_resolver
            .owner_shares(shares)
            .await
            .expect("unable to get owner_shares");

        let owner_emissions = OwnerEmissions::new(owner_shares);
        let total_owner_emissions = owner_emissions.total_emissions();

        assert_eq!(dec!(1.0), total_owner_emissions);
    }
}
