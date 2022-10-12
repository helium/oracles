use crate::{
    error::Result,
    heartbeats::{HeartbeatValue, Heartbeats},
    reward_share::{OwnerEmissions, OwnerResolver},
};
use chrono::{DateTime, Utc};
use file_store::file_sink;
use helium_crypto::PublicKey;
use helium_proto::services::{follower, Channel};
use rust_decimal::Decimal;
use std::collections::HashMap;
use std::ops::Range;

mod proto {
    pub use helium_proto::services::poc_mobile::*;
    pub use helium_proto::{SubnetworkReward, SubnetworkRewards};
}

pub struct SubnetworkRewards {
    pub epoch: Range<DateTime<Utc>>,
    pub rewards: Vec<proto::SubnetworkReward>,
}

impl SubnetworkRewards {
    pub async fn from_epoch(
        mut follower_service: follower::Client<Channel>,
        epoch: &Range<DateTime<Utc>>,
        heartbeats: &Heartbeats,
    ) -> Result<Self> {
        // Gather hotspot shares
        let mut hotspot_shares = HashMap::<PublicKey, Decimal>::new();
        for (pub_key, HeartbeatValue { weight, .. }) in &heartbeats.heartbeats {
            *hotspot_shares.entry(pub_key.clone()).or_default() += weight;
        }

        let (owner_shares, _missing_owner_shares) =
            follower_service.owner_shares(hotspot_shares).await?;

        let owner_emissions =
            OwnerEmissions::new(owner_shares, epoch.start, epoch.end - epoch.start);

        let mut rewards = owner_emissions
            .into_inner()
            .into_iter()
            .map(|(owner, amt)| proto::SubnetworkReward {
                account: owner.to_vec(),
                amount: u64::from(amt),
            })
            .collect::<Vec<_>>();

        rewards.sort_by(|a, b| {
            a.account
                .cmp(&b.account)
                .then_with(|| a.amount.cmp(&b.amount))
        });

        Ok(Self {
            epoch: epoch.clone(),
            rewards,
        })
    }

    pub async fn write(self, subnet_rewards_tx: &file_sink::MessageSender) -> Result {
        file_sink::write(
            subnet_rewards_tx,
            proto::SubnetworkRewards {
                start_epoch: self.epoch.start.timestamp() as u64,
                end_epoch: self.epoch.end.timestamp() as u64,
                rewards: self.rewards,
            },
        )
        .await?;

        Ok(())
    }
}

#[cfg(test)]
mod test {
    use super::proto::ShareValidity;
    use crate::{
        cell_type::CellType,
        reward_share::{OwnerEmissions, OwnerResolver, Share, Shares},
        reward_speed_share::{SpeedShare, SpeedShareMovingAvgs, SpeedShares},
    };
    use async_trait::async_trait;
    use chrono::TimeZone;
    use helium_crypto::PublicKey;
    use std::str::FromStr;

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
    async fn check_rewards() {
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

        let t1 = Utc.timestamp_millis(100);
        let t2 = Utc.timestamp_millis(100);
        let t3 = Utc.timestamp_millis(100);
        let t4 = Utc.timestamp_millis(100);

        let ct1 = CellType::from_cbsd_id(&c1).expect("unable to get cell_type");
        let ct2 = CellType::from_cbsd_id(&c2).expect("unable to get cell_type");
        let ct3 = CellType::from_cbsd_id(&c3).expect("unable to get cell_type");
        let ct4 = CellType::from_cbsd_id(&c4).expect("unable to get cell_type");

        let mut shares = Shares::new();
        shares.insert(
            c1,
            Share::new(
                t1,
                g1.clone(),
                ct1.reward_weight(),
                ct1,
                ShareValidity::Valid,
            ),
        );
        shares.insert(
            c2,
            Share::new(
                t2,
                g2.clone(),
                ct2.reward_weight(),
                ct2,
                ShareValidity::Valid,
            ),
        );
        shares.insert(
            c3,
            Share::new(
                t3,
                g3.clone(),
                ct3.reward_weight(),
                ct3,
                ShareValidity::Valid,
            ),
        );
        shares.insert(
            c4,
            Share::new(
                t4,
                g4.clone(),
                ct4.reward_weight(),
                ct4,
                ShareValidity::Valid,
            ),
        );

        // All g1 averages are satifsied
        let s1 = vec![
            SpeedShare::new(
                g1.clone(),
                Utc.timestamp_millis(1661578086),
                2182223,
                11739568,
                118,
                ShareValidity::Valid,
            ),
            SpeedShare::new(
                g1.clone(),
                Utc.timestamp_millis(1661581686),
                2589229,
                12618734,
                30,
                ShareValidity::Valid,
            ),
            SpeedShare::new(
                g1.clone(),
                Utc.timestamp_millis(1661585286),
                11420942,
                11376519,
                8,
                ShareValidity::Valid,
            ),
            SpeedShare::new(
                g1.clone(),
                Utc.timestamp_millis(1661588886),
                7646683,
                35517840,
                6,
                ShareValidity::Valid,
            ),
            SpeedShare::new(
                g1.clone(),
                Utc.timestamp_millis(1661588886),
                7646683,
                35517840,
                6,
                ShareValidity::Valid,
            ),
            SpeedShare::new(
                g1.clone(),
                Utc.timestamp_millis(1661588886),
                8646683,
                35517840,
                6,
                ShareValidity::Valid,
            ),
        ];
        // The avg latency for g2 is too high, should not appear in hotspot_shares
        let s2 = vec![
            SpeedShare::new(
                g2.clone(),
                Utc.timestamp_millis(1661578086),
                2182223,
                11739568,
                118,
                ShareValidity::Valid,
            ),
            SpeedShare::new(
                g2.clone(),
                Utc.timestamp_millis(1661581686),
                2589229,
                12618734,
                30,
                ShareValidity::Valid,
            ),
            SpeedShare::new(
                g2.clone(),
                Utc.timestamp_millis(1661585286),
                11420942,
                11376519,
                40,
                ShareValidity::Valid,
            ),
            SpeedShare::new(
                g2.clone(),
                Utc.timestamp_millis(1661588886),
                7646683,
                35517840,
                60,
                ShareValidity::Valid,
            ),
            SpeedShare::new(
                g2.clone(),
                Utc.timestamp_millis(1661588886),
                7646683,
                35517840,
                55,
                ShareValidity::Valid,
            ),
            SpeedShare::new(
                g2.clone(),
                Utc.timestamp_millis(1661588886),
                8646683,
                35517840,
                58,
                ShareValidity::Valid,
            ),
        ];
        // The avg upload speed for g3 is too low, should not appear in hotspot_shares
        let s3 = vec![
            SpeedShare::new(
                g1.clone(),
                Utc.timestamp_millis(1661578086),
                182223,
                11739568,
                118,
                ShareValidity::Valid,
            ),
            SpeedShare::new(
                g1.clone(),
                Utc.timestamp_millis(1661581686),
                589229,
                12618734,
                30,
                ShareValidity::Valid,
            ),
            SpeedShare::new(
                g1.clone(),
                Utc.timestamp_millis(1661585286),
                1420942,
                11376519,
                8,
                ShareValidity::Valid,
            ),
            SpeedShare::new(
                g1.clone(),
                Utc.timestamp_millis(1661588886),
                646683,
                35517840,
                6,
                ShareValidity::Valid,
            ),
            SpeedShare::new(
                g1.clone(),
                Utc.timestamp_millis(1661588886),
                646683,
                35517840,
                6,
                ShareValidity::Valid,
            ),
            SpeedShare::new(
                g1.clone(),
                Utc.timestamp_millis(1661588886),
                646683,
                35517840,
                6,
                ShareValidity::Valid,
            ),
        ];
        // The avg download speed for g4 is too low, should not appear in hotspot_shares
        let s4 = vec![
            SpeedShare::new(
                g4.clone(),
                Utc.timestamp_millis(1661578086),
                2182223,
                1739568,
                118,
                ShareValidity::Valid,
            ),
            SpeedShare::new(
                g4.clone(),
                Utc.timestamp_millis(1661581686),
                2589229,
                2618734,
                30,
                ShareValidity::Valid,
            ),
            SpeedShare::new(
                g4.clone(),
                Utc.timestamp_millis(1661585286),
                11420942,
                1376519,
                8,
                ShareValidity::Valid,
            ),
            SpeedShare::new(
                g4.clone(),
                Utc.timestamp_millis(1661588886),
                7646683,
                5517840,
                6,
                ShareValidity::Valid,
            ),
            SpeedShare::new(
                g4.clone(),
                Utc.timestamp_millis(1661588886),
                7646683,
                5517840,
                6,
                ShareValidity::Valid,
            ),
            SpeedShare::new(
                g4.clone(),
                Utc.timestamp_millis(1661588886),
                8646683,
                5517840,
                6,
                ShareValidity::Valid,
            ),
        ];

        let mut speed_shares = SpeedShares::new();
        let mut moving_avgs = SpeedShareMovingAvgs::default();
        speed_shares.insert(g1.clone(), s1);
        speed_shares.insert(g2, s2);
        speed_shares.insert(g3, s3);
        speed_shares.insert(g4, s4);
        moving_avgs.update(&speed_shares);

        let hotspot_shares = hotspot_shares(&shares, &moving_avgs);
        // TODO: re-add this later
        /*
        // Only g1 should be in hotspot_shares
        assert!(hotspot_shares.contains_key(&g1) && hotspot_shares.keys().len() == 1);
        */

        let test_owner = PublicKey::from_str("1ay5TAKuQDjLS6VTpoWU51p3ik3Sif1b3DWRstErqkXFJ4zuG7r")
            .expect("unable to get test pubkey");
        let mut owner_resolver = FixedOwnerResolver { owner: test_owner };

        let (owner_shares, _missing_owner_shares) = owner_resolver
            .owner_shares(hotspot_shares)
            .await
            .expect("unable to get owner_shares");

        let start = Utc::now();
        let duration = chrono::Duration::hours(24);
        let owner_emissions = OwnerEmissions::new(owner_shares, start, duration);
        let total_owner_emissions = owner_emissions.total_emissions();

        // 100M in bones
        assert_eq!(10000000000000000, u64::from(total_owner_emissions));

        /*

            let keypair_b64 = "EeNwbGXheUq4frT05EJwMtvGuz8zHyajOaN2h5yz5M9A58pZdf9bLayp8Ex6x0BkGxREleQnTNwOTyT2vPL0i1_nyll1_1strKnwTHrHQGQbFESV5CdM3A5PJPa88vSLXw";
            let kp = Keypair::try_from(
                Vec::from_b64_url(keypair_b64)
                    .expect("unable to get raw keypair")
                    .as_ref(),
            )
            .expect("unable to get keypair");
            let (_txn, txn_hash_str) =
                construct_txn(&kp, subnetwork_rewards, 1000, 1010).expect("unable to construct txn");

            // This is taken from a blockchain-node, constructing the exact same txn
            assert_eq!(
                "hpnFwsGQQohIlAYGyaWJ-j3Hs8kIPeskh09qEkoL3I4".to_string(),
                txn_hash_str
        );

            */
    }
}
