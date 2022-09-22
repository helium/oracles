use crate::{
    datetime_from_epoch,
    follower::FollowerService,
    reward_share::{cell_shares, gather_shares, hotspot_shares, owner_shares, OwnerEmissions},
    subnetwork_reward::sorted_rewards,
    token_type::BlockchainTokenTypeV1,
    traits::{TxnHash, TxnSign, B64},
    write_json, Error, Keypair, Result,
};
use chrono::{DateTime, Duration, Utc};
use futures::stream::{self, StreamExt};
use helium_proto::{
    BlockchainTokenTypeV1 as ProtoTokenType, BlockchainTxnSubnetworkRewardsV1,
    SubnetworkReward as ProtoSubnetworkReward,
};
use poc_store::{FileStore, FileType};
use serde::Serialize;
use serde_json::json;
use std::cmp::min;

// default minutes to delay lookup from now
pub const DEFAULT_LOOKUP_DELAY: i64 = 30;

#[derive(Debug)]
pub struct RewardPeriod {
    pub start: u64,
    pub end: u64,
}

impl RewardPeriod {
    pub fn new(start: u64, end: u64) -> Self {
        Self { start, end }
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct SubnetworkRewards(pub Vec<ProtoSubnetworkReward>);

impl SubnetworkRewards {
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    pub async fn from_period(
        store: FileStore,
        follower_service: &mut FollowerService,
        after_utc: DateTime<Utc>,
        before_utc: DateTime<Utc>,
    ) -> Result<Option<Self>> {
        if let Some(rewards) = get_rewards(store, follower_service, after_utc, before_utc).await? {
            return Ok(Some(Self(rewards)));
        }
        Ok(None)
    }

    pub async fn from_last_reward_end_time(
        store: FileStore,
        follower_service: &mut FollowerService,
        last_reward_end_time: i64,
    ) -> Result<Option<Self>> {
        let (after_utc, before_utc) = get_time_range(last_reward_end_time);
        if let Some(rewards) = get_rewards(store, follower_service, after_utc, before_utc).await? {
            return Ok(Some(Self(rewards)));
        }
        Ok(None)
    }
}

impl From<SubnetworkRewards> for Vec<ProtoSubnetworkReward> {
    fn from(subnetwork_rewards: SubnetworkRewards) -> Self {
        sorted_rewards(subnetwork_rewards.0)
    }
}

pub fn construct_txn(
    keypair: &Keypair,
    rewards: SubnetworkRewards,
    period: RewardPeriod,
) -> Result<(BlockchainTxnSubnetworkRewardsV1, String)> {
    let mut txn = BlockchainTxnSubnetworkRewardsV1 {
        rewards: rewards.into(),
        token_type: BlockchainTokenTypeV1::from(ProtoTokenType::Mobile).into(),
        start_epoch: period.start,
        end_epoch: period.end,
        reward_server_signature: vec![],
    };
    txn.reward_server_signature = txn.sign(keypair)?;

    let txn_hash = txn.hash()?;
    let txn_hash_str = txn_hash.to_b64_url()?;
    Ok((txn, txn_hash_str))
}

async fn get_rewards(
    store: FileStore,
    follower_service: &mut FollowerService,
    after_utc: DateTime<Utc>,
    before_utc: DateTime<Utc>,
) -> Result<Option<Vec<ProtoSubnetworkReward>>> {
    if before_utc <= after_utc {
        tracing::error!(
            "cannot reward future period, before: {:?}, after: {:?}",
            before_utc,
            after_utc
        );
        return Err(Error::NotFound("cannot reward future".to_string()));
    }
    let after_ts = after_utc.timestamp() as u64;
    let before_ts = before_utc.timestamp() as u64;

    let file_list = store
        .list_all(FileType::CellHeartbeat, after_utc, before_utc)
        .await?;
    metrics::histogram!("reward_server_processed_files", file_list.len() as f64);

    write_json(
        "file_list",
        after_ts,
        before_ts,
        &json!({ "file_list": file_list }),
    )?;

    let mut stream = store.source(stream::iter(file_list).map(Ok).boxed());

    let shares = gather_shares(&mut stream, after_ts, before_ts).await?;
    write_json("shares", after_ts, before_ts, &json!({ "shares": shares }))?;

    let cell_shares = cell_shares(&shares);
    write_json(
        "cell_shares",
        after_ts,
        before_ts,
        &json!({ "cell_shares": cell_shares }),
    )?;

    let hotspot_shares = hotspot_shares(&shares);
    write_json(
        "hotspot_shares",
        after_ts,
        before_ts,
        &json!({ "hotspot_shares": hotspot_shares }),
    )?;

    let (owner_shares, missing_owner_shares) =
        owner_shares(follower_service, hotspot_shares).await?;
    write_json(
        "owner_shares",
        after_ts,
        before_ts,
        &json!({ "owner_shares": owner_shares }),
    )?;

    write_json(
        "missing_owner_shares",
        after_ts,
        before_ts,
        &json!({ "missing_owner_shares": missing_owner_shares }),
    )?;

    let owner_emissions = OwnerEmissions::new(owner_shares, after_utc, before_utc - after_utc);
    write_json(
        "owner_emissions",
        after_ts,
        before_ts,
        &json!({ "owner_emissions": owner_emissions, "total_emissions": owner_emissions.total_emissions() }),
    )?;

    if !owner_emissions.is_empty() {
        let subnetwork_rewards = SubnetworkRewards::from(owner_emissions);
        return Ok(Some(subnetwork_rewards.into()));
    }

    Ok(None)
}

pub fn get_time_range(last_reward_end_time: i64) -> (DateTime<Utc>, DateTime<Utc>) {
    let after_utc = datetime_from_epoch(last_reward_end_time);
    let now = Utc::now();
    let stop_utc = now - Duration::minutes(DEFAULT_LOOKUP_DELAY);
    let start_utc = min(after_utc, stop_utc);
    (start_utc, stop_utc)
}

#[cfg(test)]
mod test {
    use crate::{
        reward_share::{OwnerEmissions, Share, Shares},
        traits::owner_resolver::OwnerResolver,
        CellType,
    };
    use async_trait::async_trait;
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

        let t1: u64 = 100;
        let t2: u64 = 100;
        let t3: u64 = 100;
        let t4: u64 = 100;

        let ct1 = CellType::from_cbsd_id(&c1).expect("unable to get cell_type");
        let ct2 = CellType::from_cbsd_id(&c2).expect("unable to get cell_type");
        let ct3 = CellType::from_cbsd_id(&c3).expect("unable to get cell_type");
        let ct4 = CellType::from_cbsd_id(&c4).expect("unable to get cell_type");

        let mut shares = Shares::new();
        shares.insert(c1, Share::new(t1, g1, ct1.reward_weight(), ct1));
        shares.insert(c2, Share::new(t2, g2, ct2.reward_weight(), ct2));
        shares.insert(c3, Share::new(t3, g3, ct3.reward_weight(), ct3));
        shares.insert(c4, Share::new(t4, g4, ct4.reward_weight(), ct4));

        let hotspot_shares = hotspot_shares(&shares);

        let test_owner = PublicKey::from_str("1ay5TAKuQDjLS6VTpoWU51p3ik3Sif1b3DWRstErqkXFJ4zuG7r")
            .expect("unable to get test pubkey");
        let mut owner_resolver = FixedOwnerResolver { owner: test_owner };

        let (owner_shares, _missing_owner_shares) =
            owner_shares(&mut owner_resolver, hotspot_shares)
                .await
                .expect("unable to get owner_shares");

        let start = Utc::now();
        let duration = Duration::hours(24);
        let owner_emissions = OwnerEmissions::new(owner_shares, start, duration);
        let total_owner_emissions = owner_emissions.total_emissions();

        // 100M in bones
        assert_eq!(10000000000000000, u64::from(total_owner_emissions));

        let subnetwork_rewards = SubnetworkRewards::from(owner_emissions);

        let keypair_b64 = "EeNwbGXheUq4frT05EJwMtvGuz8zHyajOaN2h5yz5M9A58pZdf9bLayp8Ex6x0BkGxREleQnTNwOTyT2vPL0i1_nyll1_1strKnwTHrHQGQbFESV5CdM3A5PJPa88vSLXw";
        let kp = Keypair::try_from(
            Vec::from_b64_url(keypair_b64)
                .expect("unable to get raw keypair")
                .as_ref(),
        )
        .expect("unable to get keypair");
        let (_txn, txn_hash_str) =
            construct_txn(&kp, subnetwork_rewards, RewardPeriod::new(1000, 1010))
                .expect("unable to construct txn");

        // This is taken from a blockchain-node, constructing the exact same txn
        assert_eq!(
            "hpnFwsGQQohIlAYGyaWJ-j3Hs8kIPeskh09qEkoL3I4".to_string(),
            txn_hash_str
        );
    }
}
