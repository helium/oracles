use crate::{
    error::{Error, Result},
    reward_share::{cell_shares, hotspot_shares, GatheredShares, OwnerEmissions, OwnerResolver},
    reward_speed_share::SpeedShare,
    subnetwork_reward::sorted_rewards,
    write_json,
};
use chrono::{DateTime, Duration, NaiveDateTime, Utc};
use futures::stream::{self, StreamExt};
use helium_proto::{
    follower_client::FollowerClient,
    services::{
        poc_mobile::{
            CellShare, CellShares, FileInfo, HotspotShare, HotspotShares, InvalidShare,
            InvalidShares, InvalidSpeedShare, InvalidSpeedShares, MissingOwnerShare,
            MissingOwnerShares, MovingAvg, OwnerEmission, OwnerEmissions as OwnerEmissionsProto,
            OwnerShare, OwnerShares, ProcessedFiles, Share, Shares, SpeedShareList,
            SpeedShareMovingAvgs, SpeedShares,
        },
        Channel,
    },
    SubnetworkReward as ProtoSubnetworkReward,
};
use poc_store::{FileStore, FileType};
use rust_decimal::prelude::*;
use rust_decimal_macros::dec;
use serde::Serialize;
use std::cmp::min;

// default hours to delay lookup from now
pub const DEFAULT_LOOKUP_DELAY: i64 = 20;

#[derive(Debug, Clone, Serialize)]
pub struct SubnetworkRewards(pub Vec<ProtoSubnetworkReward>);

impl SubnetworkRewards {
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    pub async fn from_period(
        input_store: &FileStore,
        output_store: &FileStore,
        follower_service: FollowerClient<Channel>,
        after_utc: DateTime<Utc>,
        before_utc: DateTime<Utc>,
    ) -> Result<Option<Self>> {
        if let Some(rewards) = get_rewards(
            input_store,
            output_store,
            follower_service,
            after_utc,
            before_utc,
        )
        .await?
        {
            return Ok(Some(Self(rewards)));
        }
        Ok(None)
    }

    pub async fn from_last_reward_end_time(
        input_store: &FileStore,
        output_store: &FileStore,
        follower_service: FollowerClient<Channel>,
        last_reward_end_time: i64,
    ) -> Result<Option<Self>> {
        let (after_utc, before_utc) = get_time_range(last_reward_end_time);
        if let Some(rewards) = get_rewards(
            input_store,
            output_store,
            follower_service,
            after_utc,
            before_utc,
        )
        .await?
        {
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

async fn get_rewards(
    input_store: &FileStore,
    output_store: &FileStore,
    mut follower_service: FollowerClient<Channel>,
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

    let mut file_list = input_store
        .list_all(FileType::CellHeartbeat, after_utc, before_utc)
        .await?;
    file_list.extend(
        input_store
            .list_all(FileType::CellSpeedtest, after_utc, before_utc)
            .await?,
    );

    metrics::histogram!("verifier_server_processed_files", file_list.len() as f64);

    let mut stream = input_store.source(stream::iter(file_list.clone()).map(Ok).boxed());

    let GatheredShares {
        shares,
        speed_shares,
        speed_shares_moving_avg,
        invalid_shares,
        invalid_speed_shares,
    } = GatheredShares::from_stream(&mut stream, after_ts, before_ts).await?;

    let cell_shares = cell_shares(&shares);

    let hotspot_shares = hotspot_shares(&shares, &speed_shares_moving_avg);

    let (owner_shares, missing_owner_shares) = follower_service
        .owner_shares(hotspot_shares.clone())
        .await?;

    let owner_emissions =
        OwnerEmissions::new(owner_shares.clone(), after_utc, before_utc - after_utc);

    ////////////////////////////////////////////////////////////////////////////
    // Write output from each step to S3:
    //
    // TODO: Clean up these conversions
    // One possibility for the common cases would be to wrap in a macro.
    // Also, a lot of the meat of these conversion should be moved to helper
    // functions.

    write_json(
        output_store,
        "file_list",
        after_ts,
        before_ts,
        &ProcessedFiles {
            files: file_list
                .into_iter()
                .map(|file_info| FileInfo {
                    key: file_info.key,
                    file_type: file_info.file_type as i32,
                    timestamp: file_info.timestamp.timestamp() as u64,
                    size: file_info.size as u64,
                })
                .collect(),
        },
    )
    .await?;

    write_json(
        output_store,
        "shares",
        after_ts,
        before_ts,
        &Shares {
            shares: shares
                .into_iter()
                .map(|(cbsd_id, share)| Share {
                    cbsd_id,
                    timestamp: share.timestamp,
                    pub_key: share.pub_key.to_vec(),
                    weight: bones_to_u64(share.weight),
                    cell_type: share.cell_type as i32,
                })
                .collect(),
        },
    )
    .await?;

    write_json(
        output_store,
        "speed_shares",
        after_ts,
        before_ts,
        &SpeedShares {
            speed_shares: speed_shares
                .into_iter()
                .map(|(key, shares)| SpeedShareList {
                    gw_pub_key: key.to_vec(),
                    speed_shares: shares.into_iter().map(SpeedShare::into).collect(),
                })
                .collect(),
        },
    )
    .await?;

    write_json(
        output_store,
        "invalid_shares",
        after_ts,
        before_ts,
        &InvalidShares {
            shares: invalid_shares
                .into_iter()
                .map(|share| InvalidShare {
                    cbsd_id: share.cbsd_id,
                    pub_key: share.pub_key,
                    weight: bones_to_u64(share.weight),
                    timestamp: share.timestamp,
                    cell_type: share.cell_type as i32,
                    invalid_reason: share.invalid_reason.to_string(),
                })
                .collect(),
        },
    )
    .await?;

    write_json(
        output_store,
        "invalid_speed_shares",
        after_ts,
        before_ts,
        &InvalidSpeedShares {
            speed_shares: invalid_speed_shares
                .into_iter()
                .map(|share| InvalidSpeedShare {
                    pub_key: share.pub_key,
                    upload_speed: share.upload_speed,
                    download_speed: share.download_speed,
                    latency: share.latency,
                    timestamp: share.timestamp,
                    invalid_reason: share.invalid_reason.to_string(),
                })
                .collect(),
        },
    )
    .await?;

    write_json(
        output_store,
        "speed_shares_moving_avg",
        after_ts,
        before_ts,
        &SpeedShareMovingAvgs {
            moving_avgs: speed_shares_moving_avg
                .into_inner()
                .into_iter()
                .map(|(key, moving_avg)| {
                    MovingAvg {
                        pub_key: key.to_vec(),
                        window_size: moving_avg.window_size as u64,
                        is_valid: moving_avg.is_valid,
                        // TODO: Flatten this so that we do not need to wrap it in an optional.
                        average: Some(moving_avg.average.into()),
                        speed_shares: moving_avg
                            .speed_shares
                            .into_iter()
                            .map(SpeedShare::into)
                            .collect(),
                    }
                })
                .collect(),
        },
    )
    .await?;

    write_json(
        output_store,
        "cell_shares",
        after_ts,
        before_ts,
        &CellShares {
            shares: cell_shares
                .into_iter()
                .map(|(cell_type, weight)| CellShare {
                    cell_type: cell_type as i32,
                    weight: cell_share_to_u64(weight),
                })
                .collect(),
        },
    )
    .await?;

    write_json(
        output_store,
        "hotspot_shares",
        after_ts,
        before_ts,
        &HotspotShares {
            shares: hotspot_shares
                .into_iter()
                .map(|(key, weight)| HotspotShare {
                    pub_key: key.to_vec(),
                    weight: bones_to_u64(weight),
                })
                .collect(),
        },
    )
    .await?;

    write_json(
        output_store,
        "owner_shares",
        after_ts,
        before_ts,
        &OwnerShares {
            shares: owner_shares
                .into_iter()
                .map(|(key, weight)| OwnerShare {
                    pub_key: key.to_vec(),
                    weight: bones_to_u64(weight),
                })
                .collect(),
        },
    )
    .await?;

    write_json(
        output_store,
        "missing_owner_shares",
        after_ts,
        before_ts,
        &MissingOwnerShares {
            shares: missing_owner_shares
                .clone()
                .into_iter()
                .map(|(key, weight)| MissingOwnerShare {
                    pub_key: key.to_vec(),
                    weight: bones_to_u64(weight),
                })
                .collect(),
        },
    )
    .await?;

    write_json(
        output_store,
        "owner_emissions",
        after_ts,
        before_ts,
        &OwnerEmissionsProto {
            emissions: owner_emissions
                .clone()
                .into_inner()
                .into_iter()
                .map(|(key, weight)| OwnerEmission {
                    pub_key: key.to_vec(),
                    weight: bones_to_u64(weight.into_inner()),
                })
                .collect(),
        },
    )
    .await?;

    if !owner_emissions.is_empty() {
        let subnetwork_rewards = SubnetworkRewards::from(owner_emissions);
        return Ok(Some(subnetwork_rewards.into()));
    }

    Ok(None)
}

fn bones_to_u64(decimal: Decimal) -> u64 {
    // One bone is one million mobiles
    (decimal * dec!(1_000_000)).to_u64().unwrap()
}

fn cell_share_to_u64(decimal: Decimal) -> u64 {
    (decimal * dec!(10)).to_u64().unwrap()
}

pub fn get_time_range(last_reward_end_time: i64) -> (DateTime<Utc>, DateTime<Utc>) {
    let after_utc = datetime_from_epoch(last_reward_end_time);
    let now = Utc::now();
    let stop_utc = now - Duration::hours(DEFAULT_LOOKUP_DELAY);
    let start_utc = min(after_utc, stop_utc);
    (start_utc, stop_utc)
}

pub fn datetime_from_epoch(secs: i64) -> DateTime<Utc> {
    DateTime::<Utc>::from_utc(NaiveDateTime::from_timestamp(secs, 0), Utc)
}

#[cfg(test)]
mod test {
    use crate::{
        cell_type::CellType,
        reward_share::{OwnerEmissions, OwnerResolver, Share, Shares},
        reward_speed_share::{SpeedShare, SpeedShareMovingAvgs, SpeedShares},
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
        shares.insert(c1, Share::new(t1, g1.clone(), ct1.reward_weight(), ct1));
        shares.insert(c2, Share::new(t2, g2.clone(), ct2.reward_weight(), ct2));
        shares.insert(c3, Share::new(t3, g3.clone(), ct3.reward_weight(), ct3));
        shares.insert(c4, Share::new(t4, g4.clone(), ct4.reward_weight(), ct4));

        // All g1 averages are satifsied
        let s1 = vec![
            SpeedShare::new(g1.clone(), 1661578086, 2182223, 11739568, 118),
            SpeedShare::new(g1.clone(), 1661581686, 2589229, 12618734, 30),
            SpeedShare::new(g1.clone(), 1661585286, 11420942, 11376519, 8),
            SpeedShare::new(g1.clone(), 1661588886, 7646683, 35517840, 6),
            SpeedShare::new(g1.clone(), 1661588886, 7646683, 35517840, 6),
            SpeedShare::new(g1.clone(), 1661588886, 8646683, 35517840, 6),
        ];
        // The avg latency for g2 is too high, should not appear in hotspot_shares
        let s2 = vec![
            SpeedShare::new(g2.clone(), 1661578086, 2182223, 11739568, 118),
            SpeedShare::new(g2.clone(), 1661581686, 2589229, 12618734, 30),
            SpeedShare::new(g2.clone(), 1661585286, 11420942, 11376519, 40),
            SpeedShare::new(g2.clone(), 1661588886, 7646683, 35517840, 60),
            SpeedShare::new(g2.clone(), 1661588886, 7646683, 35517840, 55),
            SpeedShare::new(g2.clone(), 1661588886, 8646683, 35517840, 58),
        ];
        // The avg upload speed for g3 is too low, should not appear in hotspot_shares
        let s3 = vec![
            SpeedShare::new(g1.clone(), 1661578086, 182223, 11739568, 118),
            SpeedShare::new(g1.clone(), 1661581686, 589229, 12618734, 30),
            SpeedShare::new(g1.clone(), 1661585286, 1420942, 11376519, 8),
            SpeedShare::new(g1.clone(), 1661588886, 646683, 35517840, 6),
            SpeedShare::new(g1.clone(), 1661588886, 646683, 35517840, 6),
            SpeedShare::new(g1.clone(), 1661588886, 646683, 35517840, 6),
        ];
        // The avg download speed for g4 is too low, should not appear in hotspot_shares
        let s4 = vec![
            SpeedShare::new(g4.clone(), 1661578086, 2182223, 1739568, 118),
            SpeedShare::new(g4.clone(), 1661581686, 2589229, 2618734, 30),
            SpeedShare::new(g4.clone(), 1661585286, 11420942, 1376519, 8),
            SpeedShare::new(g4.clone(), 1661588886, 7646683, 5517840, 6),
            SpeedShare::new(g4.clone(), 1661588886, 7646683, 5517840, 6),
            SpeedShare::new(g4.clone(), 1661588886, 8646683, 5517840, 6),
        ];

        let mut speed_shares = SpeedShares::new();
        let mut moving_avgs = SpeedShareMovingAvgs::default();
        speed_shares.insert(g1.clone(), s1);
        speed_shares.insert(g2, s2);
        speed_shares.insert(g3, s3);
        speed_shares.insert(g4, s4);
        moving_avgs.update(&speed_shares);

        let hotspot_shares = hotspot_shares(&shares, &moving_avgs);
        // Only g1 should be in hotspot_shares
        assert!(hotspot_shares.contains_key(&g1) && hotspot_shares.keys().len() == 1);

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
