use crate::{
    heartbeats::Heartbeats,
    reward_share::{OwnerEmissions, OwnerResolver, ResolveError},
    speedtests::SpeedtestAverages,
};
use chrono::{DateTime, Utc};
use file_store::{file_sink, file_sink_write};
use helium_crypto::PublicKey;
use rust_decimal::Decimal;
use std::collections::HashMap;
use std::ops::Range;
use tokio::sync::oneshot;

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
        mut follower_service: impl OwnerResolver,
        epoch: &Range<DateTime<Utc>>,
        heartbeats: Heartbeats,
        speedtests: SpeedtestAverages,
    ) -> Result<Self, ResolveError> {
        // Gather hotspot shares
        let mut hotspot_shares = HashMap::<PublicKey, Decimal>::new();
        for heartbeat in heartbeats.into_iter() {
            *hotspot_shares
                .entry(heartbeat.hotspot_key.clone())
                .or_default() += heartbeat.reward_weight;
        }

        let filtered_shares = hotspot_shares
            .into_iter()
            .map(|(pubkey, mut shares)| {
                let speedmultiplier = speedtests
                    .get_average(&pubkey)
                    .map_or(Decimal::ZERO, |avg| avg.reward_multiplier());
                shares *= speedmultiplier;
                (pubkey, shares)
            })
            .filter(|(_pubkey, shares)| shares > &Decimal::ZERO)
            .collect();

        let (owner_shares, _missing_owner_shares) =
            follower_service.owner_shares(filtered_shares).await?;

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

    pub async fn write(
        self,
        subnet_rewards_tx: &file_sink::MessageSender,
    ) -> file_store::Result<oneshot::Receiver<file_store::Result>> {
        file_sink_write!(
            "subnet_rewards",
            subnet_rewards_tx,
            proto::SubnetworkRewards {
                start_epoch: self.epoch.start.timestamp() as u64,
                end_epoch: self.epoch.end.timestamp() as u64,
                rewards: self.rewards,
            }
        )
        .await
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::{
        cell_type::CellType,
        heartbeats::{Heartbeat, Heartbeats},
        reward_share::OwnerResolver,
        speedtests::{Speedtest, SpeedtestAverages},
    };
    use chrono::{Duration, NaiveDateTime, Utc};
    use helium_proto::services::poc_mobile::HeartbeatValidity;
    use std::collections::{HashMap, VecDeque};

    struct MapResolver {
        owners: HashMap<PublicKey, PublicKey>,
    }

    #[async_trait::async_trait]
    impl OwnerResolver for MapResolver {
        async fn resolve_owner(
            &mut self,
            address: &PublicKey,
        ) -> Result<Option<PublicKey>, ResolveError> {
            Ok(self.owners.get(address).cloned())
        }
    }

    fn bytes_per_s(mbps: i64) -> i64 {
        mbps * 125000
    }

    fn cell_type_weight(cbsd_id: &String) -> Decimal {
        CellType::from_cbsd_id(cbsd_id)
            .expect("unable to get cell_type")
            .reward_weight()
    }

    fn acceptable_speedtest(timestamp: NaiveDateTime) -> Speedtest {
        Speedtest {
            timestamp,
            upload_speed: bytes_per_s(10),
            download_speed: bytes_per_s(100),
            latency: 25,
        }
    }

    fn degraded_speedtest(timestamp: NaiveDateTime) -> Speedtest {
        Speedtest {
            timestamp,
            upload_speed: bytes_per_s(5),
            download_speed: bytes_per_s(60),
            latency: 60,
        }
    }

    fn failed_speedtest(timestamp: NaiveDateTime) -> Speedtest {
        Speedtest {
            timestamp,
            upload_speed: bytes_per_s(1),
            download_speed: bytes_per_s(20),
            latency: 110,
        }
    }

    fn poor_speedtest(timestamp: NaiveDateTime) -> Speedtest {
        Speedtest {
            timestamp,
            upload_speed: bytes_per_s(2),
            download_speed: bytes_per_s(40),
            latency: 90,
        }
    }

    #[tokio::test]
    async fn test_single_owner_multiple_hotspots() {
        let g1: PublicKey = "11eX55faMbqZB7jzN4p67m6w7ScPMH6ubnvCjCPLh72J49PaJEL"
            .parse()
            .expect("unable to construct pubkey");
        let g2: PublicKey = "118SPA16MX8WrUKcuXxsg6SH8u5dWszAySiUAJX6tTVoQVy7nWc"
            .parse()
            .expect("unable to construct pubkey");

        let c1 = "P27-SCE4255W2107CW5000014".to_string();
        let c2 = "2AG32PBS3101S1202000464223GY0153".to_string();

        let owner1: PublicKey = "1ay5TAKuQDjLS6VTpoWU51p3ik3Sif1b3DWRstErqkXFJ4zuG7r"
            .parse()
            .expect("unable to get test pubkey");
        let owner2: PublicKey = "1126cBTucnhedhxnWp6puBWBk6Xdbpi7nkqeaX4s4xoDy2ja7bcd"
            .parse()
            .expect("unable to get pubkey");

        let mut owners = HashMap::new();
        owners.insert(g1.clone(), owner1.clone());
        owners.insert(g2.clone(), owner2.clone());

        let resolver = MapResolver { owners };

        let now = Utc::now();
        let timestamp = now.naive_utc();

        let heartbeats = vec![
            Heartbeat {
                cbsd_id: c1.clone(),
                hotspot_key: g1.clone(),
                reward_weight: cell_type_weight(&c1),
                validity: HeartbeatValidity::Valid,
                timestamp,
            },
            Heartbeat {
                cbsd_id: c2.clone(),
                hotspot_key: g1.clone(),
                reward_weight: cell_type_weight(&c2),
                validity: HeartbeatValidity::Valid,
                timestamp,
            },
            Heartbeat {
                cbsd_id: c1.clone(),
                hotspot_key: g2.clone(),
                reward_weight: cell_type_weight(&c1),
                validity: HeartbeatValidity::Valid,
                timestamp,
            },
            Heartbeat {
                cbsd_id: c1.clone(),
                hotspot_key: g2.clone(),
                reward_weight: cell_type_weight(&c1),
                validity: HeartbeatValidity::Valid,
                timestamp,
            },
        ];

        let heartbeats: Heartbeats = heartbeats.into_iter().collect();

        let last_timestamp = timestamp - Duration::hours(12);
        let g1_speedtests = vec![
            acceptable_speedtest(last_timestamp),
            acceptable_speedtest(timestamp),
        ];
        let g2_speedtests = vec![
            acceptable_speedtest(last_timestamp),
            acceptable_speedtest(timestamp),
        ];
        let mut speedtests = HashMap::new();
        speedtests.insert(g1, VecDeque::from(g1_speedtests));
        speedtests.insert(g2, VecDeque::from(g2_speedtests));
        let speedtest_avgs = SpeedtestAverages { speedtests };

        let owner_rewards: HashMap<PublicKey, _> = SubnetworkRewards::from_epoch(
            resolver,
            &(now..(now + Duration::hours(24))),
            heartbeats,
            speedtest_avgs,
        )
        .await
        .expect("Could not generate rewards")
        .rewards
        .into_iter()
        .map(|p| (PublicKey::try_from(p.account).unwrap(), p.amount))
        .collect();

        // The owner with two hotspots gets more rewards
        assert!(owner_rewards.get(&owner1).unwrap() > owner_rewards.get(&owner2).unwrap());
    }

    #[tokio::test]
    async fn reward_shares_with_speed_multiplier() {
        // init hotspots
        let owner1: PublicKey = "112NqN2WWMwtK29PMzRby62fDydBJfsCLkCAf392stdok48ovNT6"
            .parse()
            .expect("failed owner1 parse");
        let owner2: PublicKey = "11sctWiP9r5wDJVuDe1Th4XSL2vaawaLLSQF8f8iokAoMAJHxqp"
            .parse()
            .expect("failed owner2 parse");
        let owner3: PublicKey = "112DJZiXvZ8FduiWrEi8siE3wJX6hpRjjtwbavyXUDkgutEUSLAE"
            .parse()
            .expect("failed owner3 parse");
        let owner4: PublicKey = "112p1GbUtRLyfFaJr1XF8fH7yz9cSZ4exbrSpVDeu67DeGb31QUL"
            .parse()
            .expect("failed owner4 parse");

        // init hotspots
        let gw1: PublicKey = "112NqN2WWMwtK29PMzRby62fDydBJfsCLkCAf392stdok48ovNT6"
            .parse()
            .expect("failed gw1 parse");
        let gw2: PublicKey = "11sctWiP9r5wDJVuDe1Th4XSL2vaawaLLSQF8f8iokAoMAJHxqp"
            .parse()
            .expect("failed gw2 parse");
        let gw3: PublicKey = "112DJZiXvZ8FduiWrEi8siE3wJX6hpRjjtwbavyXUDkgutEUSLAE"
            .parse()
            .expect("failed gw3 parse");
        let gw4: PublicKey = "112p1GbUtRLyfFaJr1XF8fH7yz9cSZ4exbrSpVDeu67DeGb31QUL"
            .parse()
            .expect("failed gw4 parse");
        let gw5: PublicKey = "112j1iw1sV2B2Tz2DxPSeum9Cmc5kMKNdDTDg1zDRsdwuvZueq3B"
            .parse()
            .expect("failed gw5 parse");
        let gw6: PublicKey = "11fCasUk9XvU15ktsMMH64J9E7XuqQ2L5FJPv8HZMCDG6kdZ3SC"
            .parse()
            .expect("failed gw6 parse");
        let gw7: PublicKey = "11HdwRpQDrYM7LJtRGSzRF3vY2iwuumx1Z2MUhBYAVTwZdSh6Bi"
            .parse()
            .expect("failed gw7 parse");
        let gw8: PublicKey = "112qDCKek7fePg6wTpEnbLp3uD7TTn8MBH7PGKtmAaUcG1vKQ9eZ"
            .parse()
            .expect("failed gw8 parse");

        // link gws to owners
        let mut owners = HashMap::new();
        owners.insert(gw1.clone(), owner1.clone());
        owners.insert(gw2.clone(), owner1.clone());
        owners.insert(gw3.clone(), owner1.clone());
        owners.insert(gw4.clone(), owner2.clone());
        owners.insert(gw5.clone(), owner2.clone());
        owners.insert(gw6.clone(), owner3.clone());
        owners.insert(gw7.clone(), owner3.clone());
        owners.insert(gw8.clone(), owner4.clone());
        let resolver = MapResolver { owners };

        // init cells and cell_types
        let c1 = "P27-SCE4255W2107CW5000014".to_string();
        let c2 = "P27-SCE4255W2107CW5000015".to_string();
        let c3 = "2AG32PBS3101S1202000464223GY0153".to_string();
        let c4 = "2AG32PBS3101S1202000464223GY0154".to_string();
        let c5 = "P27-SCE4255W2107CW5000016".to_string();
        let c6 = "2AG32PBS3101S1202000464223GY0155".to_string();
        let c7 = "2AG32PBS3101S1202000464223GY0156".to_string();
        let c8 = "P27-SCE4255W2107CW5000017".to_string();
        let c9 = "P27-SCE4255W2107CW5000018".to_string();
        let c10 = "P27-SCE4255W2107CW5000019".to_string();
        let c11 = "P27-SCE4255W2107CW5000020".to_string();
        let c12 = "P27-SCE4255W2107CW5000021".to_string();
        let c13 = "P27-SCE4255W2107CW5000022".to_string();
        let c14 = "2AG32PBS3101S1202000464223GY0157".to_string();

        let now = Utc::now();
        let timestamp = (now - Duration::minutes(20)).naive_utc();

        // setup heartbeats
        let heartbeats = vec![
            Heartbeat {
                cbsd_id: c1.clone(),
                hotspot_key: gw1.clone(),
                reward_weight: cell_type_weight(&c1),
                validity: HeartbeatValidity::NotOperational,
                timestamp,
            },
            Heartbeat {
                cbsd_id: c2.clone(),
                hotspot_key: gw2.clone(),
                reward_weight: cell_type_weight(&c2),
                validity: HeartbeatValidity::Valid,
                timestamp,
            },
            Heartbeat {
                cbsd_id: c3.clone(),
                hotspot_key: gw2.clone(),
                reward_weight: cell_type_weight(&c3),
                validity: HeartbeatValidity::NotOperational,
                timestamp,
            },
            Heartbeat {
                cbsd_id: c4.clone(),
                hotspot_key: gw3.clone(),
                reward_weight: cell_type_weight(&c4),
                validity: HeartbeatValidity::Valid,
                timestamp,
            },
            Heartbeat {
                cbsd_id: c5.clone(),
                hotspot_key: gw4.clone(),
                reward_weight: cell_type_weight(&c5),
                validity: HeartbeatValidity::Valid,
                timestamp,
            },
            Heartbeat {
                cbsd_id: c6.clone(),
                hotspot_key: gw4.clone(),
                reward_weight: cell_type_weight(&c6),
                validity: HeartbeatValidity::Valid,
                timestamp,
            },
            Heartbeat {
                cbsd_id: c7.clone(),
                hotspot_key: gw4.clone(),
                reward_weight: cell_type_weight(&c7),
                validity: HeartbeatValidity::Valid,
                timestamp,
            },
            Heartbeat {
                cbsd_id: c8.clone(),
                hotspot_key: gw4.clone(),
                reward_weight: cell_type_weight(&c8),
                validity: HeartbeatValidity::Valid,
                timestamp,
            },
            Heartbeat {
                cbsd_id: c9.clone(),
                hotspot_key: gw4.clone(),
                reward_weight: cell_type_weight(&c9),
                validity: HeartbeatValidity::Valid,
                timestamp,
            },
            Heartbeat {
                cbsd_id: c10.clone(),
                hotspot_key: gw4.clone(),
                reward_weight: cell_type_weight(&c10),
                validity: HeartbeatValidity::Valid,
                timestamp,
            },
            Heartbeat {
                cbsd_id: c11.clone(),
                hotspot_key: gw4.clone(),
                reward_weight: cell_type_weight(&c11),
                validity: HeartbeatValidity::Valid,
                timestamp,
            },
            Heartbeat {
                cbsd_id: c12.clone(),
                hotspot_key: gw5.clone(),
                reward_weight: cell_type_weight(&c12),
                validity: HeartbeatValidity::Valid,
                timestamp,
            },
            Heartbeat {
                cbsd_id: c13.clone(),
                hotspot_key: gw6.clone(),
                reward_weight: cell_type_weight(&c13),
                validity: HeartbeatValidity::Valid,
                timestamp,
            },
            Heartbeat {
                cbsd_id: c14.clone(),
                hotspot_key: gw7.clone(),
                reward_weight: cell_type_weight(&c14),
                validity: HeartbeatValidity::Valid,
                timestamp,
            },
        ];
        let heartbeats: Heartbeats = heartbeats.into_iter().collect();

        // setup speedtests
        let last_speedtest = timestamp - Duration::hours(12);
        let gw1_speedtests = vec![
            acceptable_speedtest(last_speedtest),
            acceptable_speedtest(timestamp),
        ];
        let gw2_speedtests = vec![
            acceptable_speedtest(last_speedtest),
            acceptable_speedtest(timestamp),
        ];
        let gw3_speedtests = vec![
            acceptable_speedtest(last_speedtest),
            acceptable_speedtest(timestamp),
        ];
        let gw4_speedtests = vec![
            acceptable_speedtest(last_speedtest),
            acceptable_speedtest(timestamp),
        ];
        let gw5_speedtests = vec![
            degraded_speedtest(last_speedtest),
            degraded_speedtest(timestamp),
        ];
        let gw6_speedtests = vec![
            failed_speedtest(last_speedtest),
            failed_speedtest(timestamp),
        ];
        let gw7_speedtests = vec![poor_speedtest(last_speedtest), poor_speedtest(timestamp)];
        let mut speedtests = HashMap::new();
        speedtests.insert(gw1, VecDeque::from(gw1_speedtests));
        speedtests.insert(gw2, VecDeque::from(gw2_speedtests));
        speedtests.insert(gw3, VecDeque::from(gw3_speedtests));
        speedtests.insert(gw4, VecDeque::from(gw4_speedtests));
        speedtests.insert(gw5, VecDeque::from(gw5_speedtests));
        speedtests.insert(gw6, VecDeque::from(gw6_speedtests));
        speedtests.insert(gw7, VecDeque::from(gw7_speedtests));
        let speedtest_avgs = SpeedtestAverages { speedtests };

        // calculate the rewards for the sample group
        let owner_rewards: HashMap<PublicKey, _> = SubnetworkRewards::from_epoch(
            resolver,
            &((now - Duration::hours(1))..now),
            heartbeats,
            speedtest_avgs,
        )
        .await
        .expect("failed to generate rewards")
        .rewards
        .into_iter()
        .map(|p| (PublicKey::try_from(p.account).unwrap(), p.amount))
        .collect();

        assert_eq!(*owner_rewards.get(&owner1).unwrap(), 99_715_099_715_100);
        assert_eq!(*owner_rewards.get(&owner2).unwrap(), 299_145_299_145_299);
        assert_eq!(*owner_rewards.get(&owner3).unwrap(), 17_806_267_806_268);
        assert_eq!(owner_rewards.get(&owner4), None);

        let mut total = 0;
        for val in owner_rewards.values() {
            total += *val
        }
        assert_eq!(total, 416_666_666_666_667); // total emissions for 1 hour
    }
}
