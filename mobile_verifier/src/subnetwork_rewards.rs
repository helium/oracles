use crate::{
    bones_to_u64, cell_share_to_u64,
    error::{Error, Result},
    reward_share::{
        self, cell_shares, hotspot_shares, GatheredShares, OwnerEmissions, OwnerResolver,
    },
    reward_speed_share::SpeedShare,
};
use chrono::{DateTime, Utc};
use file_store::{file_sink, FileInfo, FileStore, FileType};
use futures::stream::{self, StreamExt};
use helium_proto::services::{follower, Channel};
use serde::Serialize;
use std::path::Path;

mod proto {
    pub use helium_proto::services::poc_mobile::*;
    pub use helium_proto::{SubnetworkReward, SubnetworkRewards};
}

#[derive(Debug, Clone, Serialize)]
pub struct SubnetworkRewards {
    pub after_ts: u64,
    pub before_ts: u64,
    pub file_list: Vec<FileInfo>,
    pub gathered_shares: GatheredShares,
    pub cell_shares: reward_share::CellShares,
    pub hotspot_shares: reward_share::HotspotShares,
    pub owner_shares: reward_share::OwnerShares,
    pub missing_owner_shares: reward_share::MissingOwnerShares,
    pub rewards: Vec<proto::SubnetworkReward>,
}

impl SubnetworkRewards {
    pub fn is_empty(&self) -> bool {
        self.rewards.is_empty()
    }

    pub async fn from_period(
        file_store: &FileStore,
        mut follower_service: follower::Client<Channel>,
        after_utc: DateTime<Utc>,
        before_utc: DateTime<Utc>,
    ) -> Result<Self> {
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

        let mut file_list = file_store
            .list_all(FileType::CellHeartbeatIngestReport, after_utc, before_utc)
            .await?;
        file_list.extend(
            file_store
                .list_all(FileType::CellSpeedtestIngestReport, after_utc, before_utc)
                .await?,
        );

        metrics::histogram!("verifier_server_processed_files", file_list.len() as f64);

        let mut stream = file_store.source(stream::iter(file_list.clone()).map(Ok).boxed());

        let gathered_shares =
            GatheredShares::from_stream(&mut stream, after_utc, before_utc).await?;

        let cell_shares = cell_shares(&gathered_shares.shares);

        let hotspot_shares = hotspot_shares(
            &gathered_shares.shares,
            &gathered_shares.speed_shares_moving_avg,
        );

        let (owner_shares, missing_owner_shares) = follower_service
            .owner_shares(hotspot_shares.clone())
            .await?;

        let owner_emissions =
            OwnerEmissions::new(owner_shares.clone(), after_utc, before_utc - after_utc);

        let mut rewards = owner_emissions
            .into_inner()
            .into_iter()
            .map(|(owner, amt)| proto::SubnetworkReward {
                account: owner.to_vec(),
                amount: u64::from(amt),
            })
            .collect::<Vec<_>>();

        // Sort the rewards
        rewards.sort_by(|a, b| {
            a.account
                .cmp(&b.account)
                .then_with(|| a.amount.cmp(&b.amount))
        });

        Ok(Self {
            after_ts,
            before_ts,
            file_list,
            gathered_shares,
            cell_shares,
            hotspot_shares,
            owner_shares,
            missing_owner_shares,
            rewards,
        })
    }

    /// Write output from each step to S3
    pub async fn write(
        self,
        shares_tx: &file_sink::MessageSender,
        invalid_shares_tx: &file_sink::MessageSender,
        subnet_tx: &file_sink::MessageSender,
    ) -> Result<()> {
        // TODO: Clean up these conversions
        // One possibility for the common cases would be to wrap in a macro.
        // Also, a lot of the meat of these conversion should be moved to helper
        // functions.
        let Self {
            after_ts,
            before_ts,
            file_list,
            gathered_shares:
                GatheredShares {
                    shares,
                    speed_shares,
                    speed_shares_moving_avg,
                    invalid_shares,
                    invalid_speed_shares,
                },
            cell_shares,
            hotspot_shares,
            owner_shares,
            missing_owner_shares,
            rewards,
        } = self;

        write_json(
            "file_list",
            after_ts,
            before_ts,
            &proto::ProcessedFiles {
                files: file_list
                    .into_iter()
                    .map(|file_info| proto::FileInfo {
                        key: file_info.key,
                        file_type: file_info.file_type as i32,
                        timestamp: file_info.timestamp.timestamp() as u64,
                        size: file_info.size as u64,
                    })
                    .collect(),
            },
        )?;

        write_json(
            "speed_shares",
            after_ts,
            before_ts,
            &proto::SpeedShares {
                speed_shares: speed_shares
                    .into_iter()
                    .map(|(key, shares)| proto::SpeedShareList {
                        gw_pub_key: key.to_vec(),
                        speed_shares: shares.into_iter().map(SpeedShare::into).collect(),
                    })
                    .collect(),
            },
        )?;

        write_json(
            "invalid_speed_shares",
            after_ts,
            before_ts,
            &proto::InvalidSpeedShares {
                speed_shares: invalid_speed_shares
                    .into_iter()
                    .map(|share| proto::SpeedShare {
                        pub_key: share.pub_key.to_vec(),
                        upload_speed_bps: share.upload_speed,
                        download_speed_bps: share.download_speed,
                        latency_ms: share.latency,
                        timestamp: share.timestamp.timestamp() as u64,
                        validity: share.validity as i32,
                    })
                    .collect(),
            },
        )?;

        write_json(
            "speed_shares_moving_avg",
            after_ts,
            before_ts,
            &proto::SpeedShareMovingAvgs {
                moving_avgs: speed_shares_moving_avg
                    .into_inner()
                    .into_iter()
                    .map(|(key, moving_avg)| {
                        proto::MovingAvg {
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
        )?;

        write_json(
            "cell_shares",
            after_ts,
            before_ts,
            &proto::CellShares {
                shares: cell_shares
                    .into_iter()
                    .map(|(cell_type, weight)| proto::CellShare {
                        cell_type: cell_type as i32,
                        weight: cell_share_to_u64(weight),
                    })
                    .collect(),
            },
        )?;

        write_json(
            "hotspot_shares",
            after_ts,
            before_ts,
            &proto::HotspotShares {
                shares: hotspot_shares
                    .into_iter()
                    .map(|(key, weight)| proto::HotspotShare {
                        pub_key: key.to_vec(),
                        weight: bones_to_u64(weight),
                    })
                    .collect(),
            },
        )?;

        write_json(
            "owner_shares",
            after_ts,
            before_ts,
            &proto::OwnerShares {
                shares: owner_shares
                    .into_iter()
                    .map(|(key, weight)| proto::OwnerShare {
                        pub_key: key.to_vec(),
                        weight: bones_to_u64(weight),
                    })
                    .collect(),
            },
        )?;

        write_json(
            "missing_owner_shares",
            after_ts,
            before_ts,
            &proto::OwnerShares {
                shares: missing_owner_shares
                    .clone()
                    .into_iter()
                    .map(|(key, weight)| proto::OwnerShare {
                        pub_key: key.to_vec(),
                        weight: bones_to_u64(weight),
                    })
                    .collect(),
            },
        )?;

        file_sink::write(
            shares_tx,
            proto::Shares {
                shares: shares
                    .into_iter()
                    .map(|(cbsd_id, share)| proto::Share {
                        cbsd_id,
                        timestamp: share.timestamp.timestamp() as u64,
                        pub_key: share.pub_key.to_vec(),
                        weight: bones_to_u64(share.weight),
                        cell_type: share.cell_type as i32,
                        validity: proto::ShareValidity::Valid as i32,
                    })
                    .collect(),
            },
        )
        .await?;

        file_sink::write(
            invalid_shares_tx,
            proto::Shares {
                shares: invalid_shares,
            },
        )
        .await?;

        file_sink::write(
            subnet_tx,
            proto::SubnetworkRewards {
                start_epoch: after_ts,
                end_epoch: before_ts,
                rewards,
            },
        )
        .await?;

        Ok(())
    }
}

fn write_json(
    fname_prefix: &str,
    after_ts: u64,
    before_ts: u64,
    data: &impl serde::Serialize,
) -> Result {
    let tmp_output_dir = std::env::var("TMP_OUTPUT_DIR").unwrap_or_else(|_| "/tmp".to_string());
    let fname = format!("{}-{}-{}.json", fname_prefix, after_ts, before_ts);
    let fpath = Path::new(&tmp_output_dir).join(&fname);
    std::fs::write(Path::new(&fpath), serde_json::to_string_pretty(data)?)?;
    Ok(())
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
        // Only g1 should be in hotspot_shares
        assert!(hotspot_shares.contains_key(&g1) && hotspot_shares.keys().len() == 1);

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
