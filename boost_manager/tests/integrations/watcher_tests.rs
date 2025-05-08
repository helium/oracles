use crate::common::{self, MockFileSinkReceiver, MockHexBoostingClient};
use boost_manager::watcher::{self, Watcher};
use chrono::{Duration as ChronoDuration, Duration, Utc};
use helium_proto::BoostedHexInfoV1 as BoostedHexInfoProto;
use mobile_config::boosted_hex_info::BoostedHexInfo;
use solana::SolPubkey;
use sqlx::PgPool;
use std::{num::NonZeroU32, str::FromStr};

const BOOST_HEX_PUBKEY: &str = "J9JiLTpjaShxL8eMvUs8txVw6TZ36E38SiJ89NxnMbLU";
const BOOST_CONFIG_PUBKEY: &str = "BZM1QTud72B2cpTW7PhEnFmRX7ZWzvY7DpPpNJJuDrWG";

#[sqlx::test]
async fn test_boosted_hex_updates_to_filestore(pool: PgPool) -> anyhow::Result<()> {
    let (hex_update_client, mut hex_update) = common::create_file_sink();

    let now = Utc::now();
    let epoch = (now - ChronoDuration::hours(24))..now;
    let boost_period_length = Duration::days(30);

    // setup boosted hex data to stream as updates
    let multipliers1 = vec![
        NonZeroU32::new(2).unwrap(),
        NonZeroU32::new(10).unwrap(),
        NonZeroU32::new(15).unwrap(),
        NonZeroU32::new(35).unwrap(),
    ];
    let start_ts_1 = epoch.start - boost_period_length;
    let end_ts_1 = start_ts_1 + (boost_period_length * multipliers1.len() as i32);
    let multipliers2 = vec![
        NonZeroU32::new(3).unwrap(),
        NonZeroU32::new(10).unwrap(),
        NonZeroU32::new(20).unwrap(),
    ];
    let start_ts_2 = epoch.start - (boost_period_length * 2);
    let end_ts_2 = start_ts_2 + (boost_period_length * multipliers2.len() as i32);

    let boosted_hexes = vec![
        BoostedHexInfo {
            location: 0x8a1fb466d2dffff_u64.try_into().expect("valid h3 cell"),
            start_ts: Some(start_ts_1),
            end_ts: Some(end_ts_1),
            period_length: boost_period_length,
            multipliers: multipliers1,
            boosted_hex_pubkey: SolPubkey::from_str(BOOST_HEX_PUBKEY).unwrap(),
            boost_config_pubkey: SolPubkey::from_str(BOOST_CONFIG_PUBKEY).unwrap(),
            version: 0,
        },
        BoostedHexInfo {
            location: 0x8a1fb49642dffff_u64.try_into().expect("valid h3 cell"),
            start_ts: Some(start_ts_2),
            end_ts: Some(end_ts_2),
            period_length: boost_period_length,
            multipliers: multipliers2,
            boosted_hex_pubkey: SolPubkey::from_str(BOOST_HEX_PUBKEY).unwrap(),
            boost_config_pubkey: SolPubkey::from_str(BOOST_CONFIG_PUBKEY).unwrap(),
            version: 0,
        },
    ];

    let hex_boosting_client = MockHexBoostingClient::new(boosted_hexes);

    let mut watcher = Watcher::new(pool.clone(), hex_update_client, hex_boosting_client)
        .await
        .unwrap();

    let last_processed_ts = now - Duration::days(1);
    watcher::save_last_processed_timestamp(&pool, &last_processed_ts).await?;

    let (_, boosted_hexes_result) = tokio::join!(
        watcher.handle_tick(),
        receive_expected_msgs(&mut hex_update)
    );

    if let Ok(boosted_hexes) = boosted_hexes_result {
        let res_boosted_hex_pubkey1 =
            SolPubkey::try_from(boosted_hexes[0].boosted_hex_pubkey.as_slice())?.to_string();
        let res_boost_config_pubkey1 =
            SolPubkey::try_from(boosted_hexes[0].boost_config_pubkey.as_slice())?.to_string();
        let res_boosted_hex_pubkey2 =
            SolPubkey::try_from(boosted_hexes[1].boosted_hex_pubkey.as_slice())?.to_string();
        let res_boost_config_pubkey2 =
            SolPubkey::try_from(boosted_hexes[1].boost_config_pubkey.as_slice())?.to_string();

        // assert the boosted hexes outputted to filestore
        assert_eq!(2, boosted_hexes.len());
        assert_eq!(0x8a1fb49642dffff_u64, boosted_hexes[0].location);
        assert_eq!(0x8a1fb466d2dffff_u64, boosted_hexes[1].location);
        assert_eq!(BOOST_HEX_PUBKEY, res_boosted_hex_pubkey1);
        assert_eq!(BOOST_CONFIG_PUBKEY, res_boost_config_pubkey1);
        assert_eq!(BOOST_HEX_PUBKEY, res_boosted_hex_pubkey2);
        assert_eq!(BOOST_CONFIG_PUBKEY, res_boost_config_pubkey2);
    } else {
        panic!("no boosted hex updates received");
    };
    Ok(())
}

async fn receive_expected_msgs(
    hex_update: &mut MockFileSinkReceiver,
) -> anyhow::Result<Vec<BoostedHexInfoProto>> {
    // get the filestore outputs
    // we will have 2 updates hexes
    let hex_update_1 = hex_update.receive_updated_hex().await;
    let hex_update_2 = hex_update.receive_updated_hex().await;
    // ordering is not guaranteed, so stick the updates into a vec and sort
    let mut updates = vec![hex_update_1, hex_update_2];
    updates.sort_by(|a, b| b.location.cmp(&a.location));
    // should be no further msgs
    hex_update.assert_no_messages();
    Ok(updates)
}
