use crate::common;
use chrono::{TimeZone, Utc};
use helium_crypto::PublicKeyBinary;
use helium_proto::services::poc_lora::{
    InvalidParticipantSide, InvalidReason, LoraBeaconReportReqV1, LoraWitnessReportReqV1,
};

use iot_verifier::{poc_report::Report, purger::Purger};
use sqlx::{PgPool, Pool, Postgres};
use std::{self, str::FromStr, time::Duration};

#[sqlx::test]
async fn test_purger(pool: PgPool) -> anyhow::Result<()> {
    // setup file sinks
    let (invalid_beacon_client, mut invalid_beacons) = common::create_file_sink();
    let (invalid_witness_client, mut invalid_witnesses) = common::create_file_sink();

    // default stale periods after which the purger will delete reports from the db
    let base_stale_period = Duration::from_secs(0);
    let beacon_stale_period = Duration::from_secs(3);
    let witness_stale_period = Duration::from_secs(3);
    let entropy_stale_period = Duration::from_secs(3);

    // create the purger
    let purger = Purger {
        base_stale_period,
        beacon_stale_period,
        witness_stale_period,
        entropy_stale_period,
        pool: pool.clone(),
        invalid_beacon_sink: invalid_beacon_client,
        invalid_witness_sink: invalid_witness_client,
    };

    // default reports timestamp
    let entropy_ts = Utc.timestamp_millis_opt(common::ENTROPY_TIMESTAMP).unwrap();
    let report_ts = entropy_ts + Duration::from_secs(60);

    //
    // inject a beacon, witness & entropy report into the db
    // the runner is not running, so these will end up being purged
    // by the purger after they are in the DB longer than stale
    // periods defined above
    //

    let beacon_to_inject = common::create_valid_beacon_report(common::BEACONER1, report_ts);
    let witness_to_inject = common::create_valid_witness_report(common::WITNESS1, report_ts);
    common::inject_beacon_report(pool.clone(), beacon_to_inject.clone()).await?;
    common::inject_witness_report(pool.clone(), witness_to_inject.clone()).await?;
    common::inject_entropy_report(pool.clone(), entropy_ts).await?;

    // sleep for a period longer than the stale periods defined above
    tokio::time::sleep(Duration::from_secs(3)).await;

    // confirm the beacon and witness reports are in the db & showing as stale
    let stale_witness_count = Report::get_stale_beacons(&pool, beacon_stale_period)
        .await?
        .len();
    assert_eq!(1, stale_witness_count);
    let stale_witness_count = Report::get_stale_witnesses(&pool, witness_stale_period)
        .await?
        .len();
    assert_eq!(1, stale_witness_count);
    // confirm the entropy report is in the db
    assert_eq!(1, get_entropy_report_count(&pool).await?);

    // run the purger tick
    purger.handle_db_tick().await?;

    // confirm we get reports to filestore
    // NOTE: purged entropy reports do not result in a filestore write
    let invalid_beacon_report = invalid_beacons.receive_invalid_beacon().await;
    let invalid_witness_report = invalid_witnesses.receive_invalid_witness().await;
    let invalid_beacon = invalid_beacon_report.report.clone().unwrap();
    let invalid_witness = invalid_witness_report.report.clone().unwrap();

    // assert the pubkeys in the outputted reports
    // match those which we injected
    assert_eq!(
        PublicKeyBinary::from(invalid_beacon.pub_key.clone()),
        PublicKeyBinary::from_str(common::BEACONER1).unwrap()
    );
    assert_eq!(
        PublicKeyBinary::from(invalid_witness.pub_key.clone()),
        PublicKeyBinary::from_str(common::WITNESS1).unwrap()
    );

    // assert the invalid details
    assert_eq!(InvalidReason::Stale as i32, invalid_beacon_report.reason);
    assert_eq!(InvalidReason::Stale as i32, invalid_witness_report.reason);
    assert_eq!(
        InvalidParticipantSide::Witness as i32,
        invalid_witness_report.participant_side
    );
    // assert the beacon and witness reports outputted to filestore
    // are unmodified from those submitted
    assert_eq!(
        invalid_beacon,
        LoraBeaconReportReqV1::from(beacon_to_inject.clone())
    );
    assert_eq!(
        invalid_witness,
        LoraWitnessReportReqV1::from(witness_to_inject.clone())
    );

    // confirm the beacon, witness & entropy reports are no longer in the db
    let stale_witness_count = Report::get_stale_beacons(&pool, beacon_stale_period)
        .await?
        .len();
    assert_eq!(0, stale_witness_count);
    let stale_witness_count = Report::get_stale_witnesses(&pool, witness_stale_period)
        .await?
        .len();
    assert_eq!(0, stale_witness_count);
    assert_eq!(0, get_entropy_report_count(&pool).await?);
    Ok(())
}

pub async fn get_entropy_report_count(db: &Pool<Postgres>) -> Result<i64, sqlx::Error> {
    sqlx::query_scalar(" select count(id) from entropy ")
        .fetch_one(db)
        .await
}
