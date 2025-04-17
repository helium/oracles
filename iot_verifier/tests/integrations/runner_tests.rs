use crate::common::{self, MockFileSinkReceiver};
use async_trait::async_trait;
use chrono::{DateTime, TimeZone, Utc};
use denylist::DenyList;
use futures_util::{stream, StreamExt as FuturesStreamExt};
use helium_crypto::PublicKeyBinary;
use helium_proto::services::poc_lora::{
    InvalidParticipantSide, InvalidReason, LoraBeaconReportReqV1, LoraInvalidBeaconReportV1,
    LoraInvalidWitnessReportV1, LoraPocV1, LoraWitnessReportReqV1, VerificationStatus,
};
use helium_proto::Region as ProtoRegion;
use iot_config::client::ClientError;
use iot_config::{
    client::{Gateways, RegionParamsInfo},
    gateway_info::{GatewayInfo, GatewayInfoStream},
};
use iot_verifier::witness_updater::WitnessUpdater;
use iot_verifier::{
    gateway_cache::GatewayCache, gateway_updater::GatewayUpdater, poc_report::Report,
    region_cache::RegionCache, runner::Runner, tx_scaler::Server as DensityScaler,
};
use lazy_static::lazy_static;
use sqlx::PgPool;
use std::{self, str::FromStr, time::Duration};

lazy_static! {
    static ref BEACON_INTERVAL: Duration = Duration::from_secs(21600);
    static ref BEACON_INTERVAL_PLUS_TWO_HOURS: Duration =
        *BEACON_INTERVAL + Duration::from_secs(2 * 60 * 60);
}
#[derive(Debug, Clone)]
pub struct MockIotConfigClient {
    resolve_gateway: GatewayInfo,
    stream_gateways: Vec<GatewayInfo>,
    region_params: RegionParamsInfo,
}

#[async_trait]
impl Gateways for MockIotConfigClient {
    async fn resolve_gateway_info(
        &mut self,
        _address: &PublicKeyBinary,
    ) -> Result<Option<GatewayInfo>, ClientError> {
        Ok(Some(self.resolve_gateway.clone()))
    }

    async fn stream_gateways_info(&mut self) -> Result<GatewayInfoStream, ClientError> {
        let stream = stream::iter(self.stream_gateways.clone()).boxed();
        Ok(stream)
    }

    async fn resolve_region_params(
        &mut self,
        _region: ProtoRegion,
    ) -> Result<RegionParamsInfo, ClientError> {
        Ok(self.region_params.clone())
    }
}

struct TestContext {
    runner: Runner<MockIotConfigClient>,
    valid_pocs: MockFileSinkReceiver<LoraPocV1>,
    invalid_beacons: MockFileSinkReceiver<LoraInvalidBeaconReportV1>,
    invalid_witnesses: MockFileSinkReceiver<LoraInvalidWitnessReportV1>,
    entropy_ts: DateTime<Utc>,
}

impl TestContext {
    async fn setup(pool: PgPool, beacon_interval: Duration) -> anyhow::Result<Self> {
        // setup file sinks
        let (invalid_beacon_client, invalid_beacons) = common::create_file_sink();
        let (invalid_witness_client, invalid_witnesses) = common::create_file_sink();
        let (valid_poc_client, valid_pocs) = common::create_file_sink();

        // create our mock iot config client
        let iot_config_client = MockIotConfigClient {
            resolve_gateway: common::valid_gateway(),
            stream_gateways: common::valid_gateway_stream(),
            region_params: common::valid_region_params(),
        };

        // setup runner resources
        let deny_list: DenyList = vec![PublicKeyBinary::from_str(common::DENIED_PUBKEY1).unwrap()]
            .try_into()
            .unwrap();
        let refresh_interval = Duration::from_secs(30);
        let (gateway_updater_receiver, _gateway_updater_server) =
            GatewayUpdater::new(refresh_interval, iot_config_client.clone()).await?;
        let gateway_cache = GatewayCache::new(gateway_updater_receiver.clone());
        let density_scaler =
            DensityScaler::new(refresh_interval, pool.clone(), gateway_updater_receiver).await?;
        let region_cache = RegionCache::new(Duration::from_secs(60), iot_config_client.clone())?;
        let (witness_updater, witness_updater_server) = WitnessUpdater::new(pool.clone()).await?;
        // create the runner
        let runner = Runner {
            pool: pool.clone(),
            beacon_interval,
            max_witnesses_per_poc: 16,
            beacon_max_retries: 2,
            witness_max_retries: 2,
            deny_list_latest_url: "https://api.github.com/repos/helium/denylist/releases/latest"
                .to_string(),
            deny_list_trigger_interval: Duration::from_secs(60),
            deny_list,
            gateway_cache: gateway_cache.clone(),
            region_cache,
            invalid_beacon_sink: invalid_beacon_client,
            invalid_witness_sink: invalid_witness_client,
            poc_sink: valid_poc_client,
            hex_density_map: density_scaler.hex_density_map.clone(),
            witness_updater,
        };

        // generate a datetime based on a hardcoded timestamp
        // this is the time the entropy is valid from
        // and all beacon and witness reports will be created
        // with a received_ts based on an offset from this ts
        let entropy_ts = Utc.timestamp_millis_opt(common::ENTROPY_TIMESTAMP).unwrap();
        let report_ts = entropy_ts + Duration::from_secs(60);
        // add the entropy to the DB
        common::inject_entropy_report(pool.clone(), entropy_ts).await?;

        // start up the witness updater
        let (_shutdown_trigger, shutdown) = triggered::trigger();
        let _handle = tokio::spawn(witness_updater_server.run(shutdown));

        Ok(Self {
            runner,
            valid_pocs,
            invalid_beacons,
            invalid_witnesses,
            entropy_ts: report_ts,
        })
    }
}

#[sqlx::test]
async fn valid_beacon_and_witness(pool: PgPool) -> anyhow::Result<()> {
    let mut ctx = TestContext::setup(pool.clone(), *BEACON_INTERVAL).await?;
    let now = ctx.entropy_ts;

    // test with a valid beacon and a valid witness
    let beacon_to_inject = common::create_valid_beacon_report(common::BEACONER1, ctx.entropy_ts);
    let witness_to_inject = common::create_valid_witness_report(common::WITNESS1, ctx.entropy_ts);
    common::inject_beacon_report(pool.clone(), beacon_to_inject.clone()).await?;
    common::inject_witness_report(pool.clone(), witness_to_inject.clone()).await?;

    // inject last beacons and witness reports into the DB
    // avoid the reports declared invalid due to reciprocity check
    // when setting the last time consider the beacon interval setup
    let mut txn = pool.begin().await?;
    common::inject_last_beacon(
        &mut txn,
        beacon_to_inject.report.pub_key.clone(),
        now - (*BEACON_INTERVAL_PLUS_TWO_HOURS),
    )
    .await?;
    common::inject_last_witness(
        &mut txn,
        beacon_to_inject.report.pub_key.clone(),
        now - (*BEACON_INTERVAL_PLUS_TWO_HOURS),
    )
    .await?;
    common::inject_last_beacon(
        &mut txn,
        witness_to_inject.report.pub_key.clone(),
        now - (*BEACON_INTERVAL_PLUS_TWO_HOURS),
    )
    .await?;
    common::inject_last_witness(
        &mut txn,
        witness_to_inject.report.pub_key.clone(),
        now - (*BEACON_INTERVAL_PLUS_TWO_HOURS),
    )
    .await?;
    txn.commit().await?;

    ctx.runner.handle_db_tick().await?;

    let valid_poc = ctx.valid_pocs.receive_valid_poc().await;
    assert_eq!(1, valid_poc.selected_witnesses.len());
    assert_eq!(0, valid_poc.unselected_witnesses.len());
    let valid_beacon = valid_poc.beacon_report.unwrap().report.clone().unwrap();
    let valid_witness_report = valid_poc.selected_witnesses[0].clone();
    let valid_witness = valid_witness_report.report.unwrap();
    // assert the pubkeys in the outputted reports
    // match those which we injected
    assert_eq!(
        PublicKeyBinary::from(valid_beacon.pub_key.clone()),
        PublicKeyBinary::from_str(common::BEACONER1).unwrap()
    );
    assert_eq!(
        PublicKeyBinary::from(valid_witness.pub_key.clone()),
        PublicKeyBinary::from_str(common::WITNESS1).unwrap()
    );
    // assert the witness reports status
    assert_eq!(
        valid_witness_report.status,
        VerificationStatus::Valid as i32
    );
    Ok(())
}

#[sqlx::test]
async fn confirm_valid_reports_unmodified(pool: PgPool) -> anyhow::Result<()> {
    let mut ctx = TestContext::setup(pool.clone(), *BEACON_INTERVAL).await?;
    let now = ctx.entropy_ts;

    // test with a valid beacon and a valid witness
    // confirm the s3 reports are passed through unmodified
    let beacon_to_inject = common::create_valid_beacon_report(common::BEACONER1, ctx.entropy_ts);
    let witness_to_inject = common::create_valid_witness_report(common::WITNESS1, ctx.entropy_ts);
    common::inject_beacon_report(pool.clone(), beacon_to_inject.clone()).await?;
    common::inject_witness_report(pool.clone(), witness_to_inject.clone()).await?;

    // inject last beacons and witness reports into the DB
    // avoid the reports declared invalid due to reciprocity check
    // when setting the last time consider the beacon interval setup
    let mut txn = pool.begin().await?;
    common::inject_last_beacon(
        &mut txn,
        beacon_to_inject.report.pub_key.clone(),
        now - (*BEACON_INTERVAL_PLUS_TWO_HOURS),
    )
    .await?;
    common::inject_last_witness(
        &mut txn,
        beacon_to_inject.report.pub_key.clone(),
        now - (*BEACON_INTERVAL_PLUS_TWO_HOURS),
    )
    .await?;
    common::inject_last_beacon(
        &mut txn,
        witness_to_inject.report.pub_key.clone(),
        now - (*BEACON_INTERVAL_PLUS_TWO_HOURS),
    )
    .await?;
    common::inject_last_witness(
        &mut txn,
        witness_to_inject.report.pub_key.clone(),
        now - (*BEACON_INTERVAL_PLUS_TWO_HOURS),
    )
    .await?;
    txn.commit().await?;

    ctx.runner.handle_db_tick().await?;

    let valid_poc = ctx.valid_pocs.receive_valid_poc().await;
    assert_eq!(1, valid_poc.selected_witnesses.len());
    assert_eq!(0, valid_poc.unselected_witnesses.len());
    let valid_beacon = valid_poc.beacon_report.unwrap().report.clone().unwrap();
    let valid_witness_report = valid_poc.selected_witnesses[0].clone();
    let valid_witness = valid_witness_report.report.unwrap();
    // assert the beacon and witness reports outputted to filestore
    // are unmodified from those submitted
    assert_eq!(
        valid_beacon,
        LoraBeaconReportReqV1::from(beacon_to_inject.clone())
    );
    assert_eq!(
        valid_witness,
        LoraWitnessReportReqV1::from(witness_to_inject.clone())
    );
    Ok(())
}

#[sqlx::test]
async fn confirm_invalid_reports_unmodified(pool: PgPool) -> anyhow::Result<()> {
    let mut ctx = TestContext::setup(pool.clone(), *BEACON_INTERVAL).await?;
    let now = Utc::now();

    // test with an invalid beacon and a valid witness
    // confirm the s3 reports are passed through unmodified
    let beacon_to_inject =
        common::create_valid_beacon_report(common::UNKNOWN_GATEWAY1, ctx.entropy_ts);
    let witness_to_inject = common::create_valid_witness_report(common::WITNESS1, ctx.entropy_ts);
    common::inject_beacon_report(pool.clone(), beacon_to_inject.clone()).await?;
    common::inject_witness_report(pool.clone(), witness_to_inject.clone()).await?;

    let mut txn = pool.begin().await?;
    common::inject_last_beacon(
        &mut txn,
        witness_to_inject.report.pub_key.clone(),
        now - chrono::Duration::hours(1).to_std()?,
    )
    .await?;
    common::inject_last_witness(
        &mut txn,
        witness_to_inject.report.pub_key.clone(),
        now - chrono::Duration::hours(1).to_std()?,
    )
    .await?;
    txn.commit().await?;

    ctx.runner.handle_db_tick().await?;

    let invalid_beacon_report = ctx.invalid_beacons.receive_invalid_beacon().await;
    let invalid_witness_report = ctx.invalid_witnesses.receive_invalid_witness().await;
    let invalid_beacon = invalid_beacon_report.report.clone().unwrap();
    let invalid_witness = invalid_witness_report.clone().report.unwrap();
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

    Ok(())
}

#[sqlx::test]
async fn confirm_valid_beacon_invalid_witness_reports_unmodified(
    pool: PgPool,
) -> anyhow::Result<()> {
    let mut ctx = TestContext::setup(pool.clone(), *BEACON_INTERVAL).await?;
    let now = ctx.entropy_ts;

    // test with an valid beacon and an  invalid witness
    // confirm the s3 reports are passed through unmodified
    let beacon_to_inject = common::create_valid_beacon_report(common::BEACONER3, ctx.entropy_ts);
    let witness_to_inject =
        common::create_valid_witness_report(common::NO_METADATA_GATEWAY1, ctx.entropy_ts);
    common::inject_beacon_report(pool.clone(), beacon_to_inject.clone()).await?;
    common::inject_witness_report(pool.clone(), witness_to_inject.clone()).await?;

    let mut txn = pool.begin().await?;
    common::inject_last_beacon(
        &mut txn,
        beacon_to_inject.report.pub_key.clone(),
        now - (*BEACON_INTERVAL_PLUS_TWO_HOURS),
    )
    .await?;
    common::inject_last_witness(
        &mut txn,
        beacon_to_inject.report.pub_key.clone(),
        now - (*BEACON_INTERVAL_PLUS_TWO_HOURS),
    )
    .await?;
    common::inject_last_beacon(
        &mut txn,
        witness_to_inject.report.pub_key.clone(),
        now - (*BEACON_INTERVAL_PLUS_TWO_HOURS),
    )
    .await?;
    common::inject_last_witness(
        &mut txn,
        witness_to_inject.report.pub_key.clone(),
        now - (*BEACON_INTERVAL_PLUS_TWO_HOURS),
    )
    .await?;
    txn.commit().await?;

    ctx.runner.handle_db_tick().await?;

    let valid_poc = ctx.valid_pocs.receive_valid_poc().await;
    assert_eq!(0, valid_poc.selected_witnesses.len());
    assert_eq!(1, valid_poc.unselected_witnesses.len());
    let valid_beacon = valid_poc.beacon_report.unwrap().report.clone().unwrap();
    let invalid_witness_report = valid_poc.unselected_witnesses[0].clone();
    let invalid_witness = invalid_witness_report.report.clone().unwrap();
    // assert the beacon and witness reports outputted to filestore
    // are unmodified from those submitted
    assert_eq!(
        valid_beacon,
        LoraBeaconReportReqV1::from(beacon_to_inject.clone())
    );
    assert_eq!(
        invalid_witness,
        LoraWitnessReportReqV1::from(witness_to_inject.clone())
    );
    Ok(())
}

#[sqlx::test]
async fn valid_beacon_irregular_schedule_with_witness(pool: PgPool) -> anyhow::Result<()> {
    let mut ctx = TestContext::setup(pool.clone(), *BEACON_INTERVAL).await?;
    let now = ctx.entropy_ts;

    // submit a valid beacon and a valid witness
    // then test with a second beacon from same beaconer
    // this will fail as it is too soon after the first
    // only one beacon per window is allowed
    let beacon_to_inject = common::create_valid_beacon_report(common::BEACONER1, ctx.entropy_ts);
    let witness_to_inject = common::create_valid_witness_report(common::WITNESS1, ctx.entropy_ts);
    common::inject_beacon_report(pool.clone(), beacon_to_inject.clone()).await?;
    common::inject_witness_report(pool.clone(), witness_to_inject.clone()).await?;

    // inject last beacons and witness reports into the DB
    // avoid the reports declared invalid due to reciprocity check
    // when setting the last time consider the beacon interval setup
    let mut txn = pool.begin().await?;
    common::inject_last_beacon(
        &mut txn,
        beacon_to_inject.report.pub_key.clone(),
        now - (*BEACON_INTERVAL_PLUS_TWO_HOURS),
    )
    .await?;
    common::inject_last_witness(
        &mut txn,
        beacon_to_inject.report.pub_key.clone(),
        now - (*BEACON_INTERVAL_PLUS_TWO_HOURS),
    )
    .await?;
    common::inject_last_beacon(
        &mut txn,
        witness_to_inject.report.pub_key.clone(),
        now - (*BEACON_INTERVAL_PLUS_TWO_HOURS),
    )
    .await?;
    common::inject_last_witness(
        &mut txn,
        witness_to_inject.report.pub_key.clone(),
        now - (*BEACON_INTERVAL_PLUS_TWO_HOURS),
    )
    .await?;
    txn.commit().await?;

    ctx.runner.handle_db_tick().await?;

    let valid_poc = ctx.valid_pocs.receive_valid_poc().await;
    assert_eq!(1, valid_poc.selected_witnesses.len());
    assert_eq!(0, valid_poc.unselected_witnesses.len());
    let valid_beacon = valid_poc.beacon_report.unwrap().report.clone().unwrap();
    let valid_witness_report = valid_poc.selected_witnesses[0].clone();
    let valid_witness = valid_witness_report.report.unwrap();
    // assert the pubkeys in the outputted reports
    // match those which we injected
    assert_eq!(
        PublicKeyBinary::from(valid_beacon.pub_key.clone()),
        PublicKeyBinary::from_str(common::BEACONER1).unwrap()
    );
    assert_eq!(
        PublicKeyBinary::from(valid_witness.pub_key.clone()),
        PublicKeyBinary::from_str(common::WITNESS1).unwrap()
    );
    // assert the witness reports status
    assert_eq!(
        valid_witness_report.status,
        VerificationStatus::Valid as i32
    );

    // submit the second beacon report
    let beacon_to_inject = common::create_valid_beacon_report(common::BEACONER1, ctx.entropy_ts);
    let witness_to_inject = common::create_valid_witness_report(common::WITNESS1, ctx.entropy_ts);
    common::inject_beacon_report(pool.clone(), beacon_to_inject.clone()).await?;
    common::inject_witness_report(pool.clone(), witness_to_inject.clone()).await?;
    ctx.runner.handle_db_tick().await?;

    let invalid_beacon_report = ctx.invalid_beacons.receive_invalid_beacon().await;
    let invalid_witness_report = ctx.invalid_witnesses.receive_invalid_witness().await;
    let invalid_beacon = invalid_beacon_report.report.clone().unwrap();
    let invalid_witness = invalid_witness_report.clone().report.unwrap();
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
    assert_eq!(
        InvalidReason::IrregularInterval as i32,
        invalid_beacon_report.reason
    );
    assert_eq!(
        InvalidReason::IrregularInterval as i32,
        invalid_witness_report.reason
    );
    assert_eq!(
        InvalidParticipantSide::Beaconer as i32,
        invalid_witness_report.participant_side
    );
    Ok(())
}

#[sqlx::test]
async fn valid_beacon_irregular_schedule_no_witness(pool: PgPool) -> anyhow::Result<()> {
    let mut ctx = TestContext::setup(pool.clone(), *BEACON_INTERVAL).await?;
    let now = ctx.entropy_ts;

    // submit a valid beacon and no witnesses
    // then test with a second beacon from same beaconer
    // this will fail as it is too soon after the first
    // only one beacon per window is allowed
    let beacon_to_inject = common::create_valid_beacon_report(common::BEACONER1, ctx.entropy_ts);
    common::inject_beacon_report(pool.clone(), beacon_to_inject.clone()).await?;

    // inject last beacons reports into the DB
    // avoid the reports declared invalid due to reciprocity check
    // when setting the last time consider the beacon interval setup
    let mut txn = pool.begin().await?;
    common::inject_last_beacon(
        &mut txn,
        beacon_to_inject.report.pub_key.clone(),
        now - (*BEACON_INTERVAL_PLUS_TWO_HOURS),
    )
    .await?;
    common::inject_last_witness(
        &mut txn,
        beacon_to_inject.report.pub_key.clone(),
        now - (*BEACON_INTERVAL_PLUS_TWO_HOURS),
    )
    .await?;
    txn.commit().await?;

    ctx.runner.handle_db_tick().await?;

    let valid_poc = ctx.valid_pocs.receive_valid_poc().await;
    assert_eq!(0, valid_poc.selected_witnesses.len());
    assert_eq!(0, valid_poc.unselected_witnesses.len());
    let valid_beacon = valid_poc.beacon_report.unwrap().report.clone().unwrap();
    // assert the pubkeys in the outputted reports
    // match those which we injected
    assert_eq!(
        PublicKeyBinary::from(valid_beacon.pub_key.clone()),
        PublicKeyBinary::from_str(common::BEACONER1).unwrap()
    );
    // assert the beacon report outputted to filestore
    // are unmodified from those submitted
    assert_eq!(
        valid_beacon,
        LoraBeaconReportReqV1::from(beacon_to_inject.clone())
    );

    // submit the second beacon report
    let beacon_to_inject = common::create_valid_beacon_report(common::BEACONER1, ctx.entropy_ts);
    let witness_to_inject = common::create_valid_witness_report(common::WITNESS1, ctx.entropy_ts);
    common::inject_beacon_report(pool.clone(), beacon_to_inject.clone()).await?;
    common::inject_witness_report(pool.clone(), witness_to_inject.clone()).await?;
    ctx.runner.handle_db_tick().await?;

    let invalid_beacon_report = ctx.invalid_beacons.receive_invalid_beacon().await;
    let invalid_witness_report = ctx.invalid_witnesses.receive_invalid_witness().await;
    let invalid_beacon = invalid_beacon_report.report.clone().unwrap();
    let invalid_witness = invalid_witness_report.clone().report.unwrap();
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
    assert_eq!(
        InvalidReason::IrregularInterval as i32,
        invalid_beacon_report.reason
    );
    assert_eq!(
        InvalidReason::IrregularInterval as i32,
        invalid_witness_report.reason
    );
    assert_eq!(
        InvalidParticipantSide::Beaconer as i32,
        invalid_witness_report.participant_side
    );
    Ok(())
}

#[sqlx::test]
async fn invalid_beacon_irregular_schedule_with_witness(pool: PgPool) -> anyhow::Result<()> {
    let mut ctx = TestContext::setup(pool.clone(), *BEACON_INTERVAL).await?;
    let now = ctx.entropy_ts;

    // submit an invalid beacon and a valid witness
    // beacon is invalid due to expired entropy
    // then test with a second beacon from same beaconer
    // this will fail as it is too soon after the first
    // only one beacon per window is allowed
    // even if the first beacon is invalid
    let beacon_to_inject = common::create_valid_beacon_report(
        common::BEACONER1,
        ctx.entropy_ts - Duration::from_secs(100),
    );
    let witness_to_inject = common::create_valid_witness_report(common::WITNESS1, ctx.entropy_ts);
    common::inject_beacon_report(pool.clone(), beacon_to_inject.clone()).await?;
    common::inject_witness_report(pool.clone(), witness_to_inject.clone()).await?;

    // inject last beacons and witness reports into the DB
    // avoid the reports declared invalid due to reciprocity check
    // when setting the last time consider the beacon interval setup
    let mut txn = pool.begin().await?;
    common::inject_last_beacon(
        &mut txn,
        beacon_to_inject.report.pub_key.clone(),
        now - (*BEACON_INTERVAL_PLUS_TWO_HOURS),
    )
    .await?;
    common::inject_last_witness(
        &mut txn,
        beacon_to_inject.report.pub_key.clone(),
        now - (*BEACON_INTERVAL_PLUS_TWO_HOURS),
    )
    .await?;
    common::inject_last_beacon(
        &mut txn,
        witness_to_inject.report.pub_key.clone(),
        now - (*BEACON_INTERVAL_PLUS_TWO_HOURS),
    )
    .await?;
    common::inject_last_witness(
        &mut txn,
        witness_to_inject.report.pub_key.clone(),
        now - (*BEACON_INTERVAL_PLUS_TWO_HOURS),
    )
    .await?;
    txn.commit().await?;

    ctx.runner.handle_db_tick().await?;

    let invalid_beacon_report = ctx.invalid_beacons.receive_invalid_beacon().await;
    let invalid_witness_report = ctx.invalid_witnesses.receive_invalid_witness().await;
    let invalid_beacon = invalid_beacon_report.report.clone().unwrap();
    let invalid_witness = invalid_witness_report.clone().report.unwrap();
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
    assert_eq!(
        InvalidReason::EntropyExpired as i32,
        invalid_beacon_report.reason
    );
    assert_eq!(
        InvalidReason::EntropyExpired as i32,
        invalid_witness_report.reason
    );
    assert_eq!(
        InvalidParticipantSide::Beaconer as i32,
        invalid_witness_report.participant_side
    );

    // submit the second beacon report
    let beacon_to_inject = common::create_valid_beacon_report(common::BEACONER1, ctx.entropy_ts);
    let witness_to_inject = common::create_valid_witness_report(common::WITNESS1, ctx.entropy_ts);
    common::inject_beacon_report(pool.clone(), beacon_to_inject.clone()).await?;
    common::inject_witness_report(pool.clone(), witness_to_inject.clone()).await?;
    ctx.runner.handle_db_tick().await?;

    let invalid_beacon_report = ctx.invalid_beacons.receive_invalid_beacon().await;
    let invalid_witness_report = ctx.invalid_witnesses.receive_invalid_witness().await;
    let invalid_beacon = invalid_beacon_report.report.clone().unwrap();
    let invalid_witness = invalid_witness_report.clone().report.unwrap();
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
    assert_eq!(
        InvalidReason::IrregularInterval as i32,
        invalid_beacon_report.reason
    );
    assert_eq!(
        InvalidReason::IrregularInterval as i32,
        invalid_witness_report.reason
    );
    assert_eq!(
        InvalidParticipantSide::Beaconer as i32,
        invalid_witness_report.participant_side
    );
    Ok(())
}

#[sqlx::test]
async fn valid_beacon_gateway_not_found(pool: PgPool) -> anyhow::Result<()> {
    let mut ctx = TestContext::setup(pool.clone(), *BEACON_INTERVAL).await?;
    let now = ctx.entropy_ts;
    //
    // test with a valid beacon and an invalid witness
    // witness is invalid due to not found in iot config
    //
    let beacon_to_inject = common::create_valid_beacon_report(common::BEACONER2, ctx.entropy_ts);
    let witness_to_inject =
        common::create_valid_witness_report(common::UNKNOWN_GATEWAY1, ctx.entropy_ts);
    common::inject_beacon_report(pool.clone(), beacon_to_inject.clone()).await?;
    common::inject_witness_report(pool.clone(), witness_to_inject.clone()).await?;

    // inject last beacons and witness reports into the DB
    // avoid the reports declared invalid due to reciprocity check
    // when setting the last time consider the beacon interval setup
    let mut txn = pool.begin().await?;
    common::inject_last_beacon(
        &mut txn,
        beacon_to_inject.report.pub_key.clone(),
        now - (*BEACON_INTERVAL_PLUS_TWO_HOURS),
    )
    .await?;
    common::inject_last_witness(
        &mut txn,
        beacon_to_inject.report.pub_key.clone(),
        now - (*BEACON_INTERVAL_PLUS_TWO_HOURS),
    )
    .await?;
    txn.commit().await?;

    ctx.runner.handle_db_tick().await?;

    let valid_poc = ctx.valid_pocs.receive_valid_poc().await;
    assert_eq!(0, valid_poc.selected_witnesses.len());
    assert_eq!(1, valid_poc.unselected_witnesses.len());
    let valid_beacon = valid_poc.beacon_report.unwrap().report.clone().unwrap();
    let invalid_witness_report = valid_poc.unselected_witnesses[0].clone();
    // assert the pubkeys in the outputted reports
    // match those which we injected
    assert_eq!(
        PublicKeyBinary::from(valid_beacon.pub_key.clone()),
        PublicKeyBinary::from_str(common::BEACONER2).unwrap()
    );
    assert_eq!(
        PublicKeyBinary::from(invalid_witness_report.report.unwrap().pub_key.clone()),
        PublicKeyBinary::from_str(common::UNKNOWN_GATEWAY1).unwrap()
    );
    // assert the invalid details
    assert_eq!(
        InvalidReason::GatewayNotFound as i32,
        invalid_witness_report.invalid_reason
    );
    assert_eq!(
        InvalidParticipantSide::Witness as i32,
        invalid_witness_report.participant_side
    );
    // assert the witness reports status
    assert_eq!(
        invalid_witness_report.status,
        VerificationStatus::Invalid as i32
    );
    Ok(())
}

#[sqlx::test]
async fn invalid_witness_no_metadata(pool: PgPool) -> anyhow::Result<()> {
    let mut ctx = TestContext::setup(pool.clone(), *BEACON_INTERVAL).await?;
    let now = ctx.entropy_ts;
    //
    // test with a valid beacon and an invalid witness
    // witness is invalid due no metadata in iot config
    //
    let beacon_to_inject = common::create_valid_beacon_report(common::BEACONER3, ctx.entropy_ts);
    let witness_to_inject =
        common::create_valid_witness_report(common::NO_METADATA_GATEWAY1, ctx.entropy_ts);
    common::inject_beacon_report(pool.clone(), beacon_to_inject.clone()).await?;
    common::inject_witness_report(pool.clone(), witness_to_inject.clone()).await?;

    let mut txn = pool.begin().await?;
    common::inject_last_beacon(
        &mut txn,
        beacon_to_inject.report.pub_key.clone(),
        now - (*BEACON_INTERVAL_PLUS_TWO_HOURS),
    )
    .await?;
    common::inject_last_witness(
        &mut txn,
        beacon_to_inject.report.pub_key.clone(),
        now - (*BEACON_INTERVAL_PLUS_TWO_HOURS),
    )
    .await?;
    common::inject_last_beacon(
        &mut txn,
        witness_to_inject.report.pub_key.clone(),
        now - (*BEACON_INTERVAL_PLUS_TWO_HOURS),
    )
    .await?;
    common::inject_last_witness(
        &mut txn,
        witness_to_inject.report.pub_key.clone(),
        now - (*BEACON_INTERVAL_PLUS_TWO_HOURS),
    )
    .await?;
    txn.commit().await?;

    ctx.runner.handle_db_tick().await?;

    let valid_poc = ctx.valid_pocs.receive_valid_poc().await;
    assert_eq!(0, valid_poc.selected_witnesses.len());
    assert_eq!(1, valid_poc.unselected_witnesses.len());
    let valid_beacon = valid_poc.beacon_report.unwrap().report.clone().unwrap();
    let invalid_witness_report = valid_poc.unselected_witnesses[0].clone();
    // assert the pubkeys in the outputted reports
    // match those which we injected
    assert_eq!(
        PublicKeyBinary::from(valid_beacon.pub_key.clone()),
        PublicKeyBinary::from_str(common::BEACONER3).unwrap()
    );
    assert_eq!(
        PublicKeyBinary::from(invalid_witness_report.report.unwrap().pub_key.clone()),
        PublicKeyBinary::from_str(common::NO_METADATA_GATEWAY1).unwrap()
    );
    // assert the witness reports status
    assert_eq!(
        VerificationStatus::Invalid as i32,
        invalid_witness_report.status
    );
    // assert the invalid details
    assert_eq!(
        InvalidReason::NotAsserted as i32,
        invalid_witness_report.invalid_reason
    );
    assert_eq!(
        InvalidParticipantSide::Witness as i32,
        invalid_witness_report.participant_side
    );

    Ok(())
}

#[sqlx::test]
async fn invalid_beacon_no_gateway_found(pool: PgPool) -> anyhow::Result<()> {
    let mut ctx = TestContext::setup(pool.clone(), *BEACON_INTERVAL).await?;
    let now = Utc::now();
    //
    // test with an invalid beacon & 1 witness
    // beacon is invalid as GW is unknown
    //
    let beacon_to_inject =
        common::create_valid_beacon_report(common::UNKNOWN_GATEWAY1, ctx.entropy_ts);
    let witness_to_inject = common::create_valid_witness_report(common::WITNESS1, ctx.entropy_ts);
    common::inject_beacon_report(pool.clone(), beacon_to_inject.clone()).await?;
    common::inject_witness_report(pool.clone(), witness_to_inject.clone()).await?;

    let mut txn = pool.begin().await?;
    common::inject_last_beacon(
        &mut txn,
        witness_to_inject.report.pub_key.clone(),
        now - chrono::Duration::hours(1),
    )
    .await?;
    common::inject_last_witness(
        &mut txn,
        witness_to_inject.report.pub_key.clone(),
        now - chrono::Duration::hours(1),
    )
    .await?;
    txn.commit().await?;

    ctx.runner.handle_db_tick().await?;

    let invalid_beacon_report = ctx.invalid_beacons.receive_invalid_beacon().await;
    let invalid_witness_report = ctx.invalid_witnesses.receive_invalid_witness().await;
    let invalid_beacon = invalid_beacon_report.report.clone().unwrap();
    let invalid_witness = invalid_witness_report.clone().report.unwrap();
    // assert the pubkeys in the outputted reports
    // match those which we injected
    assert_eq!(
        PublicKeyBinary::from(invalid_beacon.pub_key.clone()),
        PublicKeyBinary::from_str(common::UNKNOWN_GATEWAY1).unwrap()
    );
    assert_eq!(
        PublicKeyBinary::from(invalid_witness.pub_key.clone()),
        PublicKeyBinary::from_str(common::WITNESS1).unwrap()
    );
    // assert the invalid details
    assert_eq!(
        InvalidReason::GatewayNotFound as i32,
        invalid_beacon_report.reason
    );
    assert_eq!(
        InvalidReason::GatewayNotFound as i32,
        invalid_witness_report.reason
    );
    assert_eq!(
        InvalidParticipantSide::Beaconer as i32,
        invalid_witness_report.participant_side
    );

    Ok(())
}

#[sqlx::test]
async fn invalid_beacon_gateway_not_found_no_witnesses(pool: PgPool) -> anyhow::Result<()> {
    let mut ctx = TestContext::setup(pool.clone(), *BEACON_INTERVAL).await?;
    //
    // test with an invalid beacon, no witnesses
    // beacon is invalid as GW is unknown
    //
    let beacon_to_inject =
        common::create_valid_beacon_report(common::UNKNOWN_GATEWAY1, ctx.entropy_ts);
    common::inject_beacon_report(pool.clone(), beacon_to_inject.clone()).await?;
    ctx.runner.handle_db_tick().await?;

    let invalid_beacon_report = ctx.invalid_beacons.receive_invalid_beacon().await;
    // assert the pubkeys in the outputted reports
    // match those which we injected
    assert_eq!(
        PublicKeyBinary::from(invalid_beacon_report.report.unwrap().pub_key),
        PublicKeyBinary::from_str(common::UNKNOWN_GATEWAY1).unwrap()
    );
    // assert the invalid details
    assert_eq!(
        InvalidReason::GatewayNotFound as i32,
        invalid_beacon_report.reason
    );

    Ok(())
}

#[sqlx::test]
async fn invalid_beacon_bad_payload(pool: PgPool) -> anyhow::Result<()> {
    let mut ctx = TestContext::setup(pool.clone(), *BEACON_INTERVAL).await?;
    //
    // test with an invalid beacon, no witnesses
    // the beacon will have an invalid payload, resulting in an error
    // which will result in the DB attempts value being incremented
    // when it exceeds max retries it will no longer be selected
    // for processing by the runner
    //
    let beacon_to_inject = common::create_valid_beacon_report(common::BEACONER4, ctx.entropy_ts);
    common::inject_invalid_beacon_report(pool.clone(), beacon_to_inject).await?;
    ctx.runner.handle_db_tick().await?;
    ctx.runner.handle_db_tick().await?;
    ctx.runner.handle_db_tick().await?;
    ctx.runner.handle_db_tick().await?;
    ctx.runner.handle_db_tick().await?;
    tokio::time::sleep(Duration::from_secs(3)).await;
    let mut txn = pool.begin().await?;
    let beacon_report = Report::get_stale_beacons(&mut txn, Duration::from_secs(1)).await?;
    // max attempts is 2, once that is exceeded the report is no longer retried
    // so even tho we called handle_db_tick 5 times above, the report was only retried twice
    assert_eq!(2, beacon_report[0].attempts);
    // confirm no outputs to filestore, the report will remain in the DB
    // until the purger removes it
    ctx.valid_pocs.assert_no_messages();
    ctx.invalid_beacons.assert_no_messages();
    ctx.invalid_witnesses.assert_no_messages();

    Ok(())
}

#[sqlx::test]
async fn valid_beacon_and_witness_no_beacon_reciprocity(pool: PgPool) -> anyhow::Result<()> {
    let mut ctx = TestContext::setup(pool.clone(), *BEACON_INTERVAL).await?;
    let now = ctx.entropy_ts;

    // test with a valid beacon and a valid witness
    let beacon_to_inject = common::create_valid_beacon_report(common::BEACONER1, ctx.entropy_ts);
    let witness_to_inject = common::create_valid_witness_report(common::WITNESS1, ctx.entropy_ts);
    common::inject_beacon_report(pool.clone(), beacon_to_inject.clone()).await?;
    common::inject_witness_report(pool.clone(), witness_to_inject.clone()).await?;

    // inject last beacons and witness reports into the DB
    // we will only insert these for the witness
    // the beaconer will fail the reciprocity check
    let mut txn = pool.begin().await?;
    common::inject_last_beacon(
        &mut txn,
        witness_to_inject.report.pub_key.clone(),
        now - (*BEACON_INTERVAL_PLUS_TWO_HOURS),
    )
    .await?;
    common::inject_last_witness(
        &mut txn,
        witness_to_inject.report.pub_key.clone(),
        now - (*BEACON_INTERVAL_PLUS_TWO_HOURS),
    )
    .await?;
    txn.commit().await?;

    ctx.runner.handle_db_tick().await?;

    let invalid_beacon = ctx.invalid_beacons.receive_invalid_beacon().await;
    let invalid_witness = ctx.invalid_witnesses.receive_invalid_witness().await;
    // assert the pubkeys in the outputted reports
    // match those which we injected
    assert_eq!(
        PublicKeyBinary::from(invalid_beacon.report.as_ref().unwrap().pub_key.clone()),
        PublicKeyBinary::from_str(common::BEACONER1).unwrap()
    );
    assert_eq!(
        PublicKeyBinary::from(invalid_witness.report.as_ref().unwrap().pub_key.clone()),
        PublicKeyBinary::from_str(common::WITNESS1).unwrap()
    );
    // assert the invalid details
    assert_eq!(
        InvalidReason::GatewayNoValidWitnesses as i32,
        invalid_beacon.reason
    );
    assert_eq!(
        InvalidReason::GatewayNoValidWitnesses as i32,
        invalid_witness.reason
    );
    assert_eq!(
        InvalidParticipantSide::Beaconer as i32,
        invalid_witness.participant_side
    );
    Ok(())
}

#[sqlx::test]
async fn valid_beacon_and_witness_no_witness_reciprocity(pool: PgPool) -> anyhow::Result<()> {
    let mut ctx = TestContext::setup(pool.clone(), *BEACON_INTERVAL).await?;
    let now = ctx.entropy_ts;

    // test with a valid beacon and a valid witness
    let beacon_to_inject = common::create_valid_beacon_report(common::BEACONER1, ctx.entropy_ts);
    let witness_to_inject = common::create_valid_witness_report(common::WITNESS1, ctx.entropy_ts);
    common::inject_beacon_report(pool.clone(), beacon_to_inject.clone()).await?;
    common::inject_witness_report(pool.clone(), witness_to_inject.clone()).await?;

    // inject last beacons and witness reports into the DB
    // we will only insert these for the witness
    // the beaconer will fail the reciprocity check
    let mut txn = pool.begin().await?;
    common::inject_last_beacon(
        &mut txn,
        beacon_to_inject.report.pub_key.clone(),
        now - (*BEACON_INTERVAL_PLUS_TWO_HOURS),
    )
    .await?;
    common::inject_last_witness(
        &mut txn,
        beacon_to_inject.report.pub_key.clone(),
        now - (*BEACON_INTERVAL_PLUS_TWO_HOURS),
    )
    .await?;
    txn.commit().await?;

    ctx.runner.handle_db_tick().await?;

    let valid_poc = ctx.valid_pocs.receive_valid_poc().await;
    println!("{:?}", valid_poc);
    assert_eq!(0, valid_poc.selected_witnesses.len());
    assert_eq!(1, valid_poc.unselected_witnesses.len());
    let valid_beacon = valid_poc.beacon_report.unwrap().report.clone().unwrap();
    let invalid_witness_report = valid_poc.unselected_witnesses[0].clone();
    // assert the pubkeys in the outputted reports
    // match those which we injected
    assert_eq!(
        PublicKeyBinary::from(valid_beacon.pub_key.clone()),
        PublicKeyBinary::from_str(common::BEACONER1).unwrap()
    );
    assert_eq!(
        PublicKeyBinary::from(invalid_witness_report.report.unwrap().pub_key.clone()),
        PublicKeyBinary::from_str(common::WITNESS1).unwrap()
    );
    // assert the witness reports status
    assert_eq!(
        VerificationStatus::Invalid as i32,
        invalid_witness_report.status
    );
    // assert the invalid details
    assert_eq!(
        InvalidReason::GatewayNoValidBeacons as i32,
        invalid_witness_report.invalid_reason
    );
    assert_eq!(
        InvalidParticipantSide::Witness as i32,
        invalid_witness_report.participant_side
    );
    Ok(())
}

#[sqlx::test]
async fn valid_new_gateway_witness_first_reciprocity(pool: PgPool) -> anyhow::Result<()> {
    let test_beacon_interval = Duration::from_secs(5);
    let mut ctx = TestContext::setup(pool.clone(), test_beacon_interval).await?;
    let now = ctx.entropy_ts;

    // simulate a new gateway coming online or a gateway coming online after an extended period offline
    // the gateway uses beaconer5 pubkey
    // the gateways first activity will be to submit a witness report for a beacon it sees
    // this will fail the reciprocity check as there will be no previous/recent beacon from this gateway
    // whilst the witness will fail reciprocity, its last valid witness timestamp will be updated
    // as all regular ( ie non reciprocity check ) validations passed
    // the gateway will then subsequently beacon which will pass ( reciprocity check is ok as previously witnessed )
    // it will then witness again and this time it will pass  ( reciprocity check is ok as previously beaconed )

    //
    // step 1 - generate a witness from beaconer5,
    //          this witness will be valid but will fail reciprocity check as no previous beacon
    //          last witness timestamp will be updated
    //

    let beacon_to_inject = common::create_valid_beacon_report(common::BEACONER1, ctx.entropy_ts);
    let witness_to_inject = common::create_valid_witness_report(common::BEACONER5, ctx.entropy_ts);
    common::inject_beacon_report(pool.clone(), beacon_to_inject.clone()).await?;
    common::inject_witness_report(pool.clone(), witness_to_inject.clone()).await?;

    // inject last beacons and witness reports into the DB for beaconer 1
    let mut txn = pool.begin().await?;
    common::inject_last_beacon(
        &mut txn,
        beacon_to_inject.report.pub_key.clone(),
        now - (test_beacon_interval + Duration::from_secs(10)),
    )
    .await?;
    common::inject_last_witness(
        &mut txn,
        beacon_to_inject.report.pub_key.clone(),
        now - (test_beacon_interval + Duration::from_secs(10)),
    )
    .await?;
    txn.commit().await?;

    ctx.runner.handle_db_tick().await?;

    let valid_poc = ctx.valid_pocs.receive_valid_poc().await;
    println!("{:?}", valid_poc);
    assert_eq!(0, valid_poc.selected_witnesses.len());
    assert_eq!(1, valid_poc.unselected_witnesses.len());
    let valid_beacon = valid_poc.beacon_report.unwrap().report.clone().unwrap();
    let invalid_witness_report = valid_poc.unselected_witnesses[0].clone();
    let invalid_witness = invalid_witness_report.report.unwrap();
    // assert the pubkeys in the outputted reports
    // match those which we injected
    assert_eq!(
        PublicKeyBinary::from(valid_beacon.pub_key.clone()),
        PublicKeyBinary::from_str(common::BEACONER1).unwrap()
    );
    assert_eq!(
        PublicKeyBinary::from(invalid_witness.pub_key.clone()),
        PublicKeyBinary::from_str(common::BEACONER5).unwrap()
    );
    // assert the witness reports status
    assert_eq!(
        VerificationStatus::Invalid as i32,
        invalid_witness_report.status
    );
    // assert the invalid details
    assert_eq!(
        InvalidReason::GatewayNoValidBeacons as i32,
        invalid_witness_report.invalid_reason
    );
    assert_eq!(
        InvalidParticipantSide::Witness as i32,
        invalid_witness_report.participant_side
    );

    //
    // step 2
    // generate a beacon from beaconer5
    // as it will now have a previous valid witness, the beacon will pass the reciprocity check
    // the gateway will then have a valid 'last beacon' and 'last witness' timestamp
    //

    let beacon_to_inject = common::create_valid_beacon_report(common::BEACONER5, ctx.entropy_ts);
    let witness_to_inject = common::create_valid_witness_report(common::BEACONER1, ctx.entropy_ts);
    common::inject_beacon_report(pool.clone(), beacon_to_inject.clone()).await?;
    common::inject_witness_report(pool.clone(), witness_to_inject.clone()).await?;

    ctx.runner.handle_db_tick().await?;

    let valid_poc = ctx.valid_pocs.receive_valid_poc().await;
    println!("{:?}", valid_poc);
    assert_eq!(1, valid_poc.selected_witnesses.len());
    assert_eq!(0, valid_poc.unselected_witnesses.len());
    let valid_beacon = valid_poc.beacon_report.unwrap().report.clone().unwrap();
    let valid_witness_report = valid_poc.selected_witnesses[0].clone();
    let valid_witness = valid_witness_report.report.unwrap();
    // assert the pubkeys in the outputted reports
    // match those which we injected
    assert_eq!(
        PublicKeyBinary::from(valid_beacon.pub_key.clone()),
        PublicKeyBinary::from_str(common::BEACONER5).unwrap()
    );
    assert_eq!(
        PublicKeyBinary::from(valid_witness.pub_key.clone()),
        PublicKeyBinary::from_str(common::BEACONER1).unwrap()
    );
    // assert the witness reports status
    assert_eq!(
        VerificationStatus::Valid as i32,
        valid_witness_report.status
    );
    //
    // step 3
    // generate a witness from beaconer5
    // as the gateway will have both a valid 'last beacon' and 'last witness' timestamp
    // the reciprocity check will pass
    //

    let beacon_to_inject = common::create_valid_beacon_report(common::BEACONER2, ctx.entropy_ts);
    let witness_to_inject = common::create_valid_witness_report(common::BEACONER5, ctx.entropy_ts);
    common::inject_beacon_report(pool.clone(), beacon_to_inject.clone()).await?;
    common::inject_witness_report(pool.clone(), witness_to_inject.clone()).await?;

    // inject last beacons and witness reports into the DB for beaconer 1
    // avoid the reports declared invalid due to reciprocity check
    let mut txn = pool.begin().await?;
    common::inject_last_beacon(
        &mut txn,
        beacon_to_inject.report.pub_key.clone(),
        now - (test_beacon_interval + Duration::from_secs(10)),
    )
    .await?;
    common::inject_last_witness(
        &mut txn,
        beacon_to_inject.report.pub_key.clone(),
        now - (test_beacon_interval + Duration::from_secs(10)),
    )
    .await?;
    txn.commit().await?;

    ctx.runner.handle_db_tick().await?;

    let valid_poc = ctx.valid_pocs.receive_valid_poc().await;
    println!("{:?}", valid_poc);
    assert_eq!(1, valid_poc.selected_witnesses.len());
    assert_eq!(0, valid_poc.unselected_witnesses.len());
    let valid_beacon = valid_poc.beacon_report.unwrap().report.clone().unwrap();
    let valid_witness_report = valid_poc.selected_witnesses[0].clone();
    let valid_witness = valid_witness_report.report.unwrap();
    // assert the pubkeys in the outputted reports
    // match those which we injected
    assert_eq!(
        PublicKeyBinary::from(valid_beacon.pub_key.clone()),
        PublicKeyBinary::from_str(common::BEACONER2).unwrap()
    );
    assert_eq!(
        PublicKeyBinary::from(valid_witness.pub_key.clone()),
        PublicKeyBinary::from_str(common::BEACONER5).unwrap()
    );
    // assert the witness reports status
    assert_eq!(
        VerificationStatus::Valid as i32,
        valid_witness_report.status
    );
    Ok(())
}

#[sqlx::test]
async fn valid_new_gateway_beacon_first_reciprocity(pool: PgPool) -> anyhow::Result<()> {
    let test_beacon_interval = Duration::from_secs(5);
    let mut ctx = TestContext::setup(pool.clone(), test_beacon_interval).await?;
    let now = ctx.entropy_ts;

    // simulate a new gateway coming online or a gateway coming online after an extended period offline
    // the gateway uses beaconer5 pubkey
    // the gateways first activity will be to submit a beacon report
    // this will fail the reciprocity check due to no previous/recent witness from this gateway
    // whilst the beacon will fail reciprocity, its last valid beacon timestamp will be updated
    // as all regular ( ie non reciprocity check ) validations passed
    // the gateway will then subsequently witness which will pass ( reciprocity check is ok as previously beacon )
    // it will then beacon again and this time it will pass  ( reciprocity check is ok as previously witness )

    //
    // step 1 - generate a beacon from beaconer5,
    //          this beacon will be valid but will fail reciprocity check as no previous witness
    //          last beacon timestamp will be updated
    //
    let beacon_to_inject = common::create_valid_beacon_report(common::BEACONER5, ctx.entropy_ts);
    let witness_to_inject = common::create_valid_witness_report(common::WITNESS1, ctx.entropy_ts);
    common::inject_beacon_report(pool.clone(), beacon_to_inject.clone()).await?;
    common::inject_witness_report(pool.clone(), witness_to_inject.clone()).await?;

    ctx.runner.handle_db_tick().await?;

    let invalid_beacon = ctx.invalid_beacons.receive_invalid_beacon().await;
    let invalid_beacon_report = invalid_beacon.report.clone().unwrap();
    println!("{:?}", invalid_beacon);
    // assert the pubkeys in the outputted reports
    // match those which we injected
    assert_eq!(
        PublicKeyBinary::from(invalid_beacon_report.pub_key.clone()),
        PublicKeyBinary::from_str(common::BEACONER5).unwrap()
    );
    // assert the invalid details
    assert_eq!(
        InvalidReason::GatewayNoValidWitnesses as i32,
        invalid_beacon.reason
    );

    //
    // step 2
    // generate a witness from beaconer5
    // as it will now have a previous valid beacon, the witness will pass the reciprocity check
    // the gateway will then have a valid 'last beacon' and 'last witness' timestamp
    //

    let beacon_to_inject = common::create_valid_beacon_report(common::BEACONER1, ctx.entropy_ts);
    let witness_to_inject = common::create_valid_witness_report(common::BEACONER5, ctx.entropy_ts);
    common::inject_beacon_report(pool.clone(), beacon_to_inject.clone()).await?;
    common::inject_witness_report(pool.clone(), witness_to_inject.clone()).await?;

    // inject last beacons and witness reports into the DB for beaconer 1
    let mut txn = pool.begin().await?;
    common::inject_last_beacon(
        &mut txn,
        beacon_to_inject.report.pub_key.clone(),
        now - (test_beacon_interval + Duration::from_secs(10)),
    )
    .await?;
    common::inject_last_witness(
        &mut txn,
        beacon_to_inject.report.pub_key.clone(),
        now - (test_beacon_interval + Duration::from_secs(10)),
    )
    .await?;
    txn.commit().await?;

    ctx.runner.handle_db_tick().await?;

    let valid_poc = ctx.valid_pocs.receive_valid_poc().await;
    println!("{:?}", valid_poc);
    assert_eq!(1, valid_poc.selected_witnesses.len());
    assert_eq!(0, valid_poc.unselected_witnesses.len());
    let valid_beacon = valid_poc.beacon_report.unwrap().report.clone().unwrap();
    let valid_witness_report = valid_poc.selected_witnesses[0].clone();
    let valid_witness = valid_witness_report.report.unwrap();
    // assert the pubkeys in the outputted reports
    // match those which we injected
    assert_eq!(
        PublicKeyBinary::from(valid_beacon.pub_key.clone()),
        PublicKeyBinary::from_str(common::BEACONER1).unwrap()
    );
    assert_eq!(
        PublicKeyBinary::from(valid_witness.pub_key.clone()),
        PublicKeyBinary::from_str(common::BEACONER5).unwrap()
    );
    // assert the witness reports status
    assert_eq!(
        VerificationStatus::Valid as i32,
        valid_witness_report.status
    );

    //
    // step 3
    // generate a beacon from beaconer5
    // as the gateway will now have both a valid 'last beacon' and 'last witness' timestamp
    // the reciprocity check will pass
    //
    tokio::time::sleep(Duration::from_secs(6)).await;
    let beacon_to_inject = common::create_valid_beacon_report(
        common::BEACONER5,
        ctx.entropy_ts + Duration::from_secs(5),
    );
    let witness_to_inject = common::create_valid_witness_report(common::WITNESS2, ctx.entropy_ts);
    common::inject_beacon_report(pool.clone(), beacon_to_inject.clone()).await?;
    common::inject_witness_report(pool.clone(), witness_to_inject.clone()).await?;

    // inject last beacons and witness reports into the DB for witness 2
    let mut txn = pool.begin().await?;
    common::inject_last_beacon(
        &mut txn,
        witness_to_inject.report.pub_key.clone(),
        now - (test_beacon_interval + Duration::from_secs(10)),
    )
    .await?;
    common::inject_last_witness(
        &mut txn,
        witness_to_inject.report.pub_key.clone(),
        now - (test_beacon_interval + Duration::from_secs(10)),
    )
    .await?;
    txn.commit().await?;

    ctx.runner.handle_db_tick().await?;

    let valid_poc = ctx.valid_pocs.receive_valid_poc().await;
    println!("{:?}", valid_poc);
    assert_eq!(1, valid_poc.selected_witnesses.len());
    assert_eq!(0, valid_poc.unselected_witnesses.len());
    let valid_beacon = valid_poc.beacon_report.unwrap().report.clone().unwrap();
    let valid_witness_report = valid_poc.selected_witnesses[0].clone();
    let valid_witness = valid_witness_report.report.unwrap();

    // assert the pubkeys in the outputted reports
    // match those which we injected
    assert_eq!(
        PublicKeyBinary::from(valid_beacon.pub_key.clone()),
        PublicKeyBinary::from_str(common::BEACONER5).unwrap()
    );
    assert_eq!(
        PublicKeyBinary::from(valid_witness.pub_key.clone()),
        PublicKeyBinary::from_str(common::WITNESS2).unwrap()
    );
    // assert the witness reports status
    assert_eq!(
        VerificationStatus::Valid as i32,
        valid_witness_report.status
    );
    Ok(())
}

#[sqlx::test]
async fn valid_lone_wolf_beacon(pool: PgPool) -> anyhow::Result<()> {
    let test_beacon_interval = Duration::from_secs(5);
    let mut ctx = TestContext::setup(pool.clone(), test_beacon_interval).await?;
    let now = ctx.entropy_ts;

    // simulate a lone wolf gateway, broadcasting and no one around to hear it
    // the gateway uses beaconer1 pubkey

    //
    // step 1 - generate a beacon from beaconer1,
    //          this beacon will be valid but will fail reciprocity check as the gateway has not previously witnessed
    //          last beacon timestamp will NOT be updated as there are no witnesses to the beacon
    //
    let beacon_to_inject = common::create_valid_beacon_report(common::BEACONER1, ctx.entropy_ts);
    common::inject_beacon_report(pool.clone(), beacon_to_inject.clone()).await?;

    ctx.runner.handle_db_tick().await?;

    let invalid_beacon = ctx.invalid_beacons.receive_invalid_beacon().await;
    let invalid_beacon_report = invalid_beacon.report.clone().unwrap();
    println!("{:?}", invalid_beacon);
    // assert the pubkeys in the outputted reports
    // match those which we injected
    assert_eq!(
        PublicKeyBinary::from(invalid_beacon_report.pub_key.clone()),
        PublicKeyBinary::from_str(common::BEACONER1).unwrap()
    );
    // assert the invalid details
    assert_eq!(
        InvalidReason::GatewayNoValidWitnesses as i32,
        invalid_beacon.reason
    );

    //
    // step 2
    // confirm beaconer1's last beacon timestamp was not updated
    // the gateway should not be able to witness a beacon and will fail reciprocity check

    // generate a beacon from beaconer5 and have it witnessed by beaconer1
    // the beacon will be successful but the witness will fail reciprocity check
    // as beaconer1 does not have a current last beacon timestamp
    // beaconer1's last witness timestamp will be updated ( as its witness was structurally valid )
    //

    let beacon_to_inject = common::create_valid_beacon_report(common::BEACONER5, ctx.entropy_ts);
    let witness_to_inject = common::create_valid_witness_report(common::BEACONER1, ctx.entropy_ts);
    common::inject_beacon_report(pool.clone(), beacon_to_inject.clone()).await?;
    common::inject_witness_report(pool.clone(), witness_to_inject.clone()).await?;

    // inject last beacon & witness timestamps into the DB for beaconer 5
    // allow it to pass reciprocity checks
    let mut txn = pool.begin().await?;
    common::inject_last_beacon(
        &mut txn,
        beacon_to_inject.report.pub_key.clone(),
        now - (test_beacon_interval + Duration::from_secs(10)),
    )
    .await?;
    common::inject_last_witness(
        &mut txn,
        beacon_to_inject.report.pub_key.clone(),
        now - (test_beacon_interval + Duration::from_secs(10)),
    )
    .await?;
    txn.commit().await?;

    ctx.runner.handle_db_tick().await?;

    let valid_poc = ctx.valid_pocs.receive_valid_poc().await;
    println!("{:?}", valid_poc);
    assert_eq!(0, valid_poc.selected_witnesses.len());
    assert_eq!(1, valid_poc.unselected_witnesses.len());
    let valid_beacon = valid_poc.beacon_report.unwrap().report.clone().unwrap();
    let invalid_witness_report = valid_poc.unselected_witnesses[0].clone();
    let invalid_witness = invalid_witness_report.report.unwrap();
    // assert the pubkeys in the outputted reports
    // match those which we injected
    assert_eq!(
        PublicKeyBinary::from(valid_beacon.pub_key.clone()),
        PublicKeyBinary::from_str(common::BEACONER5).unwrap()
    );
    assert_eq!(
        PublicKeyBinary::from(invalid_witness.pub_key.clone()),
        PublicKeyBinary::from_str(common::BEACONER1).unwrap()
    );
    // assert the witness reports status
    assert_eq!(
        VerificationStatus::Invalid as i32,
        invalid_witness_report.status
    );
    assert_eq!(
        InvalidReason::GatewayNoValidBeacons as i32,
        invalid_witness_report.invalid_reason
    );
    assert_eq!(
        InvalidParticipantSide::Witness as i32,
        invalid_witness_report.participant_side
    );
    Ok(())
}

#[sqlx::test]
async fn valid_two_isolated_gateways_beaconing_and_witnessing(pool: PgPool) -> anyhow::Result<()> {
    let test_beacon_interval = Duration::from_secs(5);
    let mut ctx = TestContext::setup(pool.clone(), test_beacon_interval).await?;

    // simulate two gateways with no recent activity coming online and
    // witnessing each others beacons

    //
    // step 1
    // generate a beacon from gateway1 with gateway 2 as witness
    // this beacon will be valid but will fail reciprocity check as no previous witness
    // gateway 1's last beacon timestamp will be updated
    // gateway 2's last witness timestamp will be updated
    //
    let beacon_to_inject = common::create_valid_beacon_report(common::BEACONER1, ctx.entropy_ts);
    let witness_to_inject = common::create_valid_witness_report(common::WITNESS1, ctx.entropy_ts);
    common::inject_beacon_report(pool.clone(), beacon_to_inject.clone()).await?;
    common::inject_witness_report(pool.clone(), witness_to_inject.clone()).await?;

    ctx.runner.handle_db_tick().await?;

    let invalid_beacon = ctx.invalid_beacons.receive_invalid_beacon().await;
    let invalid_beacon_report = invalid_beacon.report.clone().unwrap();
    println!("{:?}", invalid_beacon);
    // assert the pubkeys in the outputted reports
    // match those which we injected
    assert_eq!(
        PublicKeyBinary::from(invalid_beacon_report.pub_key.clone()),
        PublicKeyBinary::from_str(common::BEACONER1).unwrap()
    );
    // assert the invalid details
    assert_eq!(
        InvalidReason::GatewayNoValidWitnesses as i32,
        invalid_beacon.reason
    );

    //
    // step 2
    // generate a beacon from gateway2 with gateway 1 as witness,
    // this beacon will be valid and will pass reciprocity check as the gateway 2 witnessed previously
    // gateway 2's last beacon timestamp will be updated
    // gateway 1's last witness timestamp will be updated
    //

    let beacon_to_inject = common::create_valid_beacon_report(common::WITNESS1, ctx.entropy_ts);
    let witness_to_inject = common::create_valid_witness_report(common::BEACONER1, ctx.entropy_ts);
    common::inject_beacon_report(pool.clone(), beacon_to_inject.clone()).await?;
    common::inject_witness_report(pool.clone(), witness_to_inject.clone()).await?;

    ctx.runner.handle_db_tick().await?;

    let valid_poc = ctx.valid_pocs.receive_valid_poc().await;
    println!("{:?}", valid_poc);
    assert_eq!(1, valid_poc.selected_witnesses.len());
    assert_eq!(0, valid_poc.unselected_witnesses.len());
    let valid_beacon = valid_poc.beacon_report.unwrap().report.clone().unwrap();
    let valid_witness_report = valid_poc.selected_witnesses[0].clone();
    let valid_witness = valid_witness_report.report.unwrap();
    // assert the pubkeys in the outputted reports
    // match those which we injected
    assert_eq!(
        PublicKeyBinary::from(valid_beacon.pub_key.clone()),
        PublicKeyBinary::from_str(common::WITNESS1).unwrap()
    );
    assert_eq!(
        PublicKeyBinary::from(valid_witness.pub_key.clone()),
        PublicKeyBinary::from_str(common::BEACONER1).unwrap()
    );
    // assert the witness reports status
    assert_eq!(
        VerificationStatus::Valid as i32,
        valid_witness_report.status
    );

    //
    // step 3
    // generate a beacon from gateway1 with gateway 2 as witness,
    // this beacon will be valid and pass reciprocity check as gateway 1 previously witnessed
    // gateway 2 has previously beaconed and so its witness will also be valid
    //

    let beacon_to_inject = common::create_valid_beacon_report(
        common::BEACONER1,
        ctx.entropy_ts + chrono::Duration::seconds(5),
    );
    let witness_to_inject = common::create_valid_witness_report(
        common::WITNESS1,
        ctx.entropy_ts + chrono::Duration::seconds(5),
    );
    common::inject_beacon_report(pool.clone(), beacon_to_inject.clone()).await?;
    common::inject_witness_report(pool.clone(), witness_to_inject.clone()).await?;

    ctx.runner.handle_db_tick().await?;

    let valid_poc = ctx.valid_pocs.receive_valid_poc().await;
    println!("{:?}", valid_poc);
    assert_eq!(1, valid_poc.selected_witnesses.len());
    assert_eq!(0, valid_poc.unselected_witnesses.len());
    let valid_beacon = valid_poc.beacon_report.unwrap().report.clone().unwrap();
    let valid_witness_report = valid_poc.selected_witnesses[0].clone();
    let valid_witness = valid_witness_report.report.unwrap();
    // assert the pubkeys in the outputted reports
    // match those which we injected
    assert_eq!(
        PublicKeyBinary::from(valid_beacon.pub_key.clone()),
        PublicKeyBinary::from_str(common::BEACONER1).unwrap()
    );
    assert_eq!(
        PublicKeyBinary::from(valid_witness.pub_key.clone()),
        PublicKeyBinary::from_str(common::WITNESS1).unwrap()
    );
    // assert the witness reports status
    assert_eq!(
        VerificationStatus::Valid as i32,
        valid_witness_report.status
    );

    Ok(())
}
