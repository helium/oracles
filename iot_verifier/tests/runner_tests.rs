mod common;
use async_trait::async_trait;
use chrono::{DateTime, Duration as ChronoDuration, TimeZone, Utc};
use common::MockFileSinkReceiver;
use denylist::DenyList;
use futures_util::{stream, StreamExt as FuturesStreamExt};
use helium_crypto::PublicKeyBinary;
use helium_proto::services::poc_lora::{
    InvalidParticipantSide, InvalidReason, LoraBeaconReportReqV1, LoraWitnessReportReqV1,
};
use helium_proto::Region as ProtoRegion;
use iot_config::{
    client::{Gateways, RegionParamsInfo},
    gateway_info::{GatewayInfo, GatewayInfoStream},
};
use iot_verifier::{
    gateway_cache::GatewayCache, gateway_updater::GatewayUpdater, poc_report::Report,
    region_cache::RegionCache, runner::Runner, tx_scaler::Server as DensityScaler,
};
use sqlx::PgPool;
use std::{self, str::FromStr, time::Duration};

#[derive(Debug, Clone)]
pub struct MockIotConfigClient {
    resolve_gateway: GatewayInfo,
    stream_gateways: Vec<GatewayInfo>,
    region_params: RegionParamsInfo,
}

#[async_trait]
impl Gateways for MockIotConfigClient {
    type Error = anyhow::Error;

    async fn resolve_gateway_info(
        &mut self,
        _address: &PublicKeyBinary,
    ) -> Result<Option<GatewayInfo>, Self::Error> {
        Ok(Some(self.resolve_gateway.clone()))
    }

    async fn stream_gateways_info(&mut self) -> Result<GatewayInfoStream, Self::Error> {
        let stream = stream::iter(self.stream_gateways.clone()).boxed();
        Ok(stream)
    }

    async fn resolve_region_params(
        &mut self,
        _region: ProtoRegion,
    ) -> Result<RegionParamsInfo, Self::Error> {
        Ok(self.region_params.clone())
    }
}

struct TestContext {
    runner: Runner<MockIotConfigClient>,
    valid_pocs: MockFileSinkReceiver,
    invalid_beacons: MockFileSinkReceiver,
    invalid_witnesses: MockFileSinkReceiver,
    entropy_ts: DateTime<Utc>,
}

impl TestContext {
    async fn setup(pool: PgPool) -> anyhow::Result<Self> {
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
        let refresh_interval = ChronoDuration::seconds(30);
        let (gateway_updater_receiver, _gateway_updater_server) =
            GatewayUpdater::new(refresh_interval, iot_config_client.clone()).await?;
        let gateway_cache = GatewayCache::new(gateway_updater_receiver.clone());
        let density_scaler =
            DensityScaler::new(refresh_interval, pool.clone(), gateway_updater_receiver).await?;
        let region_cache = RegionCache::new(Duration::from_secs(60), iot_config_client.clone())?;

        // create the runner
        let runner = Runner {
            pool: pool.clone(),
            beacon_interval: ChronoDuration::seconds(21600),
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
        };

        // generate a datetime based on a hardcoded timestamp
        // this is the time the entropy is valid from
        // and all beacon and witness reports will be created
        // with a received_ts based on an offset from this ts
        let entropy_ts = Utc.timestamp_millis_opt(common::ENTROPY_TIMESTAMP).unwrap();
        let report_ts = entropy_ts + ChronoDuration::minutes(1);
        // add the entropy to the DB
        common::inject_entropy_report(pool.clone(), entropy_ts).await?;

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
    let mut ctx = TestContext::setup(pool.clone()).await?;

    // test with a valid beacon and a valid witness
    //
    let beacon_to_inject = common::create_valid_beacon_report(common::BEACONER1, ctx.entropy_ts);
    let witness_to_inject = common::create_valid_witness_report(common::WITNESS1, ctx.entropy_ts);
    common::inject_beacon_report(pool.clone(), beacon_to_inject.clone()).await?;
    common::inject_witness_report(pool.clone(), witness_to_inject.clone()).await?;
    ctx.runner.handle_db_tick().await?;

    let valid_poc = ctx.valid_pocs.receive_valid_poc().await;
    assert_eq!(1, valid_poc.selected_witnesses.len());
    assert_eq!(0, valid_poc.unselected_witnesses.len());
    let valid_beacon = valid_poc.beacon_report.unwrap().report.clone().unwrap();
    let valid_witness = valid_poc.selected_witnesses[0].report.clone().unwrap();
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
async fn invalid_beacon_gateway_not_found(pool: PgPool) -> anyhow::Result<()> {
    let mut ctx = TestContext::setup(pool.clone()).await?;
    //
    // test with a valid beacon and an invalid witness
    // witness is invalid due to not found in iot config
    //
    let beacon_to_inject = common::create_valid_beacon_report(common::BEACONER2, ctx.entropy_ts);
    let witness_to_inject =
        common::create_valid_witness_report(common::UNKNOWN_GATEWAY1, ctx.entropy_ts);
    common::inject_beacon_report(pool.clone(), beacon_to_inject.clone()).await?;
    common::inject_witness_report(pool.clone(), witness_to_inject.clone()).await?;
    ctx.runner.handle_db_tick().await?;

    let valid_poc = ctx.valid_pocs.receive_valid_poc().await;
    assert_eq!(0, valid_poc.selected_witnesses.len());
    assert_eq!(1, valid_poc.unselected_witnesses.len());
    let valid_beacon = valid_poc.beacon_report.unwrap().report.clone().unwrap();
    let invalid_witness_report = valid_poc.unselected_witnesses[0].clone();
    let invalid_witness = invalid_witness_report.report.clone().unwrap();
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
async fn invalid_witness_no_metadata(pool: PgPool) -> anyhow::Result<()> {
    let mut ctx = TestContext::setup(pool.clone()).await?;
    //
    // test with a valid beacon and an invalid witness
    // witness is invalid due no metadata in iot config
    //
    let beacon_to_inject = common::create_valid_beacon_report(common::BEACONER3, ctx.entropy_ts);
    let witness_to_inject =
        common::create_valid_witness_report(common::NO_METADATA_GATEWAY1, ctx.entropy_ts);
    common::inject_beacon_report(pool.clone(), beacon_to_inject.clone()).await?;
    common::inject_witness_report(pool.clone(), witness_to_inject.clone()).await?;
    ctx.runner.handle_db_tick().await?;

    let valid_poc = ctx.valid_pocs.receive_valid_poc().await;
    assert_eq!(0, valid_poc.selected_witnesses.len());
    assert_eq!(1, valid_poc.unselected_witnesses.len());
    let valid_beacon = valid_poc.beacon_report.unwrap().report.clone().unwrap();
    let invalid_witness_report = valid_poc.unselected_witnesses[0].clone();
    let invalid_witness = invalid_witness_report.report.clone().unwrap();
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
    // assert the invalid details
    assert_eq!(
        InvalidReason::NotAsserted as i32,
        invalid_witness_report.invalid_reason
    );
    assert_eq!(
        InvalidParticipantSide::Witness as i32,
        invalid_witness_report.participant_side
    );
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
async fn invalid_beacon_no_gateway_found(pool: PgPool) -> anyhow::Result<()> {
    let mut ctx = TestContext::setup(pool.clone()).await?;
    //
    // test with an invalid beacon & 1 witness
    // beacon is invalid as GW is unknown
    //
    let beacon_to_inject =
        common::create_valid_beacon_report(common::UNKNOWN_GATEWAY1, ctx.entropy_ts);
    let witness_to_inject = common::create_valid_witness_report(common::WITNESS1, ctx.entropy_ts);
    common::inject_beacon_report(pool.clone(), beacon_to_inject.clone()).await?;
    common::inject_witness_report(pool.clone(), witness_to_inject.clone()).await?;
    ctx.runner.handle_db_tick().await?;

    let invalid_beacon_report = ctx.invalid_beacons.receive_invalid_beacon().await;
    let invalid_witness_report = ctx.invalid_witnesses.receive_invalid_witness().await;
    let invalid_beacon = invalid_beacon_report.report.clone().unwrap();
    let invalid_witness = invalid_witness_report.report.clone().unwrap();
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
async fn invalid_beacon_gateway_not_found_no_witnesses(pool: PgPool) -> anyhow::Result<()> {
    let mut ctx = TestContext::setup(pool.clone()).await?;
    //
    // test with an invalid beacon, no witnesses
    // beacon is invalid as GW is unknown
    //
    let beacon_to_inject =
        common::create_valid_beacon_report(common::UNKNOWN_GATEWAY1, ctx.entropy_ts);
    common::inject_beacon_report(pool.clone(), beacon_to_inject.clone()).await?;
    ctx.runner.handle_db_tick().await?;

    let invalid_beacon_report = ctx.invalid_beacons.receive_invalid_beacon().await;
    let invalid_beacon = invalid_beacon_report.report.clone().unwrap();
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
    // assert the beacon report outputted to filestore
    // is unmodified from those submitted
    assert_eq!(
        invalid_beacon,
        LoraBeaconReportReqV1::from(beacon_to_inject.clone())
    );

    Ok(())
}

#[sqlx::test]
async fn invalid_beacon_bad_payload(pool: PgPool) -> anyhow::Result<()> {
    let mut ctx = TestContext::setup(pool.clone()).await?;
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
    let beacon_report = Report::get_stale_beacons(&mut txn, ChronoDuration::seconds(1)).await?;
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
