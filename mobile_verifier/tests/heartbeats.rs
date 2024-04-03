use chrono::{DateTime, Duration, Utc};
use file_store::coverage::RadioHexSignalLevel;
use futures_util::TryStreamExt;
use h3o::{CellIndex, LatLng};
use helium_crypto::PublicKeyBinary;
use helium_proto::services::poc_mobile::{CoverageObjectValidity, HeartbeatValidity, SignalLevel};
use mobile_verifier::{
    cell_type::CellType,
    coverage::{CoverageObject, CoverageObjectCache},
    geofence::GeofenceValidator,
    heartbeats::{HbType, Heartbeat, HeartbeatReward, LocationCache, ValidatedHeartbeat},
    GatewayResolution, GatewayResolver,
};
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use sqlx::PgPool;
use uuid::Uuid;

#[sqlx::test]
#[ignore]
async fn test_save_wifi_heartbeat(pool: PgPool) -> anyhow::Result<()> {
    let coverage_object = Uuid::new_v4();
    let heartbeat = ValidatedHeartbeat {
        heartbeat: Heartbeat {
            hb_type: HbType::Wifi,
            hotspot_key: "11eX55faMbqZB7jzN4p67m6w7ScPMH6ubnvCjCPLh72J49PaJEL"
                .parse()
                .unwrap(),
            cbsd_id: None,
            operation_mode: true,
            lat: 0.0,
            lon: 0.0,
            coverage_object: Some(coverage_object),
            location_validation_timestamp: None,
            timestamp: "2023-08-23 00:00:00.000000000 UTC".parse().unwrap(),
        },
        cell_type: CellType::SercommIndoor,
        distance_to_asserted: Some(1000), // Cannot be null
        coverage_meta: None,
        location_trust_score_multiplier: dec!(1.0),
        validity: HeartbeatValidity::Valid,
    };

    let mut transaction = pool.begin().await?;

    heartbeat.save(&mut transaction).await?;

    let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM wifi_heartbeats")
        .fetch_one(&mut transaction)
        .await?;

    assert_eq!(count, 1);

    Ok(())
}

#[sqlx::test]
#[ignore]
async fn test_save_cbrs_heartbeat(pool: PgPool) -> anyhow::Result<()> {
    let coverage_object = Uuid::new_v4();
    let heartbeat = ValidatedHeartbeat {
        heartbeat: Heartbeat {
            hb_type: HbType::Cbrs,
            hotspot_key: "11eX55faMbqZB7jzN4p67m6w7ScPMH6ubnvCjCPLh72J49PaJEL"
                .parse()
                .unwrap(),
            cbsd_id: Some("P27-SCE4255W120200039521XGB0103".to_string()),
            operation_mode: true,
            lat: 0.0,
            lon: 0.0,
            coverage_object: Some(coverage_object),
            location_validation_timestamp: None,
            timestamp: "2023-08-23 00:00:00.000000000 UTC".parse().unwrap(),
        },
        cell_type: CellType::SercommIndoor,
        distance_to_asserted: None,
        coverage_meta: None,
        location_trust_score_multiplier: dec!(1.0),
        validity: HeartbeatValidity::Valid,
    };

    let mut transaction = pool.begin().await?;

    heartbeat.save(&mut transaction).await?;

    let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM cbrs_heartbeats")
        .fetch_one(&mut transaction)
        .await?;

    assert_eq!(count, 1);

    Ok(())
}

#[sqlx::test]
#[ignore]
async fn only_fetch_latest_hotspot(pool: PgPool) -> anyhow::Result<()> {
    let cbsd_id = "P27-SCE4255W120200039521XGB0103".to_string();
    let coverage_object = Uuid::new_v4();
    let cell_type = CellType::from_cbsd_id(&cbsd_id).expect("unable to get cell_type");
    let hotspot_1: PublicKeyBinary =
        "112NqN2WWMwtK29PMzRby62fDydBJfsCLkCAf392stdok48ovNT6".parse()?;
    let hotspot_2: PublicKeyBinary =
        "11sctWiP9r5wDJVuDe1Th4XSL2vaawaLLSQF8f8iokAoMAJHxqp".parse()?;
    sqlx::query(
        r#"
INSERT INTO cbrs_heartbeats (cbsd_id, hotspot_key, cell_type, latest_timestamp, truncated_timestamp, coverage_object, location_trust_score_multiplier)
VALUES
    ($1, $2, 'sercommindoor', '2023-08-25 00:00:00+00', '2023-08-25 00:00:00+00', $4, 1.0),
    ($1, $3, 'sercommindoor', '2023-08-25 01:00:00+00', '2023-08-25 01:00:00+00', $4, 1.0),
    ($1, $2, 'sercommindoor', '2023-08-25 02:00:00+00', '2023-08-25 02:00:00+00', $4, 1.0),
    ($1, $3, 'sercommindoor', '2023-08-25 03:00:00+00', '2023-08-25 03:00:00+00', $4, 1.0),
    ($1, $2, 'sercommindoor', '2023-08-25 04:00:00+00', '2023-08-25 04:00:00+00', $4, 1.0),
    ($1, $3, 'sercommindoor', '2023-08-25 05:00:00+00', '2023-08-25 05:00:00+00', $4, 1.0),
    ($1, $2, 'sercommindoor', '2023-08-25 06:00:00+00', '2023-08-25 06:00:00+00', $4, 1.0),
    ($1, $3, 'sercommindoor', '2023-08-25 07:00:00+00', '2023-08-25 07:00:00+00', $4, 1.0),
    ($1, $2, 'sercommindoor', '2023-08-25 08:00:00+00', '2023-08-25 08:00:00+00', $4, 1.0),
    ($1, $3, 'sercommindoor', '2023-08-25 09:00:00+00', '2023-08-25 09:00:00+00', $4, 1.0),
    ($1, $2, 'sercommindoor', '2023-08-25 10:00:00+00', '2023-08-25 10:00:00+00', $4, 1.0),
    ($1, $3, 'sercommindoor', '2023-08-25 11:00:00+00', '2023-08-25 11:00:00+00', $4, 1.0),
    ($1, $2, 'sercommindoor', '2023-08-25 12:00:00+00', '2023-08-25 12:00:00+00', $4, 1.0),
    ($1, $3, 'sercommindoor', '2023-08-25 13:00:00+00', '2023-08-25 13:00:00+00', $4, 1.0),
    ($1, $2, 'sercommindoor', '2023-08-25 14:00:00+00', '2023-08-25 14:00:00+00', $4, 1.0),
    ($1, $3, 'sercommindoor', '2023-08-25 15:00:00+00', '2023-08-25 15:00:00+00', $4, 1.0),
    ($1, $2, 'sercommindoor', '2023-08-25 16:00:00+00', '2023-08-25 16:00:00+00', $4, 1.0),
    ($1, $3, 'sercommindoor', '2023-08-25 17:00:00+00', '2023-08-25 17:00:00+00', $4, 1.0),
    ($1, $2, 'sercommindoor', '2023-08-25 18:00:00+00', '2023-08-25 18:00:00+00', $4, 1.0),
    ($1, $3, 'sercommindoor', '2023-08-25 19:00:00+00', '2023-08-25 19:00:00+00', $4, 1.0),
    ($1, $2, 'sercommindoor', '2023-08-25 20:00:00+00', '2023-08-25 20:00:00+00', $4, 1.0),
    ($1, $3, 'sercommindoor', '2023-08-25 21:00:00+00', '2023-08-25 21:00:00+00', $4, 1.0),
    ($1, $2, 'sercommindoor', '2023-08-25 22:00:00+00', '2023-08-25 22:00:00+00', $4, 1.0),
    ($1, $3, 'sercommindoor', '2023-08-25 23:00:00+00', '2023-08-25 23:00:00+00', $4, 1.0)
"#,
    )
    .bind(&cbsd_id)
    .bind(&hotspot_1)
    .bind(&hotspot_2)
    .bind(coverage_object)
    .execute(&pool)
    .await?;

    let start_period: DateTime<Utc> = "2023-08-25 00:00:00.000000000 UTC".parse()?;
    let end_period: DateTime<Utc> = "2023-08-26 00:00:00.000000000 UTC".parse()?;
    let heartbeat_reward: Vec<_> = HeartbeatReward::validated(&pool, &(start_period..end_period))
        .try_collect()
        .await?;

    assert_eq!(
        heartbeat_reward,
        vec![HeartbeatReward {
            hotspot_key: hotspot_2,
            cell_type,
            cbsd_id: Some(cbsd_id),
            trust_score_multipliers: vec![Decimal::ONE; 24],
            distances_to_asserted: None,
            coverage_object,
        }]
    );

    Ok(())
}

#[sqlx::test]
#[ignore]
async fn ensure_hotspot_does_not_affect_count(pool: PgPool) -> anyhow::Result<()> {
    let cbsd_id = "P27-SCE4255W120200039521XGB0103".to_string();
    let coverage_object = Uuid::new_v4();
    let cell_type = CellType::from_cbsd_id(&cbsd_id).expect("unable to get cell_type");
    let hotspot_1: PublicKeyBinary =
        "112NqN2WWMwtK29PMzRby62fDydBJfsCLkCAf392stdok48ovNT6".parse()?;
    let hotspot_2: PublicKeyBinary =
        "11sctWiP9r5wDJVuDe1Th4XSL2vaawaLLSQF8f8iokAoMAJHxqp".parse()?;
    sqlx::query(
        r#"
INSERT INTO cbrs_heartbeats (cbsd_id, hotspot_key, cell_type, latest_timestamp, truncated_timestamp, coverage_object, location_trust_score_multiplier)
VALUES
    ($1, $2, 'sercommindoor', '2023-08-25 00:00:00+00', '2023-08-25 00:00:00+00', $4, 1.0),
    ($1, $2, 'sercommindoor', '2023-08-25 01:00:00+00', '2023-08-25 01:00:00+00', $4, 1.0),
    ($1, $2, 'sercommindoor', '2023-08-25 02:00:00+00', '2023-08-25 02:00:00+00', $4, 1.0),
    ($1, $2, 'sercommindoor', '2023-08-25 03:00:00+00', '2023-08-25 03:00:00+00', $4, 1.0),
    ($1, $2, 'sercommindoor', '2023-08-25 04:00:00+00', '2023-08-25 04:00:00+00', $4, 1.0),
    ($1, $2, 'sercommindoor', '2023-08-25 05:00:00+00', '2023-08-25 05:00:00+00', $4, 1.0),
    ($1, $2, 'sercommindoor', '2023-08-25 06:00:00+00', '2023-08-25 06:00:00+00', $4, 1.0),
    ($1, $2, 'sercommindoor', '2023-08-25 07:00:00+00', '2023-08-25 07:00:00+00', $4, 1.0),
    ($1, $2, 'sercommindoor', '2023-08-25 08:00:00+00', '2023-08-25 08:00:00+00', $4, 1.0),
    ($1, $2, 'sercommindoor', '2023-08-25 09:00:00+00', '2023-08-25 09:00:00+00', $4, 1.0),
    ($1, $2, 'sercommindoor', '2023-08-25 10:00:00+00', '2023-08-25 10:00:00+00', $4, 1.0),
    ($1, $3, 'sercommindoor', '2023-08-25 11:00:00+00', '2023-08-25 11:00:00+00', $4, 1.0)
"#,
    )
    .bind(&cbsd_id)
    .bind(&hotspot_1)
    .bind(&hotspot_2)
    .bind(coverage_object)
    .execute(&pool)
    .await?;

    let start_period: DateTime<Utc> = "2023-08-25 00:00:00.000000000 UTC".parse()?;
    let end_period: DateTime<Utc> = "2023-08-26 00:00:00.000000000 UTC".parse()?;
    let heartbeat_reward: Vec<_> = HeartbeatReward::validated(&pool, &(start_period..end_period))
        .try_collect()
        .await?;

    assert_eq!(
        heartbeat_reward,
        vec![HeartbeatReward {
            hotspot_key: hotspot_2,
            cell_type,
            cbsd_id: Some(cbsd_id),
            trust_score_multipliers: vec![Decimal::ONE; 12],
            distances_to_asserted: None,
            coverage_object,
        }]
    );

    Ok(())
}

#[sqlx::test]
#[ignore]
async fn ensure_minimum_count(pool: PgPool) -> anyhow::Result<()> {
    let cbsd_id = "P27-SCE4255W120200039521XGB0103".to_string();
    let coverage_object = Uuid::new_v4();
    let hotspot: PublicKeyBinary =
        "112NqN2WWMwtK29PMzRby62fDydBJfsCLkCAf392stdok48ovNT6".parse()?;
    sqlx::query(
        r#"
INSERT INTO cbrs_heartbeats (cbsd_id, hotspot_key, cell_type, latest_timestamp, truncated_timestamp, coverage_object, location_trust_score_multiplier)
VALUES
    ($1, $2, 'sercommindoor', '2023-08-25 00:00:00+00', '2023-08-25 00:00:00+00', $3, 1.0),
    ($1, $2, 'sercommindoor', '2023-08-25 01:00:00+00', '2023-08-25 01:00:00+00', $3, 1.0),
    ($1, $2, 'sercommindoor', '2023-08-25 02:00:00+00', '2023-08-25 02:00:00+00', $3, 1.0),
    ($1, $2, 'sercommindoor', '2023-08-25 03:00:00+00', '2023-08-25 03:00:00+00', $3, 1.0),
    ($1, $2, 'sercommindoor', '2023-08-25 04:00:00+00', '2023-08-25 04:00:00+00', $3, 1.0),
    ($1, $2, 'sercommindoor', '2023-08-25 05:00:00+00', '2023-08-25 05:00:00+00', $3, 1.0),
    ($1, $2, 'sercommindoor', '2023-08-25 06:00:00+00', '2023-08-25 06:00:00+00', $3, 1.0),
    ($1, $2, 'sercommindoor', '2023-08-25 07:00:00+00', '2023-08-25 07:00:00+00', $3, 1.0),
    ($1, $2, 'sercommindoor', '2023-08-25 08:00:00+00', '2023-08-25 08:00:00+00', $3, 1.0),
    ($1, $2, 'sercommindoor', '2023-08-25 09:00:00+00', '2023-08-25 09:00:00+00', $3, 1.0),
    ($1, $2, 'sercommindoor', '2023-08-25 10:00:00+00', '2023-08-25 10:00:00+00', $3, 1.0)
"#,
    )
    .bind(&cbsd_id)
    .bind(&hotspot)
    .bind(coverage_object)
    .execute(&pool)
    .await?;

    let start_period: DateTime<Utc> = "2023-08-25 00:00:00.000000000 UTC".parse()?;
    let end_period: DateTime<Utc> = "2023-08-26 00:00:00.000000000 UTC".parse()?;
    let heartbeat_reward: Vec<_> = HeartbeatReward::validated(&pool, &(start_period..end_period))
        .try_collect()
        .await?;

    assert!(heartbeat_reward.is_empty());

    Ok(())
}

#[sqlx::test]
#[ignore]
async fn ensure_wifi_hotspots_are_rewarded(pool: PgPool) -> anyhow::Result<()> {
    let early_coverage_object = Uuid::new_v4();
    let latest_coverage_object = Uuid::new_v4();
    let hotspot: PublicKeyBinary =
        "112NqN2WWMwtK29PMzRby62fDydBJfsCLkCAf392stdok48ovNT6".parse()?;
    sqlx::query(
        r#"
INSERT INTO wifi_heartbeats (hotspot_key, cell_type, latest_timestamp, truncated_timestamp, coverage_object, location_trust_score_multiplier, distance_to_asserted)
VALUES
    ($1, 'novagenericwifiindoor', '2023-08-25 00:00:00+00', '2023-08-25 00:00:00+00', $2, 1.0, 0),
    ($1, 'novagenericwifiindoor', '2023-08-25 01:00:00+00', '2023-08-25 01:00:00+00', $2, 1.0, 0),
    ($1, 'novagenericwifiindoor', '2023-08-25 02:00:00+00', '2023-08-25 02:00:00+00', $2, 1.0, 0),
    ($1, 'novagenericwifiindoor', '2023-08-25 03:00:00+00', '2023-08-25 03:00:00+00', $2, 1.0, 0),
    ($1, 'novagenericwifiindoor', '2023-08-25 04:00:00+00', '2023-08-25 04:00:00+00', $2, 1.0, 0),
    ($1, 'novagenericwifiindoor', '2023-08-25 05:00:00+00', '2023-08-25 05:00:00+00', $2, 1.0, 0),
    ($1, 'novagenericwifiindoor', '2023-08-25 06:00:00+00', '2023-08-25 06:00:00+00', $2, 1.0, 0),
    ($1, 'novagenericwifiindoor', '2023-08-25 07:00:00+00', '2023-08-25 07:00:00+00', $2, 1.0, 0),
    ($1, 'novagenericwifiindoor', '2023-08-25 08:00:00+00', '2023-08-25 08:00:00+00', $2, 1.0, 0),
    ($1, 'novagenericwifiindoor', '2023-08-25 09:00:00+00', '2023-08-25 09:00:00+00', $2, 1.0, 0),
    ($1, 'novagenericwifiindoor', '2023-08-25 10:00:00+00', '2023-08-25 10:00:00+00', $2, 1.0, 0),
    ($1, 'novagenericwifiindoor', '2023-08-25 11:00:00+00', '2023-08-25 11:00:00+00', $3, 1.0, 0)
"#,
    )
    .bind(&hotspot)
    .bind(early_coverage_object)
    .bind(latest_coverage_object)
    .execute(&pool)
    .await?;

    let start_period: DateTime<Utc> = "2023-08-25 00:00:00.000000000 UTC".parse()?;
    let end_period: DateTime<Utc> = "2023-08-26 00:00:00.000000000 UTC".parse()?;
    let heartbeat_reward: Vec<_> = HeartbeatReward::validated(&pool, &(start_period..end_period))
        .try_collect()
        .await?;

    assert_eq!(
        heartbeat_reward,
        vec![HeartbeatReward {
            hotspot_key: hotspot,
            cell_type: CellType::NovaGenericWifiIndoor,
            cbsd_id: None,
            trust_score_multipliers: vec![Decimal::ONE; 12],
            distances_to_asserted: Some(vec![0; 12]),
            coverage_object: latest_coverage_object,
        }]
    );

    Ok(())
}

#[sqlx::test]
#[ignore]
async fn ensure_wifi_hotspots_use_average_location_trust_score(pool: PgPool) -> anyhow::Result<()> {
    let early_coverage_object = Uuid::new_v4();
    let latest_coverage_object = Uuid::new_v4();
    let hotspot: PublicKeyBinary =
        "112NqN2WWMwtK29PMzRby62fDydBJfsCLkCAf392stdok48ovNT6".parse()?;
    sqlx::query(
        r#"
INSERT INTO wifi_heartbeats (hotspot_key, cell_type, latest_timestamp, truncated_timestamp, coverage_object, location_trust_score_multiplier, distance_to_asserted)
VALUES
    ($1, 'novagenericwifiindoor', '2023-08-25 00:00:00+00', '2023-08-25 00:00:00+00', $2, 1.0, 0),
    ($1, 'novagenericwifiindoor', '2023-08-25 01:00:00+00', '2023-08-25 01:00:00+00', $2, 1.0, 0),
    ($1, 'novagenericwifiindoor', '2023-08-25 02:00:00+00', '2023-08-25 02:00:00+00', $2, 1.0, 0),
    ($1, 'novagenericwifiindoor', '2023-08-25 03:00:00+00', '2023-08-25 03:00:00+00', $2, 1.0, 0),
    ($1, 'novagenericwifiindoor', '2023-08-25 04:00:00+00', '2023-08-25 04:00:00+00', $2, 1.0, 0),
    ($1, 'novagenericwifiindoor', '2023-08-25 05:00:00+00', '2023-08-25 05:00:00+00', $2, 1.0, 0),
    ($1, 'novagenericwifiindoor', '2023-08-25 06:00:00+00', '2023-08-25 06:00:00+00', $2, 1.0, 0),
    ($1, 'novagenericwifiindoor', '2023-08-25 07:00:00+00', '2023-08-25 07:00:00+00', $2, 1.0, 0),
    ($1, 'novagenericwifiindoor', '2023-08-25 08:00:00+00', '2023-08-25 08:00:00+00', $2, 0.25, 0),
    ($1, 'novagenericwifiindoor', '2023-08-25 09:00:00+00', '2023-08-25 09:00:00+00', $2, 0.25, 0),
    ($1, 'novagenericwifiindoor', '2023-08-25 10:00:00+00', '2023-08-25 10:00:00+00', $2, 0.25, 0),
    ($1, 'novagenericwifiindoor', '2023-08-25 11:00:00+00', '2023-08-25 11:00:00+00', $3, 0.25, 0)
"#,
    )
    .bind(&hotspot)
    .bind(early_coverage_object)
    .bind(latest_coverage_object)
    .execute(&pool)
    .await?;

    let start_period: DateTime<Utc> = "2023-08-25 00:00:00.000000000 UTC".parse()?;
    let end_period: DateTime<Utc> = "2023-08-26 00:00:00.000000000 UTC".parse()?;
    let heartbeat_reward: Vec<_> = HeartbeatReward::validated(&pool, &(start_period..end_period))
        .try_collect()
        .await?;

    assert_eq!(
        heartbeat_reward,
        vec![HeartbeatReward {
            hotspot_key: hotspot,
            cell_type: CellType::NovaGenericWifiIndoor,
            cbsd_id: None,
            trust_score_multipliers: vec![
                dec!(1.0),
                dec!(1.0),
                dec!(1.0),
                dec!(1.0),
                dec!(1.0),
                dec!(1.0),
                dec!(1.0),
                dec!(1.0),
                dec!(0.25),
                dec!(0.25),
                dec!(0.25),
                dec!(0.25)
            ],
            distances_to_asserted: Some(vec![0; 12]),
            coverage_object: latest_coverage_object,
        }]
    );

    Ok(())
}

fn signal_level(hex: &str, signal_level: SignalLevel) -> anyhow::Result<RadioHexSignalLevel> {
    Ok(RadioHexSignalLevel {
        location: hex.parse()?,
        signal_level,
        signal_power: 0, // Unused
    })
}

#[derive(Clone)]
struct MockGeofence;

impl GeofenceValidator<Heartbeat> for MockGeofence {
    fn in_valid_region(&self, _heartbeat: &Heartbeat) -> bool {
        true
    }
}

impl GeofenceValidator<u64> for MockGeofence {
    fn in_valid_region(&self, _cell: &u64) -> bool {
        true
    }
}

#[derive(Copy, Clone)]
struct AllOwnersValid;

#[async_trait::async_trait]
impl GatewayResolver for AllOwnersValid {
    type Error = std::convert::Infallible;

    async fn resolve_gateway(
        &self,
        _address: &PublicKeyBinary,
    ) -> Result<GatewayResolution, Self::Error> {
        Ok(GatewayResolution::AssertedLocation(0x8c2681a3064d9ff))
    }
}

#[sqlx::test]
async fn use_previous_location_if_timestamp_is_none(pool: PgPool) -> anyhow::Result<()> {
    let hotspot: PublicKeyBinary =
        "112NqN2WWMwtK29PMzRby62fDydBJfsCLkCAf392stdok48ovNT6".parse()?;
    let hotspot_2: PublicKeyBinary =
        "11sctWiP9r5wDJVuDe1Th4XSL2vaawaLLSQF8f8iokAoMAJHxqp".parse()?;

    let coverage_obj = Uuid::new_v4();
    let coverage_object = file_store::coverage::CoverageObject {
        pub_key: hotspot.clone(),
        uuid: coverage_obj,
        key_type: file_store::coverage::KeyType::HotspotKey(hotspot.clone()),
        coverage_claim_time: "2022-01-01 00:00:00.000000000 UTC".parse()?,
        indoor: true,
        signature: Vec::new(),
        coverage: vec![signal_level("8c2681a3064d9ff", SignalLevel::High)?],
        trust_score: 0,
    };

    let coverage_obj_2 = Uuid::new_v4();
    let coverage_object_2 = file_store::coverage::CoverageObject {
        pub_key: hotspot_2.clone(),
        uuid: coverage_obj_2,
        key_type: file_store::coverage::KeyType::HotspotKey(hotspot_2.clone()),
        coverage_claim_time: "2022-01-01 00:00:00.000000000 UTC".parse()?,
        indoor: true,
        signature: Vec::new(),
        coverage: vec![signal_level("8c2681a3064d9ff", SignalLevel::High)?],
        trust_score: 0,
    };

    let mut transaction = pool.begin().await?;
    CoverageObject {
        coverage_object,
        validity: CoverageObjectValidity::Valid,
    }
    .save(&mut transaction)
    .await?;
    CoverageObject {
        coverage_object: coverage_object_2,
        validity: CoverageObjectValidity::Valid,
    }
    .save(&mut transaction)
    .await?;
    transaction.commit().await?;

    let coverage_objects = CoverageObjectCache::new(&pool);
    let location_cache = LocationCache::new(&pool);

    let cell: CellIndex = "8c2681a3064d9ff".parse().unwrap();
    let lat_lng: LatLng = cell.into();

    let epoch_start: DateTime<Utc> = "2023-08-20 00:00:00.000000000 UTC".parse().unwrap();
    let epoch_end: DateTime<Utc> = "2023-08-25 00:00:00.000000000 UTC".parse().unwrap();

    let first_heartbeat = Heartbeat {
        hb_type: HbType::Wifi,
        hotspot_key: hotspot.clone(),
        cbsd_id: None,
        operation_mode: true,
        lat: lat_lng.lat(),
        lon: lat_lng.lng(),
        coverage_object: Some(coverage_obj),
        location_validation_timestamp: Some(Utc::now()),
        timestamp: "2023-08-23 00:00:00.000000000 UTC".parse().unwrap(),
    };

    let first_heartbeat = ValidatedHeartbeat::validate(
        first_heartbeat,
        &AllOwnersValid,
        &coverage_objects,
        &location_cache,
        1,
        u32::MAX,
        &(epoch_start..epoch_end),
        &MockGeofence,
    )
    .await
    .unwrap();

    // First heartbeat should have a 1.0 trust score:
    assert_eq!(first_heartbeat.location_trust_score_multiplier, dec!(1.0));

    let second_heartbeat = Heartbeat {
        hb_type: HbType::Wifi,
        hotspot_key: hotspot.clone(),
        cbsd_id: None,
        operation_mode: true,
        lat: 0.0,
        lon: 0.0,
        coverage_object: Some(coverage_obj),
        location_validation_timestamp: None,
        timestamp: "2023-08-23 00:00:00.000000000 UTC".parse().unwrap(),
    };

    let second_heartbeat = ValidatedHeartbeat::validate(
        second_heartbeat,
        &AllOwnersValid,
        &coverage_objects,
        &location_cache,
        1,
        u32::MAX,
        &(epoch_start..epoch_end),
        &MockGeofence,
    )
    .await
    .unwrap();

    // Despite having no location set, we should still have a 1.0 trust score
    // for the second heartbeat:
    assert_eq!(second_heartbeat.location_trust_score_multiplier, dec!(1.0));
    // Additionally, the lat and lon should be set to the correct value
    assert_eq!(second_heartbeat.heartbeat.lat, lat_lng.lat());
    assert_eq!(second_heartbeat.heartbeat.lon, lat_lng.lng());

    // If we remove the radio from the location cache, then we should not see
    // the same behavior:

    location_cache.delete_last_location(&hotspot).await;

    let third_heartbeat = Heartbeat {
        hb_type: HbType::Wifi,
        hotspot_key: hotspot.clone(),
        cbsd_id: None,
        operation_mode: true,
        lat: 0.0,
        lon: 0.0,
        coverage_object: Some(coverage_obj),
        location_validation_timestamp: None,
        timestamp: "2023-08-23 00:00:00.000000000 UTC".parse().unwrap(),
    };

    let third_heartbeat = ValidatedHeartbeat::validate(
        third_heartbeat,
        &AllOwnersValid,
        &coverage_objects,
        &location_cache,
        1,
        u32::MAX,
        &(epoch_start..epoch_end),
        &MockGeofence,
    )
    .await
    .unwrap();

    assert_ne!(third_heartbeat.location_trust_score_multiplier, dec!(1.0));
    assert_eq!(third_heartbeat.heartbeat.lat, 0.0);
    assert_eq!(third_heartbeat.heartbeat.lon, 0.0);

    let hotspot_2_hb_1 = Heartbeat {
        hb_type: HbType::Wifi,
        hotspot_key: hotspot_2.clone(),
        cbsd_id: None,
        operation_mode: true,
        lat: 48.385318100686,
        lon: -104.697568066261,
        coverage_object: Some(coverage_obj_2),
        location_validation_timestamp: Some(Utc::now()),
        timestamp: "2023-08-23 00:00:00.000000000 UTC".parse().unwrap(),
    };

    let hotspot_2_hb_1 = ValidatedHeartbeat::validate(
        hotspot_2_hb_1,
        &AllOwnersValid,
        &coverage_objects,
        &location_cache,
        1,
        u32::MAX,
        &(epoch_start..epoch_end),
        &MockGeofence,
    )
    .await
    .unwrap();

    // We also want to ensure that if the first heartbeat is saved into the
    // db that it is properly fetched:
    let mut transaction = pool.begin().await?;
    first_heartbeat.save(&mut transaction).await?;
    hotspot_2_hb_1.save(&mut transaction).await?;
    transaction.commit().await?;

    // Also check to make sure that fetching from the DB gives us the same location
    // for hotspot 2:
    location_cache.delete_last_location(&hotspot_2).await;
    let hotspot_2_last_location = location_cache
        .fetch_last_location(&hotspot_2)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(hotspot_2_last_location.lat, 48.385318100686);
    assert_eq!(hotspot_2_last_location.lon, -104.697568066261);

    // We have to remove the last location again, as the lack of previous
    // locations was added to the cache:
    location_cache.delete_last_location(&hotspot).await;

    let fourth_heartbeat = Heartbeat {
        hb_type: HbType::Wifi,
        hotspot_key: hotspot.clone(),
        cbsd_id: None,
        operation_mode: true,
        lat: 0.0,
        lon: 0.0,
        coverage_object: Some(coverage_obj),
        location_validation_timestamp: None,
        timestamp: "2023-08-23 00:00:00.000000000 UTC".parse().unwrap(),
    };

    let fourth_heartbeat = ValidatedHeartbeat::validate(
        fourth_heartbeat,
        &AllOwnersValid,
        &coverage_objects,
        &location_cache,
        1,
        u32::MAX,
        &(epoch_start..epoch_end),
        &MockGeofence,
    )
    .await
    .unwrap();

    assert_eq!(fourth_heartbeat.location_trust_score_multiplier, dec!(1.0));
    assert_eq!(fourth_heartbeat.heartbeat.lat, lat_lng.lat());
    assert_eq!(fourth_heartbeat.heartbeat.lon, lat_lng.lng());

    // Lastly, check that if the valid heartbeat was saved over 12 hours ago
    // that it is not used:
    sqlx::query("TRUNCATE TABLE wifi_heartbeats")
        .execute(&pool)
        .await?;
    location_cache.delete_last_location(&hotspot).await;

    let fifth_heartbeat = Heartbeat {
        hb_type: HbType::Wifi,
        hotspot_key: hotspot.clone(),
        cbsd_id: None,
        operation_mode: true,
        lat: lat_lng.lat(),
        lon: lat_lng.lng(),
        coverage_object: Some(coverage_obj),
        location_validation_timestamp: Some(
            Utc::now() - (Duration::hours(12) + Duration::seconds(1)),
        ),
        timestamp: "2023-08-23 00:00:00.000000000 UTC".parse().unwrap(),
    };

    let fifth_heartbeat = ValidatedHeartbeat::validate(
        fifth_heartbeat,
        &AllOwnersValid,
        &coverage_objects,
        &location_cache,
        1,
        u32::MAX,
        &(epoch_start..epoch_end),
        &MockGeofence,
    )
    .await
    .unwrap();

    let mut transaction = pool.begin().await?;
    fifth_heartbeat.save(&mut transaction).await?;
    transaction.commit().await?;

    let sixth_heartbeat = Heartbeat {
        hb_type: HbType::Wifi,
        hotspot_key: hotspot.clone(),
        cbsd_id: None,
        operation_mode: true,
        lat: 0.0,
        lon: 0.0,
        coverage_object: Some(coverage_obj),
        location_validation_timestamp: None,
        timestamp: "2023-08-23 00:00:00.000000000 UTC".parse().unwrap(),
    };

    let sixth_heartbeat = ValidatedHeartbeat::validate(
        sixth_heartbeat,
        &AllOwnersValid,
        &coverage_objects,
        &location_cache,
        1,
        u32::MAX,
        &(epoch_start..epoch_end),
        &MockGeofence,
    )
    .await
    .unwrap();

    assert_ne!(sixth_heartbeat.location_trust_score_multiplier, dec!(1.0));

    location_cache.delete_last_location(&hotspot).await;

    let seventh_heartbeat = Heartbeat {
        hb_type: HbType::Wifi,
        hotspot_key: hotspot.clone(),
        cbsd_id: None,
        operation_mode: true,
        lat: 0.0,
        lon: 0.0,
        coverage_object: Some(coverage_obj),
        location_validation_timestamp: None,
        timestamp: "2023-08-23 00:00:00.000000000 UTC".parse().unwrap(),
    };

    let seventh_heartbeat = ValidatedHeartbeat::validate(
        seventh_heartbeat,
        &AllOwnersValid,
        &coverage_objects,
        &location_cache,
        1,
        u32::MAX,
        &(epoch_start..epoch_end),
        &MockGeofence,
    )
    .await
    .unwrap();

    assert_ne!(seventh_heartbeat.location_trust_score_multiplier, dec!(1.0));

    Ok(())
}
