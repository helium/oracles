use chrono::{DateTime, Utc};
use futures_util::TryStreamExt;
use helium_crypto::PublicKeyBinary;
use helium_proto::services::poc_mobile::HeartbeatValidity;
use mobile_verifier::cell_type::CellType;
use mobile_verifier::heartbeats::{HbType, Heartbeat, HeartbeatReward, ValidatedHeartbeat};
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
        distance_to_asserted: None,
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
            asserted_hex: None,
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
            asserted_hex: None,
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
INSERT INTO wifi_heartbeats (hotspot_key, cell_type, latest_timestamp, truncated_timestamp, coverage_object, location_trust_score_multiplier, distance_to_asserted, asserted_hex)
VALUES
    ($1, 'novagenericwifiindoor', '2023-08-25 00:00:00+00', '2023-08-25 00:00:00+00', $2, 1.0, 0, 0),
    ($1, 'novagenericwifiindoor', '2023-08-25 01:00:00+00', '2023-08-25 01:00:00+00', $2, 1.0, 0, 0),
    ($1, 'novagenericwifiindoor', '2023-08-25 02:00:00+00', '2023-08-25 02:00:00+00', $2, 1.0, 0, 0),
    ($1, 'novagenericwifiindoor', '2023-08-25 03:00:00+00', '2023-08-25 03:00:00+00', $2, 1.0, 0, 0),
    ($1, 'novagenericwifiindoor', '2023-08-25 04:00:00+00', '2023-08-25 04:00:00+00', $2, 1.0, 0, 0),
    ($1, 'novagenericwifiindoor', '2023-08-25 05:00:00+00', '2023-08-25 05:00:00+00', $2, 1.0, 0, 0),
    ($1, 'novagenericwifiindoor', '2023-08-25 06:00:00+00', '2023-08-25 06:00:00+00', $2, 1.0, 0, 0),
    ($1, 'novagenericwifiindoor', '2023-08-25 07:00:00+00', '2023-08-25 07:00:00+00', $2, 1.0, 0, 0),
    ($1, 'novagenericwifiindoor', '2023-08-25 08:00:00+00', '2023-08-25 08:00:00+00', $2, 1.0, 0, 0),
    ($1, 'novagenericwifiindoor', '2023-08-25 09:00:00+00', '2023-08-25 09:00:00+00', $2, 1.0, 0, 0),
    ($1, 'novagenericwifiindoor', '2023-08-25 10:00:00+00', '2023-08-25 10:00:00+00', $2, 1.0, 0, 0),
    ($1, 'novagenericwifiindoor', '2023-08-25 11:00:00+00', '2023-08-25 11:00:00+00', $3, 1.0, 0, 0)
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
            asserted_hex: Some(0),
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
INSERT INTO wifi_heartbeats (hotspot_key, cell_type, latest_timestamp, truncated_timestamp, coverage_object, location_trust_score_multiplier, distance_to_asserted, asserted_hex)
VALUES
    ($1, 'novagenericwifiindoor', '2023-08-25 00:00:00+00', '2023-08-25 00:00:00+00', $2, 1.0, 0, 0),
    ($1, 'novagenericwifiindoor', '2023-08-25 01:00:00+00', '2023-08-25 01:00:00+00', $2, 1.0, 0, 0),
    ($1, 'novagenericwifiindoor', '2023-08-25 02:00:00+00', '2023-08-25 02:00:00+00', $2, 1.0, 0, 0),
    ($1, 'novagenericwifiindoor', '2023-08-25 03:00:00+00', '2023-08-25 03:00:00+00', $2, 1.0, 0, 0),
    ($1, 'novagenericwifiindoor', '2023-08-25 04:00:00+00', '2023-08-25 04:00:00+00', $2, 1.0, 0, 0),
    ($1, 'novagenericwifiindoor', '2023-08-25 05:00:00+00', '2023-08-25 05:00:00+00', $2, 1.0, 0, 0),
    ($1, 'novagenericwifiindoor', '2023-08-25 06:00:00+00', '2023-08-25 06:00:00+00', $2, 1.0, 0, 0),
    ($1, 'novagenericwifiindoor', '2023-08-25 07:00:00+00', '2023-08-25 07:00:00+00', $2, 1.0, 0, 0),
    ($1, 'novagenericwifiindoor', '2023-08-25 08:00:00+00', '2023-08-25 08:00:00+00', $2, 0.25, 0, 0),
    ($1, 'novagenericwifiindoor', '2023-08-25 09:00:00+00', '2023-08-25 09:00:00+00', $2, 0.25, 0, 0),
    ($1, 'novagenericwifiindoor', '2023-08-25 10:00:00+00', '2023-08-25 10:00:00+00', $2, 0.25, 0, 0),
    ($1, 'novagenericwifiindoor', '2023-08-25 11:00:00+00', '2023-08-25 11:00:00+00', $3, 0.25, 0, 0)
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
            asserted_hex: Some(0),
            coverage_object: latest_coverage_object,
        }]
    );

    Ok(())
}
