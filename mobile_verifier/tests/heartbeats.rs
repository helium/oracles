use chrono::{DateTime, Utc};
use futures_util::TryStreamExt;
use helium_crypto::PublicKeyBinary;
use mobile_verifier::cell_type::CellType;
use mobile_verifier::heartbeats::HeartbeatReward;
use rust_decimal::Decimal;
use sqlx::PgPool;
use uuid::Uuid;

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
INSERT INTO cbrs_heartbeats (cbsd_id, hotspot_key, cell_type, latest_timestamp, truncated_timestamp, coverage_object)
VALUES
    ($1, $2, 'sercommindoor', '2023-08-25 00:00:00+00', '2023-08-25 00:00:00+00', $4),
    ($1, $3, 'sercommindoor', '2023-08-25 01:00:00+00', '2023-08-25 01:00:00+00', $4),
    ($1, $2, 'sercommindoor', '2023-08-25 02:00:00+00', '2023-08-25 02:00:00+00', $4),
    ($1, $3, 'sercommindoor', '2023-08-25 03:00:00+00', '2023-08-25 03:00:00+00', $4),
    ($1, $2, 'sercommindoor', '2023-08-25 04:00:00+00', '2023-08-25 04:00:00+00', $4),
    ($1, $3, 'sercommindoor', '2023-08-25 05:00:00+00', '2023-08-25 05:00:00+00', $4),
    ($1, $2, 'sercommindoor', '2023-08-25 06:00:00+00', '2023-08-25 06:00:00+00', $4),
    ($1, $3, 'sercommindoor', '2023-08-25 07:00:00+00', '2023-08-25 07:00:00+00', $4),
    ($1, $2, 'sercommindoor', '2023-08-25 08:00:00+00', '2023-08-25 08:00:00+00', $4),
    ($1, $3, 'sercommindoor', '2023-08-25 09:00:00+00', '2023-08-25 09:00:00+00', $4),
    ($1, $2, 'sercommindoor', '2023-08-25 10:00:00+00', '2023-08-25 10:00:00+00', $4),
    ($1, $3, 'sercommindoor', '2023-08-25 11:00:00+00', '2023-08-25 11:00:00+00', $4),
    ($1, $2, 'sercommindoor', '2023-08-25 12:00:00+00', '2023-08-25 12:00:00+00', $4),
    ($1, $3, 'sercommindoor', '2023-08-25 13:00:00+00', '2023-08-25 13:00:00+00', $4),
    ($1, $2, 'sercommindoor', '2023-08-25 14:00:00+00', '2023-08-25 14:00:00+00', $4),
    ($1, $3, 'sercommindoor', '2023-08-25 15:00:00+00', '2023-08-25 15:00:00+00', $4),
    ($1, $2, 'sercommindoor', '2023-08-25 16:00:00+00', '2023-08-25 16:00:00+00', $4),
    ($1, $3, 'sercommindoor', '2023-08-25 17:00:00+00', '2023-08-25 17:00:00+00', $4),
    ($1, $2, 'sercommindoor', '2023-08-25 18:00:00+00', '2023-08-25 18:00:00+00', $4),
    ($1, $3, 'sercommindoor', '2023-08-25 19:00:00+00', '2023-08-25 19:00:00+00', $4),
    ($1, $2, 'sercommindoor', '2023-08-25 20:00:00+00', '2023-08-25 20:00:00+00', $4),
    ($1, $3, 'sercommindoor', '2023-08-25 21:00:00+00', '2023-08-25 21:00:00+00', $4),
    ($1, $2, 'sercommindoor', '2023-08-25 22:00:00+00', '2023-08-25 22:00:00+00', $4),
    ($1, $3, 'sercommindoor', '2023-08-25 23:00:00+00', '2023-08-25 23:00:00+00', $4)
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
    let latest_timestamp: DateTime<Utc> = "2023-08-25 23:00:00.000000000 UTC".parse()?;
    let max_asserted_distance_deviation: u32 = 300;

    let heartbeat_reward: Vec<_> = HeartbeatReward::validated(
        &pool,
        &(start_period..end_period),
        max_asserted_distance_deviation,
    )
    .try_collect()
    .await?;

    assert_eq!(
        heartbeat_reward,
        vec![HeartbeatReward {
            hotspot_key: hotspot_2,
            cell_type,
            cbsd_id: Some(cbsd_id),
            reward_weight: Decimal::ONE,
            latest_timestamp,
            coverage_object: Some(coverage_object),
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
INSERT INTO cbrs_heartbeats (cbsd_id, hotspot_key, cell_type, latest_timestamp, truncated_timestamp, coverage_object)
VALUES
    ($1, $2, 'sercommindoor', '2023-08-25 00:00:00+00', '2023-08-25 00:00:00+00', $4),
    ($1, $2, 'sercommindoor', '2023-08-25 01:00:00+00', '2023-08-25 01:00:00+00', $4),
    ($1, $2, 'sercommindoor', '2023-08-25 02:00:00+00', '2023-08-25 02:00:00+00', $4),
    ($1, $2, 'sercommindoor', '2023-08-25 03:00:00+00', '2023-08-25 03:00:00+00', $4),
    ($1, $2, 'sercommindoor', '2023-08-25 04:00:00+00', '2023-08-25 04:00:00+00', $4),
    ($1, $2, 'sercommindoor', '2023-08-25 05:00:00+00', '2023-08-25 05:00:00+00', $4),
    ($1, $2, 'sercommindoor', '2023-08-25 06:00:00+00', '2023-08-25 06:00:00+00', $4),
    ($1, $2, 'sercommindoor', '2023-08-25 07:00:00+00', '2023-08-25 07:00:00+00', $4),
    ($1, $2, 'sercommindoor', '2023-08-25 08:00:00+00', '2023-08-25 08:00:00+00', $4),
    ($1, $2, 'sercommindoor', '2023-08-25 09:00:00+00', '2023-08-25 09:00:00+00', $4),
    ($1, $2, 'sercommindoor', '2023-08-25 10:00:00+00', '2023-08-25 10:00:00+00', $4),
    ($1, $3, 'sercommindoor', '2023-08-25 11:00:00+00', '2023-08-25 11:00:00+00', $4)
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
    let latest_timestamp: DateTime<Utc> = "2023-08-25 11:00:00.000000000 UTC".parse()?;
    let max_asserted_distance_deviation: u32 = 300;
    let heartbeat_reward: Vec<_> = HeartbeatReward::validated(
        &pool,
        &(start_period..end_period),
        max_asserted_distance_deviation,
    )
    .try_collect()
    .await?;

    assert_eq!(
        heartbeat_reward,
        vec![HeartbeatReward {
            hotspot_key: hotspot_2,
            cell_type,
            cbsd_id: Some(cbsd_id),
            reward_weight: Decimal::ONE,
            latest_timestamp,
            coverage_object: Some(coverage_object),
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
INSERT INTO cbrs_heartbeats (cbsd_id, hotspot_key, cell_type, latest_timestamp, truncated_timestamp, coverage_object)
VALUES
    ($1, $2, 'sercommindoor', '2023-08-25 00:00:00+00', '2023-08-25 00:00:00+00', $3),
    ($1, $2, 'sercommindoor', '2023-08-25 01:00:00+00', '2023-08-25 01:00:00+00', $3),
    ($1, $2, 'sercommindoor', '2023-08-25 02:00:00+00', '2023-08-25 02:00:00+00', $3),
    ($1, $2, 'sercommindoor', '2023-08-25 03:00:00+00', '2023-08-25 03:00:00+00', $3),
    ($1, $2, 'sercommindoor', '2023-08-25 04:00:00+00', '2023-08-25 04:00:00+00', $3),
    ($1, $2, 'sercommindoor', '2023-08-25 05:00:00+00', '2023-08-25 05:00:00+00', $3),
    ($1, $2, 'sercommindoor', '2023-08-25 06:00:00+00', '2023-08-25 06:00:00+00', $3),
    ($1, $2, 'sercommindoor', '2023-08-25 07:00:00+00', '2023-08-25 07:00:00+00', $3),
    ($1, $2, 'sercommindoor', '2023-08-25 08:00:00+00', '2023-08-25 08:00:00+00', $3),
    ($1, $2, 'sercommindoor', '2023-08-25 09:00:00+00', '2023-08-25 09:00:00+00', $3),
    ($1, $2, 'sercommindoor', '2023-08-25 10:00:00+00', '2023-08-25 10:00:00+00', $3)
"#,
    )
    .bind(&cbsd_id)
    .bind(&hotspot)
    .bind(coverage_object)
    .execute(&pool)
    .await?;

    let start_period: DateTime<Utc> = "2023-08-25 00:00:00.000000000 UTC".parse()?;
    let end_period: DateTime<Utc> = "2023-08-26 00:00:00.000000000 UTC".parse()?;
    let max_asserted_distance_deviation: u32 = 300;

    let heartbeat_reward: Vec<_> = HeartbeatReward::validated(
        &pool,
        &(start_period..end_period),
        max_asserted_distance_deviation,
    )
    .try_collect()
    .await?;

    assert!(heartbeat_reward.is_empty());

    Ok(())
}
