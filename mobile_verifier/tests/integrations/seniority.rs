use chrono::{DateTime, Utc};
use helium_proto::services::poc_mobile::{HeartbeatValidity, SeniorityUpdateReason};
use mobile_verifier::cell_type::CellType;
use mobile_verifier::heartbeats::{HbType, Heartbeat, ValidatedHeartbeat};
use mobile_verifier::seniority::{Seniority, SeniorityUpdate, SeniorityUpdateAction};
use rust_decimal_macros::dec;
use sqlx::PgPool;
use uuid::Uuid;

#[sqlx::test]
async fn test_seniority_updates(pool: PgPool) -> anyhow::Result<()> {
    let coverage_object = Uuid::new_v4();
    let mut heartbeat = ValidatedHeartbeat {
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
    let latest_seniority =
        Seniority::fetch_latest(heartbeat.heartbeat.key(), &mut transaction).await?;

    assert_eq!(latest_seniority, None);

    let action1 = SeniorityUpdate::from_heartbeat(
        &heartbeat,
        SeniorityUpdateAction::Insert {
            new_seniority: "2023-08-25 00:00:00.000000000 UTC".parse().unwrap(),
            update_reason: SeniorityUpdateReason::HeartbeatNotSeen,
        },
    )?;

    action1.execute(&mut transaction).await?;

    let latest_seniority = Seniority::fetch_latest(heartbeat.heartbeat.key(), &mut transaction)
        .await?
        .unwrap();

    let expected_seniority_ts: DateTime<Utc> = "2023-08-25 00:00:00.000000000 UTC".parse().unwrap();
    let expected_last_heartbeat: DateTime<Utc> =
        "2023-08-23 00:00:00.000000000 UTC".parse().unwrap();

    assert_eq!(latest_seniority.uuid, coverage_object,);
    assert_eq!(latest_seniority.seniority_ts, expected_seniority_ts,);
    assert_eq!(latest_seniority.last_heartbeat, expected_last_heartbeat,);

    heartbeat.heartbeat.timestamp = "2023-08-24 00:00:00.000000000 UTC".parse().unwrap();

    let action2 = SeniorityUpdate::from_heartbeat(
        &heartbeat,
        SeniorityUpdateAction::Update {
            curr_seniority: "2023-08-25 00:00:00.000000000 UTC".parse().unwrap(),
        },
    )?;

    action2.execute(&mut transaction).await?;

    let latest_seniority = Seniority::fetch_latest(heartbeat.heartbeat.key(), &mut transaction)
        .await?
        .unwrap();

    let expected_last_heartbeat: DateTime<Utc> =
        "2023-08-24 00:00:00.000000000 UTC".parse().unwrap();

    assert_eq!(latest_seniority.last_heartbeat, expected_last_heartbeat,);

    Ok(())
}
