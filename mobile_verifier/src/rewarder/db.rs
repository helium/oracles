use std::ops::Range;

use chrono::{DateTime, Utc};
use sqlx::PgPool;

pub async fn no_cbrs_heartbeats(
    pool: &PgPool,
    reward_period: &Range<DateTime<Utc>>,
) -> anyhow::Result<bool> {
    let count = sqlx::query_scalar::<_, i64>(
        "SELECT COUNT(*) FROM cbrs_heartbeats WHERE latest_timestamp >= $1",
    )
    .bind(reward_period.end)
    .fetch_one(pool)
    .await?;

    Ok(count == 0)
}

pub async fn no_wifi_heartbeats(
    pool: &PgPool,
    reward_period: &Range<DateTime<Utc>>,
) -> anyhow::Result<bool> {
    let count = sqlx::query_scalar::<_, i64>(
        "SELECT COUNT(*) FROM wifi_heartbeats WHERE latest_timestamp >= $1",
    )
    .bind(reward_period.end)
    .fetch_one(pool)
    .await?;

    Ok(count == 0)
}

pub async fn no_speedtests(
    pool: &PgPool,
    reward_period: &Range<DateTime<Utc>>,
) -> Result<bool, anyhow::Error> {
    let count =
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM speedtests WHERE timestamp >= $1")
            .bind(reward_period.end)
            .fetch_one(pool)
            .await?;

    Ok(count == 0)
}

pub async fn no_unique_connections(
    pool: &PgPool,
    reward_period: &Range<DateTime<Utc>>,
) -> anyhow::Result<bool> {
    let count = sqlx::query_scalar::<_, i64>(
        "SELECT COUNT(*) from unique_connections WHERE received_timestamp >= $1",
    )
    .bind(reward_period.end)
    .fetch_one(pool)
    .await?;

    Ok(count == 0)
}

#[cfg(test)]
mod tests {

    use helium_crypto::{KeyTag, Keypair, PublicKeyBinary};
    use rand::rngs::OsRng;
    use rust_decimal_macros::dec;

    use crate::{cell_type, heartbeats, speedtests, unique_connections};

    mod file_store {
        pub use file_store::{
            speedtest::CellSpeedtest,
            unique_connections::{UniqueConnectionReq, UniqueConnectionsIngestReport},
        };
    }

    mod proto {
        pub use helium_proto::services::poc_mobile::{HeartbeatValidity, LocationSource};
    }

    use super::*;

    #[sqlx::test]
    async fn test_empty_db(pool: PgPool) -> anyhow::Result<()> {
        let reward_period = Utc::now() - chrono::Duration::days(1)..Utc::now();

        // Reports not found
        assert!(no_cbrs_heartbeats(&pool, &reward_period).await?);
        assert!(no_wifi_heartbeats(&pool, &reward_period).await?);
        assert!(no_speedtests(&pool, &reward_period).await?);
        assert!(no_unique_connections(&pool, &reward_period).await?);

        Ok(())
    }

    #[sqlx::test]
    async fn test_single_report_from_today(pool: PgPool) -> anyhow::Result<()> {
        let reward_period = Utc::now() - chrono::Duration::days(1)..Utc::now();

        let (cbrs_heartbeat, wifi_heartbeat, speedtest, unique_connection) =
            create_with_timestamp(Utc::now());

        let mut txn = pool.begin().await?;
        cbrs_heartbeat.save(&mut txn).await?;
        wifi_heartbeat.save(&mut txn).await?;
        speedtests::save_speedtest(&speedtest, &mut txn).await?;
        unique_connections::db::save(&mut txn, &[unique_connection]).await?;
        txn.commit().await?;

        // Reports found
        assert!(!no_cbrs_heartbeats(&pool, &reward_period).await?);
        assert!(!no_wifi_heartbeats(&pool, &reward_period).await?);
        assert!(!no_speedtests(&pool, &reward_period).await?);
        assert!(!no_unique_connections(&pool, &reward_period).await?);

        Ok(())
    }

    #[sqlx::test]
    async fn test_single_report_from_yesterday(pool: PgPool) -> anyhow::Result<()> {
        let reward_period = Utc::now() - chrono::Duration::days(1)..Utc::now();

        let (cbrs_heartbeat, wifi_heartbeat, speedtest, unique_connection) =
            create_with_timestamp(Utc::now() - chrono::Duration::days(1));

        let mut txn = pool.begin().await?;
        cbrs_heartbeat.save(&mut txn).await?;
        wifi_heartbeat.save(&mut txn).await?;
        speedtests::save_speedtest(&speedtest, &mut txn).await?;
        unique_connections::db::save(&mut txn, &[unique_connection]).await?;
        txn.commit().await?;

        // Reports not found
        assert!(no_cbrs_heartbeats(&pool, &reward_period).await?);
        assert!(no_wifi_heartbeats(&pool, &reward_period).await?);
        assert!(no_speedtests(&pool, &reward_period).await?);
        assert!(no_unique_connections(&pool, &reward_period).await?);

        Ok(())
    }

    fn create_with_timestamp(
        timestamp: DateTime<Utc>,
    ) -> (
        heartbeats::ValidatedHeartbeat,
        heartbeats::ValidatedHeartbeat,
        file_store::CellSpeedtest,
        file_store::UniqueConnectionsIngestReport,
    ) {
        let cbrs_keypair = Keypair::generate(KeyTag::default(), &mut OsRng);
        let cbrs_pubkey_bin: PublicKeyBinary = cbrs_keypair.public_key().to_owned().into();

        let wifi_keypair = Keypair::generate(KeyTag::default(), &mut OsRng);
        let wifi_pubkey_bin: PublicKeyBinary = wifi_keypair.public_key().to_owned().into();

        let cbrs_heartbeat = heartbeats::ValidatedHeartbeat {
            heartbeat: heartbeats::Heartbeat {
                hb_type: heartbeats::HbType::Cbrs,
                hotspot_key: cbrs_pubkey_bin.clone(),
                cbsd_id: Some("cbsd-id".to_string()),
                operation_mode: true,
                lat: 0.0,
                lon: 0.0,
                coverage_object: Some(uuid::Uuid::new_v4()),
                location_validation_timestamp: Some(Utc::now()),
                location_source: proto::LocationSource::Asserted,
                timestamp,
            },
            cell_type: cell_type::CellType::Nova430I,
            location_trust_score_multiplier: dec!(1),
            distance_to_asserted: Some(0),
            coverage_meta: None,
            validity: proto::HeartbeatValidity::Valid,
        };

        let wifi_heartbeat = heartbeats::ValidatedHeartbeat {
            heartbeat: heartbeats::Heartbeat {
                hb_type: heartbeats::HbType::Wifi,
                hotspot_key: wifi_pubkey_bin,
                cbsd_id: None,
                operation_mode: true,
                lat: 0.0,
                lon: 0.0,
                coverage_object: Some(uuid::Uuid::new_v4()),
                location_validation_timestamp: Some(Utc::now()),
                location_source: proto::LocationSource::Asserted,
                timestamp,
            },
            cell_type: cell_type::CellType::Nova430I,
            location_trust_score_multiplier: dec!(1),
            distance_to_asserted: Some(0),
            coverage_meta: None,
            validity: proto::HeartbeatValidity::Valid,
        };

        let speedtest = file_store::CellSpeedtest {
            pubkey: cbrs_pubkey_bin.clone(),
            serial: "cbrs-serial".to_string(),
            timestamp,
            upload_speed: 1_000_000,
            download_speed: 1_000_000,
            latency: 0,
        };

        let unique_connection = file_store::UniqueConnectionsIngestReport {
            received_timestamp: timestamp,
            report: file_store::UniqueConnectionReq {
                pubkey: cbrs_pubkey_bin.clone(),
                start_timestamp: Utc::now() - chrono::Duration::days(7),
                end_timestamp: Utc::now(),
                unique_connections: 42,
                timestamp: Utc::now(),
                carrier_key: cbrs_pubkey_bin,
                signature: vec![],
            },
        };

        (cbrs_heartbeat, wifi_heartbeat, speedtest, unique_connection)
    }
}
