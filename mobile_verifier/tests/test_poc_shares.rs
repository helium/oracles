use std::{ops::Range, pin::pin};

use chrono::{DateTime, Duration, Utc};
use file_store::{
    heartbeat::{CellHeartbeat, CellHeartbeatIngestReport},
    speedtest::CellSpeedtest,
};
use futures::stream::{self, StreamExt};
use helium_crypto::PublicKeyBinary;
use mobile_verifier::{
    heartbeats::{Heartbeat, HeartbeatReward},
    reward_shares::PocShares,
    speedtests::{SpeedtestAverages, SpeedtestRollingAverage},
    HasOwner,
};
use rust_decimal_macros::dec;
use sqlx::PgPool;

#[derive(Copy, Clone)]
struct AllOwnersValid;

#[async_trait::async_trait]
impl HasOwner for AllOwnersValid {
    type Error = anyhow::Error;

    async fn has_owner(&self, _address: &PublicKeyBinary) -> Result<bool, Self::Error> {
        Ok(true)
    }
}

fn heartbeats<'a>(
    num: usize,
    start: DateTime<Utc>,
    hotspot_key: &'a PublicKeyBinary,
    cbsd_id: &'a str,
    lon: f64,
    lat: f64,
) -> impl Iterator<Item = CellHeartbeatIngestReport> + 'a {
    (0..num).map(move |i| {
        let report = CellHeartbeat {
            pubkey: hotspot_key.clone(),
            lon,
            lat,
            operation_mode: true,
            cbsd_id: cbsd_id.to_string(),
            // Unused:
            hotspot_type: String::new(),
            cell_id: 0,
            timestamp: DateTime::<Utc>::MIN_UTC,
            cbsd_category: String::new(),
        };
        CellHeartbeatIngestReport {
            report,
            received_timestamp: start + Duration::hours(i as i64),
        }
    })
}

fn bytes_per_s(mbps: u64) -> u64 {
    mbps * 125000
}

fn acceptable_speedtests(
    num: usize,
    owner: &'_ PublicKeyBinary,
    timestamp_start: DateTime<Utc>,
) -> impl Iterator<Item = CellSpeedtest> + '_ {
    (0..num).map(move |i| {
        acceptable_speedtest(owner, timestamp_start + Duration::hours((i * 12) as i64))
    })
}

fn acceptable_speedtest(owner: &PublicKeyBinary, timestamp: DateTime<Utc>) -> CellSpeedtest {
    CellSpeedtest {
        pubkey: owner.clone(),
        timestamp,
        upload_speed: bytes_per_s(10),
        download_speed: bytes_per_s(100),
        latency: 25,
        // Unused:
        serial: String::new(),
    }
}

async fn rewards(
    pool: &PgPool,
    epoch: &Range<DateTime<Utc>>,
    heartbeats: impl Iterator<Item = CellHeartbeatIngestReport>,
    speedtests: impl Iterator<Item = CellSpeedtest>,
) -> anyhow::Result<PocShares> {
    let mut transaction = pool.begin().await?;

    // Heartbeats:
    let mut heartbeats = pin!(Heartbeat::validate_heartbeats(
        &AllOwnersValid,
        stream::iter(heartbeats),
        epoch
    ));
    while let Some(heartbeat) = heartbeats.next().await.transpose()? {
        heartbeat.save(&mut transaction).await?;
    }

    // Speedtests:
    let mut speedtests = pin!(
        SpeedtestRollingAverage::validate_speedtests(
            &AllOwnersValid,
            stream::iter(speedtests),
            &mut transaction,
        )
        .await?
    );
    while let Some(speedtest) = speedtests.next().await.transpose()? {
        speedtest.save(&mut transaction).await?;
    }

    transaction.commit().await?;

    let heartbeats = HeartbeatReward::validated(pool, epoch);
    let speedtests = SpeedtestAverages::validated(pool, epoch.end).await?;
    let poc_rewards = PocShares::aggregate(heartbeats, speedtests).await?;

    Ok(poc_rewards)
}

#[sqlx::test]
#[ignore]
pub async fn poc_shares_simple_integration_test(pool: PgPool) -> anyhow::Result<()> {
    let reward_start: DateTime<Utc> = "2023-08-14 01:00:00+0000".parse()?;
    let reward_end: DateTime<Utc> = "2023-08-15 01:00:00+0000".parse()?;
    let c1 = "P27-SCE4255W2107CW5000015";
    let owner1 = "112NqN2WWMwtK29PMzRby62fDydBJfsCLkCAf392stdok48ovNT6".parse()?;
    let c1_hbs = heartbeats(12, reward_start, &owner1, c1, 0.0, 0.0);
    let c1_spd = acceptable_speedtests(2, &owner1, reward_start);
    let c2 = "2AG32PBS3101S1202000464223GY0153";
    let owner2 = "118SPA16MX8WrUKcuXxsg6SH8u5dWszAySiUAJX6tTVoQVy7nWc".parse()?;
    let c2_hbs = heartbeats(12, reward_start, &owner2, c2, 0.0, 0.0);
    let c2_spd = acceptable_speedtests(2, &owner2, reward_start);

    let rewards = rewards(
        &pool,
        &(reward_start..reward_end),
        c1_hbs.chain(c2_hbs),
        c1_spd.chain(c2_spd),
    )
    .await?;

    // The SercommIndoor has a lower reward weight than the Nova430I:
    assert!(
        rewards.hotspot_shares.get(&owner2).unwrap().total_shares()
            > rewards.hotspot_shares.get(&owner1).unwrap().total_shares()
    );

    // Both should be greater than zero:
    assert!(rewards.hotspot_shares.get(&owner1).unwrap().total_shares() > dec!(0));
    assert!(rewards.hotspot_shares.get(&owner2).unwrap().total_shares() > dec!(0));

    Ok(())
}
