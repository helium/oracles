use std::{
    cmp::Ordering,
    collections::{BTreeMap, BinaryHeap, HashMap},
    sync::Arc,
};

use chrono::{DateTime, Utc};
use file_store::{coverage::CoverageObjectIngestReport, file_info_poller::FileInfoStream};
use futures::stream::{StreamExt, TryStreamExt};
use h3o::CellIndex;
use helium_crypto::PublicKeyBinary;
use helium_proto::services::poc_mobile::SignalLevel as SignalLevelProto;
use mobile_config::client::AuthorizationClient;
use retainer::{entry::CacheReadGuard, Cache};
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use sqlx::{FromRow, Pool, Postgres, Type};
use tokio::sync::mpsc::Receiver;
use uuid::Uuid;

use crate::heartbeats::HeartbeatReward;

#[derive(Copy, Clone, PartialOrd, Ord, PartialEq, Eq, Type)]
pub enum SignalLevel {
    No,
    Low,
    Medium,
    High,
}
/*
pub struct CoverageCache {
    pool: Pool<Postgres>,
    objs: Arc<Cache<Uuid, CoverageObject>>,
}

impl CoverageCache {
    pub fn get(&self, key: &Uuid) -> CacheReadGuard<'_, CoverageObject> {
        todo!()
    }
}
*/

pub struct CoverageDaemon {
    pool: Pool<Postgres>,
    //    cache: CoverageCache,
    auth_client: AuthorizationClient,
    coverages_objs: Receiver<FileInfoStream<CoverageObjectIngestReport>>,
}

#[derive(FromRow)]
pub struct HexCoverage {
    uuid: Uuid,
    hex: i64,
    indoor: bool,
    cbsd_id: String,
    signal_level: SignalLevel,
    coverage_claim_time: DateTime<Utc>,
}

#[derive(Eq, Ord)]
struct CoverageLevel {
    coverage_claim_time: DateTime<Utc>,
    hotspot: PublicKeyBinary,
    indoor: bool,
    signal_level: SignalLevel,
}

impl PartialEq for CoverageLevel {
    fn eq(&self, other: &Self) -> bool {
        self.coverage_claim_time == other.coverage_claim_time
    }
}

impl PartialOrd for CoverageLevel {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.coverage_claim_time.cmp(&other.coverage_claim_time))
    }
}

impl CoverageLevel {
    fn coverage_points(&self) -> anyhow::Result<Decimal> {
        Ok(match (self.indoor, self.signal_level) {
            (true, SignalLevel::High) => dec!(400),
            (true, SignalLevel::Low) => dec!(100),
            (false, SignalLevel::High) => dec!(16),
            (false, SignalLevel::Medium) => dec!(8),
            (false, SignalLevel::Low) => dec!(4),
            (_, SignalLevel::No) => dec!(0),
            _ => anyhow::bail!("Indoor radio cannot have a signal level of medium"),
        })
    }
}

pub struct CoverageReward {
    points: Decimal,
    hotspot: PublicKeyBinary,
}

pub const MAX_RADIOS_PER_HEX: usize = 5;

pub struct CoveredHexes {
    hexes: HashMap<CellIndex, BTreeMap<SignalLevel, BinaryHeap<CoverageLevel>>>,
}

impl CoveredHexes {
    pub async fn fetch_heartbeat_coverage(
        &mut self,
        pool: &Pool<Postgres>,
        hotspot: &PublicKeyBinary,
        cbsd_id: &str,
        coverage_obj: &Uuid,
    ) -> Result<(), sqlx::Error> {
        let mut covered_hexes =
            sqlx::query_as("SELECT * FROM coverage WHERE cbsd_id = $1 AND uuid = $2")
                .bind(cbsd_id)
                .bind(coverage_obj)
                .fetch(pool);

        while let Some(HexCoverage {
            hex,
            indoor,
            signal_level,
            coverage_claim_time,
            ..
        }) = covered_hexes.next().await.transpose()?
        {
            self.hexes
                .entry(CellIndex::try_from(hex as u64).unwrap())
                .or_default()
                .entry(signal_level)
                .or_default()
                .push(CoverageLevel {
                    coverage_claim_time,
                    indoor,
                    signal_level,
                    hotspot: hotspot.clone(),
                });
        }

        Ok(())
    }

    pub fn into_iter(self) -> impl Iterator<Item = CoverageReward> {
        self.hexes
            .into_values()
            .map(|mut radios| {
                radios.pop_last().map(|(_, radios)| {
                    radios
                        .into_sorted_vec()
                        .into_iter()
                        .rev()
                        .take(MAX_RADIOS_PER_HEX)
                        .map(|cl| CoverageReward {
                            points: cl.coverage_points().unwrap(),
                            hotspot: cl.hotspot,
                        })
                })
            })
            .flatten()
            .flatten()
    }
}
