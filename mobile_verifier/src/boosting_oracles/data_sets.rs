//! Hex boosting assignment helpers.
//!
//! The data-set *downloader* daemon was removed once Proof-of-Coverage stopped
//! being rewarded (HIP-149). What remains is the code that applies hex-boost
//! assignments to the `hexes` table, kept because the coverage reward-share
//! path (now dead) and its tests still populate/read those assignment columns.
use crate::coverage::SignalLevel;
use futures::{stream::Stream, TryStreamExt};
use hex_assignments::{assignment::HexAssignments, HexBoostDataAssignmentsExt};
use sqlx::{FromRow, PgPool, QueryBuilder};
use std::{collections::HashMap, pin::pin};

pub mod db {
    use super::*;

    pub fn fetch_hexes_with_null_assignments(
        pool: &PgPool,
    ) -> impl Stream<Item = sqlx::Result<UnassignedHex>> + '_ {
        sqlx::query_as(
            "SELECT
                uuid, hex, signal_level, signal_power
            FROM
                hexes
            WHERE
                urbanized IS NULL
                OR footfall IS NULL
                OR landtype IS NULL
                OR service_provider_override IS NULL",
        )
        .fetch(pool)
    }
}

pub struct AssignedCoverageObjects {
    pub coverage_objs: HashMap<uuid::Uuid, Vec<AssignedHex>>,
}

impl AssignedCoverageObjects {
    pub async fn assign_hex_stream(
        stream: impl Stream<Item = sqlx::Result<UnassignedHex>>,
        data_sets: &impl HexBoostDataAssignmentsExt,
    ) -> anyhow::Result<Self> {
        let mut coverage_objs = HashMap::<uuid::Uuid, Vec<AssignedHex>>::new();
        let mut stream = pin!(stream);
        while let Some(hex) = stream.try_next().await? {
            let hex = hex.assign(data_sets)?;
            coverage_objs.entry(hex.uuid).or_default().push(hex);
        }
        Ok(Self { coverage_objs })
    }

    pub async fn save(self, pool: &PgPool) -> anyhow::Result<()> {
        const NUMBER_OF_FIELDS_IN_QUERY: u16 = 8;
        const ASSIGNMENTS_MAX_BATCH_ENTRIES: usize =
            (u16::MAX / NUMBER_OF_FIELDS_IN_QUERY) as usize;

        let assigned_hexes: Vec<_> = self.coverage_objs.into_values().flatten().collect();
        for assigned_hexes in assigned_hexes.chunks(ASSIGNMENTS_MAX_BATCH_ENTRIES) {
            QueryBuilder::new(
                "INSERT INTO hexes (uuid, hex, signal_level, signal_power, footfall, landtype, urbanized, service_provider_override)",
            )
                .push_values(assigned_hexes, |mut b, hex| {
                    b.push_bind(hex.uuid)
                        .push_bind(hex.hex as i64)
                        .push_bind(hex.signal_level)
                        .push_bind(hex.signal_power)
                        .push_bind(hex.assignments.footfall)
                        .push_bind(hex.assignments.landtype)
                        .push_bind(hex.assignments.urbanized)
                        .push_bind(hex.assignments.service_provider_override);
                })
                .push(
                    r#"
                    ON CONFLICT (uuid, hex) DO UPDATE SET
                        footfall = EXCLUDED.footfall,
                        landtype = EXCLUDED.landtype,
                        urbanized = EXCLUDED.urbanized,
                        service_provider_override = EXCLUDED.service_provider_override
                    "#,
                )
                .build()
                .execute(pool)
                .await?;
        }

        Ok(())
    }
}

#[derive(FromRow)]
pub struct UnassignedHex {
    uuid: uuid::Uuid,
    #[sqlx(try_from = "i64")]
    hex: u64,
    signal_level: SignalLevel,
    signal_power: i32,
}

impl UnassignedHex {
    fn assign(self, data_sets: &impl HexBoostDataAssignmentsExt) -> anyhow::Result<AssignedHex> {
        let cell = hextree::Cell::try_from(self.hex)?;

        Ok(AssignedHex {
            uuid: self.uuid,
            hex: self.hex,
            signal_level: self.signal_level,
            signal_power: self.signal_power,
            assignments: data_sets.assignments(cell)?,
        })
    }
}

pub struct AssignedHex {
    pub uuid: uuid::Uuid,
    pub hex: u64,
    pub signal_level: SignalLevel,
    pub signal_power: i32,
    pub assignments: HexAssignments,
}
