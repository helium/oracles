use crate::diff::{DiffTable, FloatEq, DEFAULT_F64_EPSILON};
use crate::CommonArgs;
use anyhow::{Context, Result};
use helium_crypto::PublicKeyBinary;
use sqlx::{Pool, Postgres, Row};
use std::collections::BTreeMap;
use trino_client::Client as TrinoClient;

/// Bytes/DC pending in the mobile_packet_verifier database. There is no Trino
/// equivalent — "pending" sessions are by definition unprocessed and so have
/// no row in `rewards.data_transfer`. We still run the diff harness so the
/// output format stays consistent, but the Trino column is always empty here.
#[derive(Debug, Clone, Copy, Default, PartialEq)]
struct PendingTotal {
    rewardable_bytes: u64,
}

impl FloatEq for PendingTotal {
    fn float_eq(&self, other: &Self, _: f64) -> bool {
        self == other
    }
}

/// Copy of `mobile_packet_verifier::pending_burns::initialize`'s SQL. We
/// duplicate rather than pull `mobile_packet_verifier` as a dep — the function
/// is two lines of SQL and a cross-binary dep just for a diff CLI is heavy.
const PG_SQL: &str = r#"
    SELECT payer, sum(rewardable_bytes)::bigint AS total_rewardable_bytes
    FROM data_transfer_sessions
    GROUP BY payer
"#;

pub async fn run(mpv_pg: &Pool<Postgres>, _trino: &TrinoClient, args: &CommonArgs) -> Result<()> {
    let rows = sqlx::query(PG_SQL)
        .fetch_all(mpv_pg)
        .await
        .context("running mobile_packet_verifier pending_burns initialize SQL")?;
    let pg_rows: BTreeMap<String, PendingTotal> = rows
        .into_iter()
        .map(|row| {
            let payer: PublicKeyBinary = row.get("payer");
            let total: i64 = row.get("total_rewardable_bytes");
            (
                payer.to_string(),
                PendingTotal {
                    rewardable_bytes: total.max(0) as u64,
                },
            )
        })
        .collect();
    let trino_rows: BTreeMap<String, PendingTotal> = BTreeMap::new();

    let mut table = DiffTable::new(
        "pending-burns — mobile_packet_verifier initialize() (no Trino counterpart)",
        "payer",
        "pending_bytes",
    )
    .with_note(
        "Pending sessions are unprocessed by design — no equivalent Trino table. All rows will show as PG_ONLY.",
    )
    .with_options(true, args.limit); // force show_all=true since everything is PG_ONLY
    table.fill(
        pg_rows,
        trino_rows,
        DEFAULT_F64_EPSILON,
        |k| k.clone(),
        |v| v.rewardable_bytes.to_string(),
        |_p, _t| String::new(),
    );
    table.print();
    Ok(())
}
