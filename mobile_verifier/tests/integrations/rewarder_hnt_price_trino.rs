//! End-to-end test of the HNT price recovery against a real Trino, seeding the
//! Solana on-chain epoch tables the rewarder reads from.
//!
//! The production query targets `solana.public.dao_epoch_infos` /
//! `sub_dao_epoch_infos` (a Postgres-connector catalog). The test harness only
//! serves iceberg tables in a per-test catalog, so we recreate those tables here
//! (columns as varchar, matching the indexer) and point the query at
//! `<catalog>.public` via `price_in_bones_for_epoch`'s `schema` parameter —
//! production passes [`hnt_price::SOLANA_SCHEMA`].

use helium_iceberg::{FieldDefinition, IcebergTestHarness, PartitionDefinition, TableDefinition};
use mobile_verifier::rewarder::hnt_price;
use serde::Serialize;

// Real on-chain addresses from the prod Solana indexer — the values the
// production query filters on (see the `resolvers_match_onchain_addresses`
// canary in the crate lib). IOT is the decoy sub-DAO that must be ignored.
const DAO: &str = "BQ3MCuTT5zVBhNfQ4SjMh3NPVhFy73MPV8rjfq5d1zie";
const MOBILE_SUB_DAO: &str = "Gm9xDCJawDEKDrrQW6haw94gABaYzQwCq4ZQU8h8bd22";
const IOT_SUB_DAO: &str = "39Lw1RH6zt8AJvKn3BTxmUDofzduCM2J3kSaGDZ8L7Sk";

/// Namespace the seeded tables live in, mirroring production's `public` schema.
const NAMESPACE: &str = "public";

// Both magnitudes are u64 on-chain but varchar in the indexer catalog, so the
// seed columns are strings — exactly what the recovery parses.
#[derive(Serialize)]
struct DaoEpochInfo {
    epoch: String,
    dao: String,
    deployer_cap_hnt: String,
}

#[derive(Serialize)]
struct SubDaoEpochInfo {
    epoch: String,
    sub_dao: String,
    dc_burned: String,
}

fn dao_epoch_infos_table() -> anyhow::Result<TableDefinition> {
    Ok(TableDefinition::builder(NAMESPACE, "dao_epoch_infos")
        .with_fields([
            FieldDefinition::required_string("epoch"),
            FieldDefinition::required_string("dao"),
            FieldDefinition::required_string("deployer_cap_hnt"),
        ])
        .with_partition(PartitionDefinition::identity("epoch"))
        .build()?)
}

fn sub_dao_epoch_infos_table() -> anyhow::Result<TableDefinition> {
    Ok(TableDefinition::builder(NAMESPACE, "sub_dao_epoch_infos")
        .with_fields([
            FieldDefinition::required_string("epoch"),
            FieldDefinition::required_string("sub_dao"),
            FieldDefinition::required_string("dc_burned"),
        ])
        .with_partition(PartitionDefinition::identity("epoch"))
        .build()?)
}

async fn harness() -> anyhow::Result<IcebergTestHarness> {
    Ok(IcebergTestHarness::new_with_tables([
        dao_epoch_infos_table()?,
        sub_dao_epoch_infos_table()?,
    ])
    .await?)
}

/// The query's catalog.schema qualifier: this test's dedicated catalog plus the
/// namespace the tables were created in (production uses `hnt_price::SOLANA_SCHEMA`).
fn schema(h: &IcebergTestHarness) -> String {
    format!("{}.{}", h.catalog_name(), NAMESPACE)
}

async fn seed_dao(h: &IcebergTestHarness, id: &str, rows: Vec<DaoEpochInfo>) -> anyhow::Result<()> {
    h.get_table_writer_in::<DaoEpochInfo>(NAMESPACE, "dao_epoch_infos")
        .await?
        .write_idempotent(id, rows)
        .await?;
    Ok(())
}

async fn seed_sub_dao(
    h: &IcebergTestHarness,
    id: &str,
    rows: Vec<SubDaoEpochInfo>,
) -> anyhow::Result<()> {
    h.get_table_writer_in::<SubDaoEpochInfo>(NAMESPACE, "sub_dao_epoch_infos")
        .await?
        .write_idempotent(id, rows)
        .await?;
    Ok(())
}

fn dao_row(epoch: &str, deployer_cap_hnt: &str) -> DaoEpochInfo {
    DaoEpochInfo {
        epoch: epoch.into(),
        dao: DAO.into(),
        deployer_cap_hnt: deployer_cap_hnt.into(),
    }
}

fn sub_dao_row(epoch: &str, sub_dao: &str, dc_burned: &str) -> SubDaoEpochInfo {
    SubDaoEpochInfo {
        epoch: epoch.into(),
        sub_dao: sub_dao.into(),
        dc_burned: dc_burned.into(),
    }
}

async fn recover(h: &IcebergTestHarness, epoch: u64) -> anyhow::Result<Option<u64>> {
    // The query derives the mobile sub-DAO / HNT DAO internally, so the seeded
    // rows use those real addresses (see the DAO / MOBILE_SUB_DAO consts).
    let client = trino_client::Client::from_client(h.owned_trino().await?);
    hnt_price::price_in_bones_for_epoch(&client, &schema(h), epoch).await
}

#[tokio::test]
async fn recovers_price_from_seeded_backstop() -> anyhow::Result<()> {
    let h = harness().await?;

    // Two consecutive epochs of real prod data: dao_epoch_infos.deployer_cap_hnt
    // and the mobile sub_dao_epoch_infos.dc_burned, each alongside the IOT
    // sub-DAO's burn for the same epoch — which must be ignored, since only the
    // mobile sub-DAO's dc_burned feeds the mobile deployer cap. (Using IOT's tiny
    // burn would recover a wildly wrong price, ~$0.0037.)
    seed_dao(
        &h,
        "dao",
        vec![
            dao_row("20654", "11573616090838"),
            dao_row("20655", "11384926369100"),
        ],
    )
    .await?;
    seed_sub_dao(
        &h,
        "sub_dao",
        vec![
            sub_dao_row("20654", MOBILE_SUB_DAO, "785184940"),
            sub_dao_row("20654", IOT_SUB_DAO, "14164317"),
            sub_dao_row("20655", MOBILE_SUB_DAO, "783065558"),
            sub_dao_row("20655", IOT_SUB_DAO, "13285338"),
        ],
    )
    .await?;

    // price_in_bones = dc_burned * 3 * 10^11 / deployer_cap_hnt.
    assert_eq!(
        recover(&h, 20654).await?,
        Some(20_352_799), // $0.20352799 / HNT
    );
    assert_eq!(
        recover(&h, 20655).await?,
        Some(20_634_272), // $0.20634272 / HNT
    );
    Ok(())
}

#[tokio::test]
async fn zero_deployer_cap_reads_as_not_ready() -> anyhow::Result<()> {
    let h = harness().await?;

    // The dao row exists but the cap hasn't been written yet (defaults to "0").
    seed_dao(&h, "dao", vec![dao_row("20654", "0")]).await?;
    seed_sub_dao(
        &h,
        "sub_dao",
        vec![sub_dao_row("20654", MOBILE_SUB_DAO, "785184940")],
    )
    .await?;

    assert_eq!(
        recover(&h, 20654).await?,
        None,
        "zero cap -> price unrecoverable, rewarder waits & retries"
    );
    Ok(())
}

#[tokio::test]
async fn missing_epoch_reads_as_not_ready() -> anyhow::Result<()> {
    let h = harness().await?;

    // Only epoch 20654 is seeded; asking for 20655 finds no rows to join.
    seed_dao(&h, "dao", vec![dao_row("20654", "11573616090838")]).await?;
    seed_sub_dao(
        &h,
        "sub_dao",
        vec![sub_dao_row("20654", MOBILE_SUB_DAO, "785184940")],
    )
    .await?;

    assert_eq!(recover(&h, 20655).await?, None);
    Ok(())
}
