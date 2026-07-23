//! End-to-end test of the epoch reward-info + price resolution against a real
//! Trino, seeding the Solana on-chain epoch tables the rewarder reads from.
//!
//! The production query targets `solana.public.dao_epoch_infos` /
//! `sub_dao_epoch_infos` (a Postgres-connector catalog). The test harness only
//! serves iceberg tables in a per-test catalog, so we recreate those tables here
//! (columns as varchar, matching the indexer) and point the query at
//! `<catalog>.public` via `resolve`'s `schema` parameter — production passes
//! [`epoch_reward_info::SOLANA_SCHEMA`].

use helium_iceberg::{FieldDefinition, IcebergTestHarness, PartitionDefinition, TableDefinition};
use mobile_verifier::rewarder::epoch_reward_info::{self, ResolvedEpoch};
use rust_decimal::Decimal;
use serde::Serialize;

// Real on-chain addresses from the prod Solana indexer — the values the
// production query filters on (see the `resolvers_match_onchain_addresses`
// canary in the crate lib). IOT is the decoy sub-DAO that must be ignored.
const DAO: &str = "BQ3MCuTT5zVBhNfQ4SjMh3NPVhFy73MPV8rjfq5d1zie";
const MOBILE_SUB_DAO: &str = "Gm9xDCJawDEKDrrQW6haw94gABaYzQwCq4ZQU8h8bd22";
const IOT_SUB_DAO: &str = "39Lw1RH6zt8AJvKn3BTxmUDofzduCM2J3kSaGDZ8L7Sk";

/// Namespace the seeded tables live in, mirroring production's `public` schema.
const NAMESPACE: &str = "public";

// Indexer big-ints are varchar, so the seed columns are strings — exactly what
// the resolver parses.
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
    address: String,
    dc_burned: String,
    hnt_rewards_issued: String,
    delegation_rewards_issued: String,
    rewards_issued_at: String,
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
            FieldDefinition::required_string("address"),
            FieldDefinition::required_string("dc_burned"),
            FieldDefinition::required_string("hnt_rewards_issued"),
            FieldDefinition::required_string("delegation_rewards_issued"),
            FieldDefinition::required_string("rewards_issued_at"),
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
/// namespace the tables were created in (production uses `epoch_reward_info::SOLANA_SCHEMA`).
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

fn sub_dao_row(
    epoch: &str,
    sub_dao: &str,
    address: &str,
    dc_burned: &str,
    hnt_rewards_issued: &str,
    delegation_rewards_issued: &str,
    rewards_issued_at: &str,
) -> SubDaoEpochInfo {
    SubDaoEpochInfo {
        epoch: epoch.into(),
        sub_dao: sub_dao.into(),
        address: address.into(),
        dc_burned: dc_burned.into(),
        hnt_rewards_issued: hnt_rewards_issued.into(),
        delegation_rewards_issued: delegation_rewards_issued.into(),
        rewards_issued_at: rewards_issued_at.into(),
    }
}

async fn resolve_epoch(
    h: &IcebergTestHarness,
    epoch: u64,
) -> anyhow::Result<Option<ResolvedEpoch>> {
    // The query derives the mobile sub-DAO / HNT DAO internally, so the seeded
    // rows use those real addresses (see the DAO / MOBILE_SUB_DAO consts).
    let client = trino_client::Client::from_client(h.owned_trino().await?);
    epoch_reward_info::resolve(&client, &schema(h), epoch).await
}

#[tokio::test]
async fn resolves_reward_info_and_price_from_seeded_epochs() -> anyhow::Result<()> {
    let h = harness().await?;

    // Two consecutive epochs of real prod data, each with the mobile and the IOT
    // sub-DAO rows. Only the mobile row feeds the result — the IOT decoy (its own
    // smaller burn + rewards) must be ignored.
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
            sub_dao_row(
                "20654",
                MOBILE_SUB_DAO,
                "aKtGx8Hf4FMDLm3Xbp4UGGP8UFRw4Azo71VscGcRum5",
                "785184940",
                "2599729243320",
                "165940164467",
                "1784592034",
            ),
            sub_dao_row(
                "20654",
                IOT_SUB_DAO,
                "2oLR5eYkFdvvRoaQ1L3V1cDjCeFNmiQE67GkGkN5ZW9N",
                "14164317",
                "301412090426",
                "19239069601",
                "1784592033",
            ),
            sub_dao_row(
                "20655",
                MOBILE_SUB_DAO,
                "HiwwwiEzHt7nuyXyXyqND1PUsnWdGywbU1XorDtpZ1Ax",
                "783065558",
                "2558164463035",
                "163287093385",
                "1784678401",
            ),
            sub_dao_row(
                "20655",
                IOT_SUB_DAO,
                "7jNw8VU8n9kDergpgjzAKG4jj11ksUGHQyE3zD3eaxQd",
                "13285338",
                "296382827412",
                "18918052813",
                "1784678401",
            ),
        ],
    )
    .await?;

    let e654 = resolve_epoch(&h, 20654).await?.expect("20654 resolved");
    assert_eq!(e654.price_in_bones, 20_352_799); // $0.20352799 / HNT
    assert_eq!(e654.reward_info.epoch_day, 20654);
    assert_eq!(
        e654.reward_info.hnt_rewards_issued,
        Decimal::from(2_599_729_243_320u64)
    );
    // epoch_emissions = hnt_rewards_issued + delegation_rewards_issued.
    assert_eq!(
        e654.reward_info.epoch_emissions,
        Decimal::from(2_765_669_407_787u64)
    );

    let e655 = resolve_epoch(&h, 20655).await?.expect("20655 resolved");
    assert_eq!(e655.price_in_bones, 20_634_272); // $0.20634272 / HNT
    assert_eq!(
        e655.reward_info.hnt_rewards_issued,
        Decimal::from(2_558_164_463_035u64)
    );
    assert_eq!(
        e655.reward_info.epoch_emissions,
        Decimal::from(2_721_451_556_420u64)
    );

    Ok(())
}

#[tokio::test]
async fn not_closed_epoch_reads_as_not_ready() -> anyhow::Result<()> {
    let h = harness().await?;

    // Cap written but no rewards issued yet (hnt & delegation both "0") — the
    // epoch hasn't closed, so there's nothing to reward.
    seed_dao(&h, "dao", vec![dao_row("20654", "11573616090838")]).await?;
    seed_sub_dao(
        &h,
        "sub_dao",
        vec![sub_dao_row(
            "20654",
            MOBILE_SUB_DAO,
            "addr",
            "785184940",
            "0",
            "0",
            "1784592034",
        )],
    )
    .await?;

    assert!(
        resolve_epoch(&h, 20654).await?.is_none(),
        "no rewards issued -> epoch not closed -> not ready"
    );
    Ok(())
}

#[tokio::test]
async fn zero_deployer_cap_reads_as_not_ready() -> anyhow::Result<()> {
    let h = harness().await?;

    // Rewards issued (epoch closed) but the deployer cap hasn't been written
    // ("0"), so the price can't be recovered — wait & retry.
    seed_dao(&h, "dao", vec![dao_row("20654", "0")]).await?;
    seed_sub_dao(
        &h,
        "sub_dao",
        vec![sub_dao_row(
            "20654",
            MOBILE_SUB_DAO,
            "addr",
            "785184940",
            "2599729243320",
            "165940164467",
            "1784592034",
        )],
    )
    .await?;

    assert!(
        resolve_epoch(&h, 20654).await?.is_none(),
        "zero cap -> price unrecoverable -> not ready"
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
        vec![sub_dao_row(
            "20654",
            MOBILE_SUB_DAO,
            "addr",
            "785184940",
            "2599729243320",
            "165940164467",
            "1784592034",
        )],
    )
    .await?;

    assert!(resolve_epoch(&h, 20655).await?.is_none());
    Ok(())
}
