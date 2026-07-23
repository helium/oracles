//! Per-epoch reward inputs recovered from on-chain HIP-149 data via Trino.
//!
//! Each epoch the rewarder needs two things from the chain: the emissions split
//! it must distribute ([`EpochRewardInfo`]) and the HNT price to stamp on the
//! rewards. Both are read here in a single query against the Solana indexer
//! tables (`dao_epoch_infos` + the mobile `sub_dao_epoch_infos`), so they come
//! from one consistent snapshot rather than two data paths.
//!
//! ## Price
//!
//! Rather than a price feed (whose value can drift from the price the chain
//! pinned at epoch close), the price is reversed out of the deployer cap the
//! chain stored. At epoch close `helium-sub-daos::compute_backstop` derives it:
//!
//! ```text
//! deployer_cap_hnt = mobile_dc_burned * 3 * decimals_factor / hnt_price_cap
//! ```
//!
//! so inverting recovers `hnt_price_cap` — a Pyth fixed-point USD/HNT price
//! (×10^8), which is exactly mobile-verifier's [`PriceInfo::price_in_bones`]:
//!
//! ```text
//! hnt_price_cap = mobile_dc_burned * 3 * decimals_factor / deployer_cap_hnt
//! ```
//!
//! ## Emissions split
//!
//! `hnt_rewards_issued` / `delegation_rewards_issued` come straight off the
//! mobile `sub_dao_epoch_infos` row; `epoch_emissions` is their sum. This is the
//! same data the mobile-config `SubDaoClient` used to serve over gRPC — read
//! directly from Trino here so the reward pipeline has a single source.
//!
//! [`PriceInfo::price_in_bones`]: crate::PriceInfo

use anyhow::Context;
use chrono::{DateTime, Utc};
use mobile_config::{sub_dao_epoch_reward_info::EpochRewardInfo, EpochInfo};
use rust_decimal::Decimal;
use trino_rust_client::Trino;

/// `10^(hnt_decimals - pyth_exponent - 5)` — the DC→HNT scale factor the chain's
/// backstop applies (`compute_backstop`). With HNT's fixed 8 decimals and the
/// Pyth HNT/USD feed's `-8` exponent this is `10^(8 - (-8) - 5) = 10^11`.
///
/// This must match the chain exactly to recover the price. It is stable as long
/// as the Pyth HNT/USD exponent stays `-8` (its long-standing value); the
/// exponent is not persisted on `dao_epoch_infos`, so it cannot be read back and
/// is assumed here.
const DECIMALS_FACTOR: u128 = 100_000_000_000; // 10^11

/// The deployer cap is `3.0x` the carrier-paid USD value of the epoch's burn.
const DEPLOYER_CAP_MULTIPLIER: u128 = 3;

/// Catalog + schema holding the Solana on-chain epoch tables in production
/// (`solana.public.dao_epoch_infos` / `sub_dao_epoch_infos`). Exposed so
/// integration tests can point the query at seeded fixtures in another catalog.
pub const SOLANA_SCHEMA: &str = "solana.public";

/// An epoch's reward inputs, resolved from one Trino snapshot: the emissions
/// split the rewarder distributes and the HNT price to stamp on the rewards.
pub struct ResolvedEpoch {
    pub reward_info: EpochRewardInfo,
    pub price_in_bones: u64,
}

/// A mobile epoch's on-chain reward row. The indexer stores big-ints as varchar
/// and the query `CAST`s everything to varchar, so each column arrives as a
/// string and is parsed here (field names match the `SELECT ... AS` aliases).
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, Trino)]
struct EpochRow {
    deployer_cap_hnt: String,
    dc_burned: String,
    hnt_rewards_issued: String,
    delegation_rewards_issued: String,
    rewards_issued_at: String,
    epoch_address: String,
}

/// Recover the epoch-close HNT price (`price_in_bones` = USD/HNT × 10^8) by
/// inverting the backstop formula. Returns `None` when the cap is zero (the
/// epoch hasn't closed / been indexed yet, or — vanishingly rare — no DC was
/// burned), where the price cannot be recovered.
///
/// Inputs are `u128` so parsing the indexer's varchar big-ints can't overflow;
/// `checked_mul` guards the intermediate (`dc * 3 * 10^11` exceeds `u64`).
fn recover_price_in_bones(deployer_cap_hnt: u128, dc_burned: u128) -> Option<u64> {
    if deployer_cap_hnt == 0 {
        return None;
    }
    let numerator = dc_burned
        .checked_mul(DEPLOYER_CAP_MULTIPLIER)?
        .checked_mul(DECIMALS_FACTOR)?;
    // A price that doesn't fit u64 would mean USD/HNT > ~1.8e11 — not real.
    u64::try_from(numerator / deployer_cap_hnt).ok()
}

/// Parse the indexer's varchar cap/burn columns, then recover the price.
fn recover_from_columns(deployer_cap_hnt: &str, dc_burned: &str) -> anyhow::Result<Option<u64>> {
    let deployer_cap_hnt: u128 = deployer_cap_hnt
        .parse()
        .context("parsing dao_epoch_infos.deployer_cap_hnt")?;
    let dc_burned: u128 = dc_burned
        .parse()
        .context("parsing sub_dao_epoch_infos.dc_burned")?;
    Ok(recover_price_in_bones(deployer_cap_hnt, dc_burned))
}

/// Resolve the epoch's reward inputs, or `None` when the on-chain data isn't
/// ready yet — the rows aren't indexed, the epoch isn't closed (no rewards
/// issued), or the deployer cap hasn't been written (price unrecoverable). The
/// caller waits and retries.
///
/// This is the mobile rewarder, so the rows are fixed: the HNT DAO's
/// `deployer_cap_hnt` and the mobile sub-DAO's `sub_dao_epoch_infos` row.
/// `schema` is the catalog+schema the tables live in ([`SOLANA_SCHEMA`] in prod).
pub async fn resolve(
    trino: &trino_client::Client,
    schema: &str,
    epoch: u64,
) -> anyhow::Result<Option<ResolvedEpoch>> {
    let dao = crate::resolve_dao_pubkey().to_string();
    let sub_dao = crate::resolve_subdao_pubkey().to_string();

    let Some(row) = trino
        .get_all(epoch_statement(schema, epoch, &dao, &sub_dao).typed::<EpochRow>())
        .await?
        .into_iter()
        .next()
    else {
        return Ok(None);
    };

    let hnt_rewards_issued: u64 = row
        .hnt_rewards_issued
        .parse()
        .context("parsing sub_dao_epoch_infos.hnt_rewards_issued")?;
    let delegation_rewards_issued: u64 = row
        .delegation_rewards_issued
        .parse()
        .context("parsing sub_dao_epoch_infos.delegation_rewards_issued")?;

    // Both zero => the epoch hasn't been closed / had rewards issued yet (the
    // mobile-config resolver's readiness check).
    if hnt_rewards_issued == 0 && delegation_rewards_issued == 0 {
        return Ok(None);
    }

    // No deployer cap written yet => the price can't be recovered; wait.
    let Some(price_in_bones) = recover_from_columns(&row.deployer_cap_hnt, &row.dc_burned)? else {
        return Ok(None);
    };

    let rewards_issued_at_secs: i64 = row
        .rewards_issued_at
        .parse()
        .context("parsing sub_dao_epoch_infos.rewards_issued_at")?;
    let rewards_issued_at = DateTime::<Utc>::from_timestamp(rewards_issued_at_secs, 0)
        .context("sub_dao_epoch_infos.rewards_issued_at out of range")?;

    let hnt_rewards_issued = Decimal::from(hnt_rewards_issued);
    let delegation_rewards_issued = Decimal::from(delegation_rewards_issued);

    let reward_info = EpochRewardInfo {
        epoch_day: epoch,
        epoch_address: row.epoch_address,
        sub_dao_address: sub_dao,
        epoch_period: EpochInfo::from(epoch).period,
        // The 100% total: what the rewarder distributes plus what already went
        // to veHNT delegators on-chain.
        epoch_emissions: hnt_rewards_issued + delegation_rewards_issued,
        hnt_rewards_issued,
        delegation_rewards_issued,
        rewards_issued_at,
    };

    Ok(Some(ResolvedEpoch {
        reward_info,
        price_in_bones,
    }))
}

/// Select the mobile epoch's price inputs (`deployer_cap_hnt`, `dc_burned`) and
/// emissions split (`hnt_rewards_issued`, `delegation_rewards_issued`,
/// `rewards_issued_at`, `address`) for `epoch` under `schema` (`solana.public`
/// in prod).
///
/// The indexer's numeric columns are `CAST ... AS VARCHAR` so the query is
/// agnostic to whether they're stored as varchar or bigint; they're parsed on
/// the Rust side. `epoch` is a varchar column, bound as its decimal string.
/// Qualifying the tables with `schema` (catalog.schema) makes the reference
/// independent of the client's default catalog.
fn epoch_statement(schema: &str, epoch: u64, dao: &str, sub_dao: &str) -> trino_client::Statement {
    trino_client::Statement::new(format!(
        "
        SELECT
            CAST(d.deployer_cap_hnt AS VARCHAR)          AS deployer_cap_hnt,
            CAST(s.dc_burned AS VARCHAR)                 AS dc_burned,
            CAST(s.hnt_rewards_issued AS VARCHAR)        AS hnt_rewards_issued,
            CAST(s.delegation_rewards_issued AS VARCHAR) AS delegation_rewards_issued,
            CAST(s.rewards_issued_at AS VARCHAR)         AS rewards_issued_at,
            s.address                                    AS epoch_address
        FROM {schema}.dao_epoch_infos d
        JOIN {schema}.sub_dao_epoch_infos s
            ON s.epoch = d.epoch
        WHERE d.epoch = :epoch
          AND d.dao = :dao
          AND s.sub_dao = :sub_dao
        "
    ))
    .bind("epoch", epoch.to_string())
    .bind("dao", dao.to_string())
    .bind("sub_dao", sub_dao.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn recovers_the_epoch_close_price() {
        // Epoch 20654 (closed 2026-07-21 00:00 UTC), read off-chain:
        //   dc_burned        = 785_184_940       (mobile sub_dao_epoch_infos)
        //   deployer_cap_hnt = 11_573_616_090_838 (dao_epoch_infos)
        // Inverting the cap recovers price_in_bones = 20_352_799 ($0.20352799),
        // the price the epoch actually used. Locks the `×3` and `10^11` constants.
        assert_eq!(
            recover_price_in_bones(11_573_616_090_838, 785_184_940),
            Some(20_352_799)
        );
    }

    #[test]
    fn recovers_through_varchar_parsing() {
        // The indexer serves these columns as varchar; the parse path lands on
        // the same recovered price.
        assert_eq!(
            recover_from_columns("11573616090838", "785184940").unwrap(),
            Some(20_352_799)
        );
    }

    #[test]
    fn non_numeric_column_is_an_error() {
        assert!(recover_from_columns("not-a-number", "785184940").is_err());
    }

    #[test]
    fn round_trips_a_known_price() {
        // Forward (chain): cap = dc * 3 * 10^11 / price. Inverting recovers price
        // exactly when the forward division is clean.
        let price = 250_000_000u64; // $2.50/HNT
        let dc_burned: u128 = 1_000_000_000;
        let cap = dc_burned * DEPLOYER_CAP_MULTIPLIER * DECIMALS_FACTOR / price as u128;
        assert_eq!(recover_price_in_bones(cap, dc_burned), Some(price));
    }

    #[test]
    fn zero_cap_is_unrecoverable() {
        // Cap not yet written (epoch not closed/indexed) — wait, don't divide by 0.
        assert_eq!(recover_price_in_bones(0, 785_184_940), None);
        assert_eq!(recover_price_in_bones(0, 0), None);
    }

    #[test]
    fn absurd_price_does_not_overflow_u64() {
        // A cap of 1 against a large burn yields a price far beyond u64 (and any
        // real USD/HNT): recovered as None rather than panicking or wrapping.
        assert_eq!(recover_price_in_bones(1, 1_000_000_000_000_000), None);
    }

    #[test]
    fn statement_targets_both_solana_tables_and_binds_all_keys() {
        let rendered = epoch_statement(
            SOLANA_SCHEMA,
            20654,
            "hntDaoPubkey111",
            "mobileSubDaoPubkey111",
        )
        .render()
        .unwrap();
        assert!(
            rendered.contains("solana.public.dao_epoch_infos"),
            "{rendered}"
        );
        assert!(
            rendered.contains("solana.public.sub_dao_epoch_infos"),
            "{rendered}"
        );
        // Emissions-split columns are selected alongside the price inputs.
        assert!(rendered.contains("hnt_rewards_issued"), "{rendered}");
        assert!(rendered.contains("delegation_rewards_issued"), "{rendered}");
        // Bound params render as positional placeholders (EXECUTE IMMEDIATE).
        assert!(rendered.contains("d.epoch = ?"), "{rendered}");
        assert!(rendered.contains("d.dao = ?"), "{rendered}");
        assert!(rendered.contains("s.sub_dao = ?"), "{rendered}");
    }
}
