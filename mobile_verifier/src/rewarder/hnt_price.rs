//! HNT reward price recovered from on-chain HIP-149 backstop data.
//!
//! Rather than reading the HNT price from a price feed (whose value can drift
//! from the price the chain pinned at epoch close), the rewarder reverses it out
//! of the deployer cap the chain already stored.
//!
//! At epoch close `helium-sub-daos::compute_backstop` derives the mobile
//! deployer cap from the epoch's data-credit burn and the pinned EMA price:
//!
//! ```text
//! deployer_cap_hnt = mobile_dc_burned * 3 * decimals_factor / hnt_price_cap
//! ```
//!
//! Every input except the price is persisted on-chain — `deployer_cap_hnt` on
//! `dao_epoch_infos`, the raw `dc_burned` on the mobile `sub_dao_epoch_infos` —
//! so the price is recoverable by inverting the formula:
//!
//! ```text
//! hnt_price_cap = mobile_dc_burned * 3 * decimals_factor / deployer_cap_hnt
//! ```
//!
//! `hnt_price_cap` is a Pyth fixed-point USD/HNT price (×10^8), which is exactly
//! mobile-verifier's [`PriceInfo::price_in_bones`](crate::PriceInfo) — so the
//! recovered value drops straight into `PriceInfo::new`.

use anyhow::Context;
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

/// The mobile sub-DAO's backstop inputs for an epoch, read from Trino.
///
/// Both magnitudes are `u64` on-chain but the Solana indexer catalog stores them
/// as varchar big-ints, so they arrive as strings and are parsed before use.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize, Trino)]
struct BackstopRow {
    /// `dao_epoch_infos.deployer_cap_hnt` — the stored HNT cap (bones). "0" until
    /// the epoch-close score calculation writes it, so a positive value doubles
    /// as the "epoch closed and cap written" readiness signal.
    deployer_cap_hnt: String,
    /// `sub_dao_epoch_infos.dc_burned` — the mobile sub-DAO's raw data credits
    /// burned that epoch, the same value the backstop multiplied.
    dc_burned: String,
}

/// Recover the epoch-close HNT price (`price_in_bones` = USD/HNT × 10^8) by
/// inverting the backstop formula. Returns `None` when the cap is zero (the
/// epoch hasn't closed / been indexed yet, or — vanishingly rare — no DC was
/// burned), where the price cannot be recovered; the caller waits and retries.
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

/// Parse the indexer's varchar big-int columns, then recover the price.
fn recover_from_columns(deployer_cap_hnt: &str, dc_burned: &str) -> anyhow::Result<Option<u64>> {
    let deployer_cap_hnt: u128 = deployer_cap_hnt
        .parse()
        .context("parsing dao_epoch_infos.deployer_cap_hnt")?;
    let dc_burned: u128 = dc_burned
        .parse()
        .context("parsing sub_dao_epoch_infos.dc_burned")?;
    Ok(recover_price_in_bones(deployer_cap_hnt, dc_burned))
}

/// The recovered `price_in_bones` for `epoch`, or `None` when the on-chain
/// backstop data isn't available yet (the DAO/sub-DAO epoch rows aren't in Trino
/// or the deployer cap hasn't been written). `schema` is the catalog+schema the
/// epoch tables live in ([`SOLANA_SCHEMA`] in production).
///
/// This is the mobile rewarder, so the rows are fixed: the deployer cap is the
/// HNT DAO's ([`resolve_dao_pubkey`](crate::resolve_dao_pubkey)) and the
/// `dc_burned` is the mobile sub-DAO's ([`resolve_subdao_pubkey`](crate::resolve_subdao_pubkey)).
pub async fn price_in_bones_for_epoch(
    trino: &trino_client::Client,
    schema: &str,
    epoch: u64,
) -> anyhow::Result<Option<u64>> {
    let dao = crate::resolve_dao_pubkey().to_string();
    let sub_dao = crate::resolve_subdao_pubkey().to_string();

    let Some(row) = trino
        .get_all(backstop_statement(schema, epoch, &dao, &sub_dao).typed::<BackstopRow>())
        .await?
        .into_iter()
        .next()
    else {
        return Ok(None);
    };

    recover_from_columns(&row.deployer_cap_hnt, &row.dc_burned)
}

/// Select the HNT DAO's `deployer_cap_hnt` and the mobile sub-DAO's `dc_burned`
/// for `epoch`, from the epoch tables under `schema` (`solana.public` in prod).
///
/// Both tables are keyed by `epoch`; `dc_burned` is per sub-DAO, so it's filtered
/// to the mobile `sub_dao`. `epoch` is a varchar column, so it's bound as its
/// decimal string. Qualifying the tables with `schema` (catalog.schema) makes the
/// reference independent of the client's default catalog.
fn backstop_statement(
    schema: &str,
    epoch: u64,
    dao: &str,
    sub_dao: &str,
) -> trino_client::Statement {
    trino_client::Statement::new(format!(
        "
        SELECT
            d.deployer_cap_hnt AS deployer_cap_hnt,
            s.dc_burned        AS dc_burned
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
        let rendered = backstop_statement(
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
        // Bound params render as positional placeholders (EXECUTE IMMEDIATE).
        assert!(rendered.contains("d.epoch = ?"), "{rendered}");
        assert!(rendered.contains("d.dao = ?"), "{rendered}");
        assert!(rendered.contains("s.sub_dao = ?"), "{rendered}");
    }
}
