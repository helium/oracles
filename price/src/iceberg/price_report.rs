use anyhow::anyhow;
use chrono::{DateTime, FixedOffset, TimeZone, Utc};
use helium_iceberg::{FieldDefinition, PartitionDefinition, TableDefinition};
use helium_proto::PriceReportV1;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use solana::Token;
use std::str::FromStr;

use super::NAMESPACE;
pub const TABLE_NAME: &str = "prices";

/// Decimal column scale for `price_usd`. Wider than any token we mint
/// (HNT=8, MOBILE/IOT=6) so the column doesn't drop digits when we add
/// new tokens. Precision picked to leave 8 integer digits — comfortably
/// above any plausible per-token price.
const PRICE_USD_PRECISION: u32 = 18;
const PRICE_USD_SCALE: u32 = 10;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct IcebergPriceReport {
    pub timestamp: DateTime<FixedOffset>,
    /// Raw price scaled by the token's decimals (e.g. for HNT, USD * 10^8).
    /// Preserved for backwards compatibility with the protobuf record.
    pub price: u64,
    /// Same value rendered as plain USD (`price / 10^token.decimals()`).
    /// Stored as decimal so Trino / downstream consumers don't have to
    /// know the per-token scaling.
    pub price_usd: Decimal,
    pub token_type: String,
}

pub fn table_definition() -> helium_iceberg::Result<TableDefinition> {
    TableDefinition::builder(NAMESPACE, TABLE_NAME)
        .with_fields([
            FieldDefinition::required_timestamptz("timestamp"),
            FieldDefinition::required_long("price"),
            FieldDefinition::required_decimal("price_usd", PRICE_USD_PRECISION, PRICE_USD_SCALE),
            FieldDefinition::required_string("token_type"),
        ])
        .with_partition(PartitionDefinition::day("timestamp", "timestamp_day"))
        .build()
}

impl TryFrom<&PriceReportV1> for IcebergPriceReport {
    type Error = anyhow::Error;

    fn try_from(value: &PriceReportV1) -> Result<Self, Self::Error> {
        let timestamp = Utc
            .timestamp_opt(value.timestamp as i64, 0)
            .single()
            .ok_or_else(|| anyhow!("invalid timestamp {}", value.timestamp))?;

        // The proto's `as_str_name` and `Token::FromStr` agree on lowercase
        // token names ("hnt", "mobile", "iot"), so the round-trip is direct.
        // `Token::decimals()` is the source of truth for per-token scaling.
        let token_name = value.token_type().as_str_name();
        let token = Token::from_str(token_name)
            .map_err(|err| anyhow!("unknown token {token_name:?}: {err}"))?;
        let scale = u32::from(token.decimals());
        let divisor = 10u64
            .checked_pow(scale)
            .ok_or_else(|| anyhow!("decimals overflow for {token_name}"))?;
        let price_usd = Decimal::from(value.price)
            .checked_div(Decimal::from(divisor))
            .ok_or_else(|| anyhow!("price scaling overflow for {token_name}"))?;

        Ok(Self {
            timestamp: timestamp.into(),
            price: value.price,
            price_usd,
            token_type: token_name.to_string(),
        })
    }
}
