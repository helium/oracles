//! Exact decimals for Iceberg `decimal(P, S)` columns.
//!
//! Bridges two APIs that don't quite meet. The write path serializes a row with
//! serde and hands the JSON to `arrow-json`, whose `DecimalArrayDecoder`
//! accepts a decimal *string*. The read path uses `trino-rust-client`'s `Trino`
//! trait, whose `Decimal<P, S>` implements `DeserializeSeed` but neither
//! `Serialize` nor `Deserialize` — so a row struct containing one cannot derive
//! the serde impls the writer needs.
//!
//! [`IcebergDecimal`] implements all three, so a row type can carry an exact
//! decimal and still `#[derive(Trino, Serialize, Deserialize)]` like every other
//! table in this crate.
//!
//! Why not `double`: values that are not binary-representable — 1.3, 2.7 — pick
//! up artifacts a float cannot shed, and these columns are a public record.

use std::str::FromStr;

use serde::{de::DeserializeSeed, Deserialize, Deserializer, Serialize, Serializer};
use trino_rust_client::{
    types::{Context, Decimal as TrinoDecimal},
    Trino,
};

/// An exact decimal stored in an Iceberg `decimal(P, S)` column.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct IcebergDecimal<const P: usize, const S: usize>(TrinoDecimal<P, S>);

#[derive(thiserror::Error, Debug)]
#[error("invalid decimal: {0}")]
pub struct ParseDecimalError(String);

impl<const P: usize, const S: usize> IcebergDecimal<P, S> {
    pub fn as_string(&self) -> String {
        self.0.clone().into_bigdecimal().to_string()
    }
}

impl<const P: usize, const S: usize> FromStr for IcebergDecimal<P, S> {
    type Err = ParseDecimalError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        TrinoDecimal::from_str(s)
            .map(Self)
            .map_err(|_| ParseDecimalError(s.to_string()))
    }
}

impl<const P: usize, const S: usize> TryFrom<rust_decimal::Decimal> for IcebergDecimal<P, S> {
    type Error = ParseDecimalError;

    fn try_from(value: rust_decimal::Decimal) -> Result<Self, Self::Error> {
        Self::from_str(&value.to_string())
    }
}

impl<const P: usize, const S: usize> std::fmt::Display for IcebergDecimal<P, S> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.as_string())
    }
}

/// Serialized as a string, which is what `arrow-json` decodes into a
/// `Decimal128` column. A JSON number would work for small values and lose
/// precision for large ones.
impl<const P: usize, const S: usize> Serialize for IcebergDecimal<P, S> {
    fn serialize<T: Serializer>(&self, serializer: T) -> Result<T::Ok, T::Error> {
        serializer.serialize_str(&self.as_string())
    }
}

impl<'de, const P: usize, const S: usize> Deserialize<'de> for IcebergDecimal<P, S> {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let s = String::deserialize(deserializer)?;
        Self::from_str(&s).map_err(serde::de::Error::custom)
    }
}

/// Wraps the inner seed so deserialization yields an [`IcebergDecimal`] rather
/// than the type it delegates to.
pub struct IcebergDecimalSeed<'a, 'de, const P: usize, const S: usize>(
    <TrinoDecimal<P, S> as Trino>::Seed<'a, 'de>,
);

impl<'de, 'a, const P: usize, const S: usize> DeserializeSeed<'de>
    for IcebergDecimalSeed<'a, 'de, P, S>
{
    type Value = IcebergDecimal<P, S>;

    fn deserialize<D: Deserializer<'de>>(self, deserializer: D) -> Result<Self::Value, D::Error> {
        self.0.deserialize(deserializer).map(IcebergDecimal)
    }
}

/// Delegates to the wrapped type, so Trino still sees a `decimal(P, S)`.
impl<const P: usize, const S: usize> Trino for IcebergDecimal<P, S> {
    type ValueType<'a> = <TrinoDecimal<P, S> as Trino>::ValueType<'a>;
    type Seed<'a, 'de> = IcebergDecimalSeed<'a, 'de, P, S>;

    fn value(&self) -> Self::ValueType<'_> {
        self.0.value()
    }

    fn ty() -> trino_rust_client::types::TrinoTy {
        TrinoDecimal::<P, S>::ty()
    }

    fn seed<'a, 'de>(ctx: &'a Context) -> Self::Seed<'a, 'de> {
        IcebergDecimalSeed(TrinoDecimal::<P, S>::seed(ctx))
    }

    fn empty() -> Self {
        Self(TrinoDecimal::empty())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal::dec;

    type Multiplier = IcebergDecimal<9, 6>;

    #[test]
    fn round_trips_through_serde_as_a_string() {
        for value in [dec!(1), dec!(1.5), dec!(1.3), dec!(5), dec!(2.718281)] {
            let decimal = Multiplier::try_from(value).expect("in range");

            let json = serde_json::to_string(&decimal).expect("serialize");
            assert!(
                json.starts_with('"'),
                "must serialize as a string for arrow-json, got {json}"
            );

            let back: Multiplier = serde_json::from_str(&json).expect("deserialize");
            assert_eq!(decimal, back);
        }
    }

    /// The case that a `double` column would get wrong: 1.3 has no exact binary
    /// representation, so a float round trip yields 1.3000000000000000444.
    #[test]
    fn keeps_values_a_float_would_mangle() {
        let decimal = Multiplier::try_from(dec!(1.3)).expect("in range");
        assert_eq!(decimal.as_string(), "1.3");

        let json = serde_json::to_string(&decimal).expect("serialize");
        assert_eq!(json, "\"1.3\"");
    }

    #[test]
    fn rejects_nonsense() {
        assert!(Multiplier::from_str("").is_err());
        assert!(Multiplier::from_str("abc").is_err());
    }
}
