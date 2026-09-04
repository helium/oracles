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

use bigdecimal::RoundingMode;
use serde::{de::DeserializeSeed, Deserialize, Deserializer, Serialize, Serializer};
use trino_rust_client::{
    types::{Context, Decimal as TrinoDecimal},
    Trino,
};

/// An exact decimal stored in an Iceberg `decimal(P, S)` column.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct IcebergDecimal<const P: usize, const S: usize>(TrinoDecimal<P, S>);

#[derive(thiserror::Error, Debug, PartialEq, Eq)]
pub enum ParseDecimalError {
    #[error("invalid decimal: {0}")]
    Unparseable(String),
    /// The value is too large for the column it is destined for.
    ///
    /// Separate from [`Self::Unparseable`] because the two mean different
    /// things to whoever is looking: one is a malformed value, the other a
    /// well-formed value that this column cannot hold.
    #[error("decimal {value} does not fit decimal({precision}, {scale})")]
    OutOfRange {
        value: String,
        precision: usize,
        scale: usize,
    },
}

impl<const P: usize, const S: usize> IcebergDecimal<P, S> {
    pub fn as_string(&self) -> String {
        self.0.clone().into_bigdecimal().to_string()
    }
}

impl<const P: usize, const S: usize> FromStr for IcebergDecimal<P, S> {
    type Err = ParseDecimalError;

    /// Parses *and* range-checks against `P` and `S`.
    ///
    /// The range check is not redundant. `TrinoDecimal`'s own `FromStr` is a
    /// `BigDecimal` parse that ignores both const parameters, so a value far
    /// too large for the column parses happily here and fails much later —
    /// when `arrow-json` decodes the row into a `Decimal128(P, S)` and reports
    /// `parse decimal overflow`. That is a bad place to find out: it fails
    /// mid-write, taking whatever transaction the write was part of with it.
    /// Refusing here gives the caller a value it can act on.
    ///
    /// Excess *scale* is deliberately not refused. Arrow truncates it, as
    /// Trino would, so `1.5000001` into a `decimal(9, 6)` column stores as
    /// `1.500000`. Only magnitude is unrecoverable.
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let inner = TrinoDecimal::<P, S>::from_str(s)
            .map_err(|_| ParseDecimalError::Unparseable(s.to_string()))?;

        // Rescaling to the column's scale first is what makes `digits()` the
        // precision the column actually needs: it counts the unscaled integer,
        // so it only answers the right question once the scale matches.
        //
        // `Down` (truncate toward zero), not `HalfUp`, because that is what
        // arrow does — verified against `arrow-json`, which accepts
        // `999.9999996` into a `decimal(9, 6)` by dropping the trailing digit.
        // Rounding up here instead would carry it to `1000.000000`, ten digits,
        // and refuse a value the writer would have taken.
        let digits = inner
            .clone()
            .into_bigdecimal()
            .with_scale_round(S as i64, RoundingMode::Down)
            .digits();

        if digits > P as u64 {
            return Err(ParseDecimalError::OutOfRange {
                value: s.to_string(),
                precision: P,
                scale: S,
            });
        }

        Ok(Self(inner))
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
        assert!(matches!(
            Multiplier::from_str("").unwrap_err(),
            ParseDecimalError::Unparseable(_)
        ));
        assert!(matches!(
            Multiplier::from_str("abc").unwrap_err(),
            ParseDecimalError::Unparseable(_)
        ));
    }

    /// The bug this guards: `TrinoDecimal`'s `FromStr` ignores `P` and `S`, so
    /// without a check here an unstorable value parses fine and only fails
    /// later, inside `arrow-json`, with `parse decimal overflow` — mid-write,
    /// where it fails the enclosing transaction rather than the caller.
    ///
    /// `decimal(9, 6)` leaves three digits ahead of the point, so anything from
    /// 1000 up cannot be stored.
    #[test]
    fn rejects_values_too_large_for_the_column() {
        for input in ["1000", "12345678901234", "1E+20", "-1000"] {
            assert!(
                matches!(
                    Multiplier::from_str(input).unwrap_err(),
                    ParseDecimalError::OutOfRange { .. }
                ),
                "{input} must be refused, not deferred to the writer"
            );
        }
    }

    /// The boundary, so the check above is a real edge rather than a blanket
    /// refusal of anything large.
    ///
    /// The last case is the one that pins the rounding mode. Arrow truncates
    /// the extra digit and stores `999.999999`; rounding it up here instead
    /// would make this a refusal and lose a value the writer would have taken.
    /// Checked against `arrow-json` directly — every input in this test and in
    /// `rejects_values_too_large_for_the_column` was run through the real
    /// decoder, and the verdicts match.
    #[test]
    fn accepts_right_up_to_the_boundary() {
        for input in ["999.999999", "-999.999999", "0", "999.9999996"] {
            assert!(
                Multiplier::from_str(input).is_ok(),
                "{input} fits decimal(9, 6) and must not be refused"
            );
        }
    }

    /// Scale is not magnitude: arrow truncates extra fractional digits rather
    /// than failing, so refusing them here would drop values the column can
    /// perfectly well hold.
    #[test]
    fn tolerates_more_scale_than_the_column_keeps() {
        let decimal = Multiplier::try_from(dec!(1.5000001)).expect("scale is not fatal");
        assert_eq!(decimal.as_string(), "1.5000001");
    }

    /// The path the ticket history actually uses. `try_from(...).ok()` has to
    /// yield `None` for an unstorable multiplier — that is what lets the row be
    /// written with a null and the refusal recorded.
    #[test]
    fn try_from_reports_none_for_an_unstorable_value() {
        assert!(Multiplier::try_from(dec!(1.5)).is_ok());
        assert!(Multiplier::try_from(dec!(10000000000)).ok().is_none());
    }
}
