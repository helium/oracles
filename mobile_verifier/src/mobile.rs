use crate::{error::DecodeError, Error, Result};
use core::fmt;
use rust_decimal::prelude::*;
use serde::{de::Deserializer, Deserialize, Serialize};
use std::str::FromStr;

macro_rules! decimal_scalar {
    ($stype:ident, $scalar:literal, $scale:literal) => {
        #[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
        pub struct $stype(Decimal);

        impl FromStr for $stype {
            type Err = Error;

            fn from_str(s: &str) -> Result<Self> {
                match Decimal::from_str(s).or_else(|_| Decimal::from_scientific(s)) {
                    Ok(data) if data.scale() > 8 => Err(DecodeError::decimals(s).into()),
                    Ok(data) => Ok(Self(data)),
                    Err(_) => Err(DecodeError::decimals(s).into()),
                }
            }
        }

        impl fmt::Display for $stype {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                self.0.fmt(f)
            }
        }

        impl $stype {
            pub fn new(d: Decimal) -> Self {
                Self(d)
            }

            pub fn into_inner(self) -> Decimal {
                self.0
            }

            pub fn deserialize<'de, D>(d: D) -> std::result::Result<Self, D::Error>
            where
                D: Deserializer<'de>,
            {
                let val = u64::deserialize(d)?;
                Ok(Self::from(val))
            }

            pub fn deserialize_option<'de, D>(d: D) -> std::result::Result<Option<Self>, D::Error>
            where
                D: Deserializer<'de>,
            {
                let v: Option<u64> = Option::deserialize(d)?;
                if let Some(val) = v {
                    Ok(Some(Self::from(val)))
                } else {
                    Ok(None)
                }
            }
        }

        impl From<u64> for $stype {
            fn from(v: u64) -> Self {
                if let Some(mut data) = Decimal::from_u64(v) {
                    data.set_scale($scale).unwrap();
                    return Self(data);
                }
                panic!("u64 could not be converted into Decimal")
            }
        }

        impl From<Decimal> for $stype {
            fn from(mut v: Decimal) -> Self {
                // rescale the decimal back to 8 digits of precision
                v.rescale(8);
                Self(v)
            }
        }

        impl From<$stype> for u64 {
            fn from(v: $stype) -> Self {
                if let Some(scaled_dec) = v.0.checked_mul($scalar.into()) {
                    if let Some(num) = scaled_dec.to_u64() {
                        return num;
                    }
                }
                panic!("Invalid scaled decimal construction")
            }
        }

        impl From<i32> for $stype {
            fn from(v: i32) -> Self {
                if let Some(mut data) = Decimal::from_i32(v) {
                    data.set_scale($scale).unwrap();
                    return Self(data);
                }
                panic!("u64 could not be converted into Decimal")
            }
        }

        impl From<$stype> for i32 {
            fn from(v: $stype) -> Self {
                if let Some(scaled_dec) = v.0.checked_mul($scalar.into()) {
                    if let Some(num) = scaled_dec.to_i32() {
                        return num;
                    }
                }
                panic!("Invalid scaled decimal construction")
            }
        }
    };
}

decimal_scalar!(Mobile, 100_000_000, 8);
