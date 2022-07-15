use crate::{Error, Result};
use core::fmt;
use rust_decimal::prelude::*;
use serde::{de::Deserializer, Deserialize, Serialize};
use std::ops::{Add, Div, Mul};
use std::str::FromStr;

macro_rules! decimal_scalar {
    ($stype:ident, $scalar:literal, $scale:literal) => {
        #[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq)]
        pub struct $stype(Decimal);

        impl FromStr for $stype {
            type Err = Error;

            fn from_str(s: &str) -> Result<Self> {
                match Decimal::from_str(s).or_else(|_| Decimal::from_scientific(s)) {
                    Ok(data) if data.scale() > 8 => Err(Error::decimals(s)),
                    Ok(data) => Ok(Self(data)),
                    Err(_) => Err(Error::decimals(s)),
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

            pub fn get_decimal(&self) -> Decimal {
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

        impl From<f64> for $stype {
            fn from(v: f64) -> Self {
                if let Some(mut data) = Decimal::from_f64_retain(v) {
                    data.set_scale($scale).unwrap();
                    return Self(data);
                }
                panic!("f64 could not be converted into Decimal")
            }
        }

        impl From<Decimal> for $stype {
            fn from(mut v: Decimal) -> Self {
                match v.set_scale($scale) {
                    Ok(()) => Self(v),
                    Err(_e) => panic!("Decimal could not be converted to scaled Decimal"),
                }
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

        impl Mul<f64> for $stype {
            type Output = Self;
            fn mul(self, rhs: f64) -> Self {
                if let Some(rhs_d) = Decimal::from_f64_retain(rhs) {
                    if let Some(ans) = rhs_d.checked_mul(self.get_decimal()) {
                        return Self(ans);
                    }
                }
                panic!("Invalid scaled decimal multiplication")
            }
        }

        impl Mul<u64> for $stype {
            type Output = Self;
            fn mul(self, rhs: u64) -> Self {
                let rhs_d = Decimal::from(rhs);
                if let Some(ans) = rhs_d.checked_mul(self.get_decimal()) {
                    return Self(ans);
                }
                panic!("Invalid scaled decimal multiplication")
            }
        }

        impl Mul<Decimal> for $stype {
            type Output = Self;
            fn mul(self, rhs: Decimal) -> Self {
                if let Some(ans) = rhs.checked_mul(self.get_decimal()) {
                    return Self(ans);
                }
                panic!("Invalid scaled decimal multiplication")
            }
        }

        impl Add for $stype {
            type Output = Self;
            fn add(self, other: Self) -> Self {
                let x = self.get_decimal();
                let y = other.get_decimal();
                if let Some(ans) = x.checked_add(y) {
                    return Self(ans);
                }
                panic!("Invalid scaled decimal addition")
            }
        }

        impl Div for $stype {
            type Output = Self;
            fn div(self, other: Self) -> Self {
                if other.get_decimal() == Decimal::from(0) {
                    panic!("Cannot divide by 0")
                }
                let x = self.get_decimal();
                let y = other.get_decimal();
                if let Some(ans) = x.checked_div(y) {
                    return Self(ans);
                }
                panic!("Invalid scaled decimal division")
            }
        }
    };
}

decimal_scalar!(Mobile, 100_000_000, 8);
