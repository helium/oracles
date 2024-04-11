use helium_proto::services::poc_mobile::oracle_boosting_hex_assignment;
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use std::fmt;

#[derive(Copy, Clone, PartialEq, Eq, Debug, sqlx::Type)]
#[sqlx(type_name = "oracle_assignment")]
#[sqlx(rename_all = "lowercase")]
pub enum Assignment {
    A,
    B,
    C,
}

impl From<Assignment> for oracle_boosting_hex_assignment::Assignment {
    fn from(assignment: Assignment) -> Self {
        match assignment {
            Assignment::A => Self::A,
            Assignment::B => Self::B,
            Assignment::C => Self::C,
        }
    }
}

impl From<Assignment> for i32 {
    fn from(assignment: Assignment) -> i32 {
        oracle_boosting_hex_assignment::Assignment::from(assignment) as i32
    }
}

impl TryFrom<i32> for Assignment {
    type Error = anyhow::Error;

    fn try_from(value: i32) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(Assignment::A),
            1 => Ok(Assignment::B),
            2 => Ok(Assignment::C),
            other => Err(anyhow::anyhow!("could not make Assignment from {other}")),
        }
    }
}

impl fmt::Display for Assignment {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        use Assignment::*;

        match self {
            A => write!(f, "a"),
            B => write!(f, "b"),
            C => write!(f, "c"),
        }
    }
}

pub fn footfall_and_urbanization_multiplier(
    footfall: Assignment,
    urbanization: Assignment,
) -> Decimal {
    use Assignment::*;

    match (footfall, urbanization) {
        (A, A) => dec!(1.0),
        (A, B) => dec!(1.0),
        (B, A) => dec!(0.75),
        (B, B) => dec!(0.50),
        (C, A) => dec!(0.40),
        (C, B) => dec!(0.10),
        (A, C) => dec!(0.00),
        (B, C) => dec!(0.00),
        (C, C) => dec!(0.00),
    }
}

pub fn boosting_oracles_multiplier(
    footfall: Assignment,
    landtype: Assignment,
    urbanization: Assignment,
) -> Decimal {
    use Assignment::*;

    match (footfall, landtype, urbanization) {
        // POI ≥ 1 Urbanized
        (A, A, A) => dec!(1.00),
        (A, B, A) => dec!(1.00),
        (A, C, A) => dec!(1.00),
        // POI ≥ 1 Not Urbanized
        (A, A, B) => dec!(1.00),
        (A, B, B) => dec!(1.00),
        (A, C, B) => dec!(1.00),
        // Point of Interest Urbanized
        (B, A, A) => dec!(0.70),
        (B, B, A) => dec!(0.70),
        (B, C, A) => dec!(0.70),
        // Point of Interest Not Urbanized
        (B, A, B) => dec!(0.50),
        (B, B, B) => dec!(0.50),
        (B, C, B) => dec!(0.50),
        // No POI Urbanized
        (C, A, A) => dec!(0.40),
        (C, B, A) => dec!(0.30),
        (C, C, A) => dec!(0.05),
        // No POI Not Urbanized
        (C, A, B) => dec!(0.20),
        (C, B, B) => dec!(0.15),
        (C, C, B) => dec!(0.03),
        // Outside of USA
        (_, _, C) => dec!(0.00),
    }
}
