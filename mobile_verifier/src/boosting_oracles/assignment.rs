use rust_decimal::Decimal;
use rust_decimal_macros::dec;

#[derive(Copy, Clone, PartialEq, Eq, Debug, sqlx::Type)]
#[sqlx(type_name = "oracle_assignment")]
#[sqlx(rename_all = "lowercase")]
pub enum Assignment {
    A,
    B,
    C,
}

impl ToString for Assignment {
    fn to_string(&self) -> String {
        use Assignment::*;

        match self {
            A => "A".to_string(),
            B => "B".to_string(),
            C => "C".to_string(),
        }
    }
}

pub fn urbanization_multiplier(urbanization: Assignment) -> Decimal {
    use Assignment::*;

    match urbanization {
        A => dec!(1.0),
        B => dec!(0.25),
        C => dec!(0.0),
    }
}

#[allow(dead_code)]
pub fn urbanization_and_footfall_multiplier(
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

#[allow(dead_code)]
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
