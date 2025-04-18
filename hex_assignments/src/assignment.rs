use helium_proto::services::poc_mobile::OracleBoostingAssignment as ProtoAssignment;
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use std::fmt;

#[derive(Debug, Clone, PartialEq, Eq, sqlx::FromRow)]
pub struct HexAssignments {
    pub footfall: Assignment,
    pub landtype: Assignment,
    pub urbanized: Assignment,
    pub service_provider_override: Assignment,
}

#[derive(Copy, Clone, PartialEq, Eq, Debug, sqlx::Type)]
#[sqlx(type_name = "oracle_assignment")]
#[sqlx(rename_all = "lowercase")]
pub enum Assignment {
    A,
    B,
    C,
}

impl From<Assignment> for ProtoAssignment {
    fn from(assignment: Assignment) -> Self {
        match assignment {
            Assignment::A => Self::A,
            Assignment::B => Self::B,
            Assignment::C => Self::C,
        }
    }
}

// used solely for service provider override assignment
// internally SP override assignment is a regular assignment
// but when outputted as part of a `covered_hex` or
// `oracle_boosting_hex_assignment` proto msg it gets converted to a bool
// we do not read either of those protos back into the verifier
// hence the one direction for this conversion
// should we ever need to read the proto back into the verifier
// we will need to replace this with a From<bool> for Assignment
// but at that point we should just make v2's of the relevant msgs and enums
#[allow(clippy::from_over_into)]
impl Into<bool> for Assignment {
    fn into(self) -> bool {
        matches!(self, Assignment::A)
    }
}

impl From<Assignment> for i32 {
    fn from(assignment: Assignment) -> i32 {
        ProtoAssignment::from(assignment) as i32
    }
}

impl From<ProtoAssignment> for Assignment {
    fn from(value: ProtoAssignment) -> Self {
        match value {
            ProtoAssignment::A => Assignment::A,
            ProtoAssignment::B => Assignment::B,
            ProtoAssignment::C => Assignment::C,
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

impl HexAssignments {
    pub fn boosting_multiplier(&self) -> Decimal {
        let HexAssignments {
            footfall,
            landtype,
            urbanized,
            service_provider_override,
        } = self;

        use Assignment::*;
        match (footfall, landtype, urbanized, service_provider_override) {
            // service provider override hex
            // Overrides other dataset assignments if set
            (_, _, _, A) => dec!(1.00),
            // yellow - POI ≥ 1 Urbanized
            (A, A, A, _) => dec!(1.00),
            (A, B, A, _) => dec!(1.00),
            (A, C, A, _) => dec!(1.00),
            // orange - POI ≥ 1 Not Urbanized
            (A, A, B, _) => dec!(1.00),
            (A, B, B, _) => dec!(1.00),
            (A, C, B, _) => dec!(1.00),
            // light green - Point of Interest Urbanized
            (B, A, A, _) => dec!(0.70),
            (B, B, A, _) => dec!(0.70),
            (B, C, A, _) => dec!(0.70),
            // dark green - Point of Interest Not Urbanized
            (B, A, B, _) => dec!(0.50),
            (B, B, B, _) => dec!(0.50),
            (B, C, B, _) => dec!(0.50),
            // gray - Outside of USA
            (_, _, C, _) => dec!(0.00),
            //HRP-20250409 - all footfall C hexes are 0.03 multiplier
            (C, _, _, _) => dec!(0.03),
        }
    }
}
