use anyhow::Result;
use helium_proto::services::poc_mobile::oracle_boosting_hex_assignment::Assignment as ProtoAssignment;
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use std::fmt;

use super::HexAssignment;

#[derive(Debug, Clone, PartialEq, Eq, sqlx::FromRow)]
pub struct HexAssignments {
    pub footfall: Assignment,
    pub landtype: Assignment,
    pub urbanized: Assignment,
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
    pub fn builder(cell: hextree::Cell) -> HexAssignmentsBuilder {
        HexAssignmentsBuilder {
            cell,
            footfall: None,
            landtype: None,
            urbanized: None,
        }
    }

    pub fn boosting_multiplier(&self) -> Decimal {
        let HexAssignments {
            footfall,
            landtype,
            urbanized,
        } = self;

        use Assignment::*;
        match (footfall, landtype, urbanized) {
            // yellow - POI ≥ 1 Urbanized
            (A, A, A) => dec!(1.00),
            (A, B, A) => dec!(1.00),
            (A, C, A) => dec!(1.00),
            // orange - POI ≥ 1 Not Urbanized
            (A, A, B) => dec!(1.00),
            (A, B, B) => dec!(1.00),
            (A, C, B) => dec!(1.00),
            // light green - Point of Interest Urbanized
            (B, A, A) => dec!(0.70),
            (B, B, A) => dec!(0.70),
            (B, C, A) => dec!(0.70),
            // dark green - Point of Interest Not Urbanized
            (B, A, B) => dec!(0.50),
            (B, B, B) => dec!(0.50),
            (B, C, B) => dec!(0.50),
            // light blue - No POI Urbanized
            (C, A, A) => dec!(0.40),
            (C, B, A) => dec!(0.30),
            (C, C, A) => dec!(0.05),
            // dark blue - No POI Not Urbanized
            (C, A, B) => dec!(0.20),
            (C, B, B) => dec!(0.15),
            (C, C, B) => dec!(0.03),
            // gray - Outside of USA
            (_, _, C) => dec!(0.00),
        }
    }
}

pub struct HexAssignmentsBuilder {
    cell: hextree::Cell,
    footfall: Option<Result<Assignment>>,
    landtype: Option<Result<Assignment>>,
    urbanized: Option<Result<Assignment>>,
}

impl HexAssignmentsBuilder {
    pub fn footfall(mut self, footfall: &impl HexAssignment) -> Self {
        self.footfall = Some(footfall.assignment(self.cell));
        self
    }

    pub fn landtype(mut self, landtype: &impl HexAssignment) -> Self {
        self.landtype = Some(landtype.assignment(self.cell));
        self
    }

    pub fn urbanized(mut self, urbanized: &impl HexAssignment) -> Self {
        self.urbanized = Some(urbanized.assignment(self.cell));
        self
    }

    pub fn build(self) -> anyhow::Result<HexAssignments> {
        let Some(footfall) = self.footfall else {
            anyhow::bail!("footfall assignment not set");
        };
        let Some(landtype) = self.landtype else {
            anyhow::bail!("landtype assignment not set");
        };
        let Some(urbanized) = self.urbanized else {
            anyhow::bail!("urbanized assignment not set");
        };
        Ok(HexAssignments {
            footfall: footfall?,
            urbanized: urbanized?,
            landtype: landtype?,
        })
    }
}

#[cfg(test)]
impl HexAssignments {
    pub fn test_best() -> Self {
        Self {
            footfall: Assignment::A,
            urbanized: Assignment::A,
            landtype: Assignment::A,
        }
    }
}
