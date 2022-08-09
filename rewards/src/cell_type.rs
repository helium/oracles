use crate::{Error, Result};
use std::str::FromStr;

use rust_decimal::Decimal;
use rust_decimal_macros::dec;

#[derive(Debug, Eq, Hash, PartialEq, Copy, Clone)]
pub enum CellType {
    Nova436H,
    Nova430I,
    Neutrino430,
    SercommIndoor,
    SercommOutdoor,
}

impl FromStr for CellType {
    type Err = Error;
    fn from_str(s: &str) -> Result<Self> {
        if s.starts_with("2AG32MBS3100196N") {
            Ok(CellType::Nova436H)
        } else if s.starts_with("2AG32PBS3101S") {
            Ok(CellType::Nova430I)
        } else if s.starts_with("2AG32PBS31010") {
            Ok(CellType::Neutrino430)
        } else if s.starts_with("P27-SCE4255W") {
            Ok(CellType::SercommIndoor)
        } else if s.starts_with("P27-SCO4255PA10") {
            Ok(CellType::SercommOutdoor)
        } else {
            Err(Error::not_found(format!("invalid cell_type {}", s)))
        }
    }
}

impl CellType {
    pub fn fcc_id(&self) -> &'static str {
        match self {
            Self::Nova436H => "2AG32MBS3100196N",
            Self::Nova430I => "2AG32PBS3101S",
            Self::Neutrino430 => "2AG32PBS31010",
            Self::SercommIndoor => "P27-SCE4255W",
            Self::SercommOutdoor => "P27-SCO4255PA10",
        }
    }

    pub fn reward_weight(&self) -> Decimal {
        match self {
            Self::Nova436H => dec!(4.0),
            Self::Nova430I => dec!(2.5),
            Self::Neutrino430 => dec!(1.0),
            Self::SercommIndoor => dec!(1.0),
            Self::SercommOutdoor => dec!(2.5),
        }
    }

    pub fn reward_shares(&self, units: u64) -> Decimal {
        self.reward_weight() * Decimal::from(units)
    }

    pub fn rewards(&self, base_rewards: Decimal) -> Decimal {
        base_rewards * self.reward_weight()
    }
}
