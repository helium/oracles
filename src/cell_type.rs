use rust_decimal::Decimal;
use rust_decimal_macros::dec;

#[derive(Debug, Eq, Hash, PartialEq)]
pub enum CellType {
    Nova436H,
    Nova430I,
    Neutrino430,
    SercommIndoor,
    SercommOutdoor,
}

impl CellType {
    pub fn fcc_id(&self) -> &'static str {
        match self {
            Self::Nova436H => "2AG32PBS3101S",
            Self::Nova430I => "2AG32PBS3101S",
            Self::Neutrino430 => "2AG32PBS31010",
            Self::SercommIndoor => "P27-SCE4255W",
            Self::SercommOutdoor => "P27-SCO4255PA10",
        }
    }

    pub fn reward_weight(&self) -> Decimal {
        match self {
            Self::Nova436H => dec!(2.0),
            Self::Nova430I => dec!(1.5),
            Self::Neutrino430 => dec!(1.0),
            Self::SercommIndoor => dec!(1.0),
            Self::SercommOutdoor => dec!(1.5),
        }
    }

    pub fn reward_shares(&self, units: u64) -> Decimal {
        self.reward_weight() * Decimal::from(units)
    }

    pub fn rewards(&self, base_rewards: Decimal) -> Decimal {
        self.reward_weight() * base_rewards
    }
}
