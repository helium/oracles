use rust_decimal::Decimal;
use rust_decimal_macros::dec;

#[derive(Debug, Eq, Hash, PartialEq)]
pub enum CellModel {
    Nova436H,
    Nova430I,
    Neutrino430,
    SercommIndoor,
    SercommOutdoor,
}

pub struct CellType<'a> {
    pub cell_model: CellModel,
    pub fcc_id: &'a str,
    // reward_wt is x10, so 15 = 1.5 (actual)
    pub reward_wt: Decimal,
}

impl<'a> CellType<'a> {
    pub const NOVA436H: CellType<'a> = CellType {
        cell_model: CellModel::Nova436H,
        fcc_id: "2AG32MBS3100196N",
        reward_wt: dec!(2.0),
    };
    pub const NOVA430I: CellType<'a> = CellType {
        cell_model: CellModel::Nova430I,
        fcc_id: "2AG32PBS3101S",
        reward_wt: dec!(1.5),
    };
    pub const NEUTRINO430: CellType<'a> = CellType {
        cell_model: CellModel::Neutrino430,
        fcc_id: "2AG32PBS31010",
        reward_wt: dec!(1.0),
    };
    pub const SERCOMMINDOOR: CellType<'a> = CellType {
        cell_model: CellModel::SercommIndoor,
        fcc_id: "P27-SCE4255W",
        reward_wt: dec!(1.0),
    };
    pub const SERCOMMOUTDOOR: CellType<'a> = CellType {
        cell_model: CellModel::SercommOutdoor,
        fcc_id: "P27-SCO4255PA10",
        reward_wt: dec!(1.5),
    };

    pub fn reward_shares(&self, units: u64) -> Decimal {
        self.reward_wt * Decimal::from(units)
    }

    pub fn rewards(&self, base_rewards: Decimal) -> Decimal {
        self.reward_wt * base_rewards
    }
}
