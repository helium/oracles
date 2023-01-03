use helium_proto::services::poc_mobile::CellType as CellTypeProto;
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use serde::Serialize;

pub const CELLTYPE_NOVA_436H: &str = "2AG32MBS3100196N";
pub const CELLTYPE_NOVA_430I: &str = "2AG32PBS3101S";
pub const CELLTYPE_NEUTRINO_430: &str = "2AG32PBS31010";
pub const CELLTYPE_SERCCOMM_INDOOR: &str = "P27-SCE4255W";
pub const CELLTYPE_SERCCOMM_OUTDOOR: &str = "P27-SCO4255PA10";

#[derive(Debug, Eq, Hash, PartialEq, Copy, Clone, Serialize)]
pub enum CellType {
    Nova436H = 0,
    Nova430I = 1,
    Neutrino430 = 2,
    SercommIndoor = 3,
    SercommOutdoor = 4,
}

impl CellType {
    pub fn from_cbsd_id(s: &str) -> Option<Self> {
        match s {
            s if s.starts_with(CELLTYPE_NOVA_436H) => Some(CellType::Nova436H),
            s if s.starts_with(CELLTYPE_NOVA_430I) => Some(CellType::Nova430I),
            s if s.starts_with(CELLTYPE_NEUTRINO_430) => Some(CellType::Neutrino430),
            s if s.starts_with(CELLTYPE_SERCCOMM_INDOOR) => Some(CellType::SercommIndoor),
            s if s.starts_with(CELLTYPE_SERCCOMM_OUTDOOR) => Some(CellType::SercommOutdoor),
            &_ => None,
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
}

impl From<CellType> for CellTypeProto {
    fn from(ct: CellType) -> Self {
        match ct {
            CellType::Nova436H => Self::Nova436h,
            CellType::Nova430I => Self::Nova430i,
            CellType::Neutrino430 => Self::Neutrino430,
            CellType::SercommIndoor => Self::SercommIndoor,
            CellType::SercommOutdoor => Self::SercommOutdoor,
        }
    }
}
