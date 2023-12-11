use chrono::{DateTime, Utc};
use helium_proto::services::poc_mobile::CellType as CellTypeProto;
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use serde::Serialize;

pub const CELLTYPE_NOVA_436H: &str = "2AG32MBS3100196N";
pub const CELLTYPE_NOVA_430I: &str = "2AG32PBS3101S";
pub const CELLTYPE_NEUTRINO_430: &str = "2AG32PBS31010";
pub const CELLTYPE_SERCCOMM_INDOOR: &str = "P27-SCE4255W";
pub const CELLTYPE_SERCCOMM_OUTDOOR: &str = "P27-SCO4255PA10";

#[derive(Debug, Eq, Hash, PartialEq, Copy, Clone, Serialize, sqlx::Type)]
#[sqlx(type_name = "cell_type")]
#[sqlx(rename_all = "lowercase")]
pub enum CellType {
    Nova436H = 0,
    Nova430I = 1,
    Neutrino430 = 2,
    SercommIndoor = 3,
    SercommOutdoor = 4,
    CellTypeNone = 5,
    NovaGenericWifiIndoor = 6,
    NovaGenericWifiOutdoor = 7,
}

#[derive(PartialEq)]
pub enum CellTypeLabel {
    CellTypeLabelNone = 0,
    CBRS = 1,
    Wifi = 2,
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

    pub fn to_label(self) -> CellTypeLabel {
        match self {
            Self::Nova436H => CellTypeLabel::CBRS,
            Self::Nova430I => CellTypeLabel::CBRS,
            Self::Neutrino430 => CellTypeLabel::CBRS,
            Self::SercommIndoor => CellTypeLabel::CBRS,
            Self::SercommOutdoor => CellTypeLabel::CBRS,
            Self::NovaGenericWifiIndoor => CellTypeLabel::Wifi,
            Self::NovaGenericWifiOutdoor => CellTypeLabel::Wifi,
            Self::CellTypeNone => CellTypeLabel::CellTypeLabelNone,
        }
    }

    pub fn location_weight(
        &self,
        location_validation_timestamp: Option<DateTime<Utc>>,
        distance: Option<i64>,
        max_distance_to_asserted: u32,
    ) -> Decimal {
        match (self, distance, location_validation_timestamp.is_some()) {
            (Self::NovaGenericWifiIndoor, Some(dist), true)
                if dist <= max_distance_to_asserted as i64 =>
            {
                dec!(1.0)
            }
            (Self::NovaGenericWifiIndoor, Some(dist), true)
                if dist > max_distance_to_asserted as i64 =>
            {
                dec!(0.25)
            }
            (Self::NovaGenericWifiIndoor, _, _) => dec!(0.25),
            _ => dec!(1.0),
        }
    }
}

impl From<CellType> for CellTypeProto {
    fn from(ct: CellType) -> CellTypeProto {
        match ct {
            CellType::Nova436H => CellTypeProto::Nova436h,
            CellType::Nova430I => CellTypeProto::Nova430i,
            CellType::Neutrino430 => CellTypeProto::Neutrino430,
            CellType::SercommIndoor => CellTypeProto::SercommIndoor,
            CellType::SercommOutdoor => CellTypeProto::SercommOutdoor,
            CellType::NovaGenericWifiIndoor => CellTypeProto::NovaGenericWifiIndoor,
            CellType::NovaGenericWifiOutdoor => CellTypeProto::NovaGenericWifiOutdoor,
            CellType::CellTypeNone => CellTypeProto::None,
        }
    }
}
