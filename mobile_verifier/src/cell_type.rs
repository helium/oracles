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

/// Max distance in meters between the asserted location of a WIFI hotspot
/// and its heartbeats lat/lng beyond which its location weight will be reduced
/// TODO: confirm max value
const MAX_ASSERTED_DISTANCE_DEVIATION: i64 = 300;

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
}
#[derive(PartialEq)]
pub enum CellTypeLabel {
    CellTypeLabelNone = 0,
    Cell = 1,
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
            Self::Nova436H => CellTypeLabel::Cell,
            Self::Nova430I => CellTypeLabel::Cell,
            Self::Neutrino430 => CellTypeLabel::Cell,
            Self::SercommIndoor => CellTypeLabel::Cell,
            Self::SercommOutdoor => CellTypeLabel::Cell,
            Self::CellTypeNone => CellTypeLabel::CellTypeLabelNone,
            Self::NovaGenericWifiIndoor => CellTypeLabel::Wifi,
        }
    }

    pub fn reward_weight(&self) -> Decimal {
        match self {
            Self::Nova436H => dec!(4.0),
            Self::Nova430I => dec!(2.5),
            Self::Neutrino430 => dec!(1.0),
            Self::SercommIndoor => dec!(1.0),
            Self::SercommOutdoor => dec!(2.5),
            Self::CellTypeNone => dec!(0.0),
            Self::NovaGenericWifiIndoor => dec!(0.4),
        }
    }

    // TODO: is location_validation_timestamp now redundant as we now check the HB lat/lng??
    pub fn location_weight(
        &self,
        location_validation_timestamp: Option<DateTime<Utc>>,
        distance: Option<i64>,
    ) -> Decimal {
        match (self, location_validation_timestamp.is_some()) {
            (Self::NovaGenericWifiIndoor, true) => match distance {
                Some(v) => {
                    if v > MAX_ASSERTED_DISTANCE_DEVIATION {
                        dec!(0.25)
                    } else {
                        dec!(1.0)
                    }
                }
                // shouldnt ever hit here but allow for it
                None => dec!(0.25),
            },
            (Self::NovaGenericWifiIndoor, false) => dec!(0.25),
            _ => dec!(1.0),
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
            CellType::CellTypeNone => Self::None,
            CellType::NovaGenericWifiIndoor => Self::NovaGenericWifiIndoor,
        }
    }
}
