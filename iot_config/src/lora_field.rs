use serde::{Deserialize, Deserializer, Serialize, Serializer};
use sqlx::{postgres::PgRow, FromRow, Row};
use std::{fmt::Display, str::FromStr};

#[derive(Clone, Copy, Debug, Default, Eq, Hash, PartialEq)]
pub struct LoraField<const WIDTH: usize>(pub u64);

pub type NetIdField = LoraField<6>;
pub type DevAddrField = LoraField<8>;
pub type EuiField = LoraField<16>;

pub mod proto {
    pub use helium_proto::services::iot_config::{
        DevaddrConstraintV1, DevaddrRangeV1, EuiPairV1, OrgV1, SkfV1,
    };
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct DevAddrRange {
    pub route_id: String,
    pub start_addr: DevAddrField,
    pub end_addr: DevAddrField,
}

impl DevAddrRange {
    pub fn new(route_id: String, start_addr: DevAddrField, end_addr: DevAddrField) -> Self {
        Self {
            route_id,
            start_addr,
            end_addr,
        }
    }

    pub fn contains_addr(&self, addr: DevAddrField) -> bool {
        self.start_addr <= addr && self.end_addr >= addr
    }
}

impl FromRow<'_, PgRow> for DevAddrRange {
    fn from_row(row: &PgRow) -> sqlx::Result<Self> {
        Ok(Self {
            route_id: row
                .try_get::<sqlx::types::Uuid, &str>("route_id")?
                .to_string(),
            start_addr: row.try_get::<i32, &str>("start_addr")?.into(),
            end_addr: row.try_get::<i32, &str>("end_addr")?.into(),
        })
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct DevAddrConstraint {
    pub start_addr: DevAddrField,
    pub end_addr: DevAddrField,
}

impl DevAddrConstraint {
    pub fn new(
        start_addr: DevAddrField,
        end_addr: DevAddrField,
    ) -> Result<Self, DevAddrRangeError> {
        if end_addr <= start_addr {
            return Err(DevAddrRangeError::EndLessThanStart);
        }
        if start_addr.0 % 2 != 0 || end_addr.0 % 2 == 0 {
            return Err(DevAddrRangeError::RangeUneven);
        }

        Ok(Self {
            start_addr,
            end_addr,
        })
    }

    pub fn next_start(&self) -> Result<DevAddrField, DevAddrRangeError> {
        let end: u32 = self.end_addr.into();
        Ok(devaddr(end + 1))
    }

    pub fn contains_range(&self, range: &DevAddrRange) -> bool {
        self.start_addr <= range.start_addr && self.end_addr >= range.end_addr
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct EuiPair {
    pub route_id: String,
    pub app_eui: EuiField,
    pub dev_eui: EuiField,
}

impl EuiPair {
    pub fn new(route_id: String, app_eui: EuiField, dev_eui: EuiField) -> Self {
        Self {
            route_id,
            app_eui,
            dev_eui,
        }
    }
}

impl FromRow<'_, PgRow> for EuiPair {
    fn from_row(row: &PgRow) -> sqlx::Result<Self> {
        Ok(Self {
            route_id: row
                .try_get::<sqlx::types::Uuid, &str>("route_id")?
                .to_string(),
            app_eui: row.try_get::<i64, &str>("app_eui")?.into(),
            dev_eui: row.try_get::<i64, &str>("dev_eui")?.into(),
        })
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct Skf {
    pub route_id: String,
    pub devaddr: DevAddrField,
    pub session_key: String,
    pub max_copies: u32,
}

impl Skf {
    pub fn new(
        route_id: String,
        devaddr: DevAddrField,
        session_key: String,
        max_copies: u32,
    ) -> Self {
        Self {
            route_id,
            devaddr,
            session_key,
            max_copies,
        }
    }
}

impl FromRow<'_, PgRow> for Skf {
    fn from_row(row: &PgRow) -> sqlx::Result<Self> {
        Ok(Self {
            route_id: row
                .try_get::<sqlx::types::Uuid, &str>("route_id")?
                .to_string(),
            devaddr: row.get::<i32, &str>("devaddr").into(),
            session_key: row.get::<String, &str>("session_key"),
            max_copies: row.get::<i32, &str>("max_copies") as u32,
        })
    }
}

#[derive(thiserror::Error, Debug)]
pub enum ParseError {
    #[error("char len mismatch: expected {0}, found {1}")]
    LengthMismatch(usize, usize),
    #[error("parse int failed: {0}")]
    ParseInt(#[from] std::num::ParseIntError),
}

#[derive(thiserror::Error, Debug)]
#[error("net_id field error: {0}")]
pub struct NetIdError(#[from] ParseError);

#[derive(thiserror::Error, Debug)]
#[error("devaddr field error: {0}")]
pub struct DevAddrError(#[from] ParseError);

#[derive(thiserror::Error, Debug)]
#[error("eui field error: {0}")]
pub struct EuiError(#[from] ParseError);

#[derive(thiserror::Error, Debug)]
#[error("invalid net id type: {0}")]
pub struct InvalidNetId(u32);

#[derive(thiserror::Error, Debug)]
pub enum DevAddrRangeError {
    #[error("devaddr end less than start")]
    EndLessThanStart,
    #[error("devaddr next addr failed")]
    NextStartUnavailable,
    #[error("devaddr range uneven")]
    RangeUneven,
}

impl<const WIDTH: usize> PartialOrd for LoraField<WIDTH> {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        self.0.partial_cmp(&other.0)
    }
}

// Freely convert any LoraField to and from 64-bit integers
// but limit conversion between 32-bit integers to the NetId
// and DevAddr fields only
impl<const WIDTH: usize> From<LoraField<WIDTH>> for u64 {
    fn from(field: LoraField<WIDTH>) -> Self {
        field.0
    }
}

impl<const WIDTH: usize> From<LoraField<WIDTH>> for i64 {
    fn from(field: LoraField<WIDTH>) -> Self {
        field.0 as i64
    }
}

impl From<LoraField<6>> for u32 {
    fn from(field: LoraField<6>) -> Self {
        field.0 as u32
    }
}

impl From<LoraField<8>> for u32 {
    fn from(field: LoraField<8>) -> Self {
        field.0 as u32
    }
}

impl From<LoraField<6>> for i32 {
    fn from(field: LoraField<6>) -> Self {
        field.0 as i32
    }
}

impl From<LoraField<8>> for i32 {
    fn from(field: LoraField<8>) -> Self {
        field.0 as i32
    }
}

impl<const WIDTH: usize> From<u64> for LoraField<WIDTH> {
    fn from(val: u64) -> Self {
        LoraField(val)
    }
}

impl<const WIDTH: usize> From<i64> for LoraField<WIDTH> {
    fn from(val: i64) -> Self {
        LoraField(val as u64)
    }
}

impl From<u32> for LoraField<6> {
    fn from(val: u32) -> Self {
        LoraField::<6>(val as u64)
    }
}

impl From<u32> for LoraField<8> {
    fn from(val: u32) -> Self {
        LoraField::<8>(val as u64)
    }
}

impl From<i32> for LoraField<6> {
    fn from(val: i32) -> Self {
        LoraField::<6>((val as u32) as u64)
    }
}

impl From<i32> for LoraField<8> {
    fn from(val: i32) -> Self {
        LoraField::<8>((val as u32) as u64)
    }
}

impl<const WIDTH: usize> Display for LoraField<WIDTH> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // pad with 0s to the left up to WIDTH
        write!(f, "{:0>width$X}", self.0, width = WIDTH)
    }
}

impl<const WIDTH: usize> FromStr for LoraField<WIDTH> {
    type Err = ParseError;
    fn from_str(s: &str) -> Result<LoraField<WIDTH>, Self::Err> {
        if "*" == s {
            return Ok(LoraField::<WIDTH>(0));
        }
        verify_len(s, WIDTH)?;
        Ok(LoraField::<WIDTH>(u64::from_str_radix(s, 16)?))
    }
}

impl<const WIDTH: usize> Serialize for LoraField<WIDTH> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&format!("{self}"))
    }
}

impl<'de, const WIDTH: usize> Deserialize<'de> for LoraField<WIDTH> {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct LoraFieldVisitor<const IN_WIDTH: usize>;

        impl<const IN_WIDTH: usize> serde::de::Visitor<'_> for LoraFieldVisitor<IN_WIDTH> {
            type Value = LoraField<IN_WIDTH>;

            fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
                formatter.write_str(&format!("field string {IN_WIDTH} wide"))
            }

            fn visit_str<E>(self, value: &str) -> Result<LoraField<IN_WIDTH>, E>
            where
                E: serde::de::Error,
            {
                let field = LoraField::<IN_WIDTH>::from_str(value)
                    .map_err(|_| serde::de::Error::invalid_length(value.len(), &self))?;
                Ok(field)
            }

            fn visit_u64<E>(self, v: u64) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                Ok(LoraField::<IN_WIDTH>(v))
            }
        }

        deserializer.deserialize_any(LoraFieldVisitor::<WIDTH>)
    }
}

pub fn validate_net_id(s: &str) -> Result<NetIdField, NetIdError> {
    NetIdField::from_str(s).map_err(NetIdError)
}

pub fn validate_devaddr(s: &str) -> Result<DevAddrField, DevAddrError> {
    DevAddrField::from_str(s).map_err(DevAddrError)
}

pub fn validate_eui(s: &str) -> Result<EuiField, EuiError> {
    EuiField::from_str(s).map_err(EuiError)
}

pub fn devaddr(val: u32) -> DevAddrField {
    val.into()
}

pub fn eui(val: u64) -> EuiField {
    val.into()
}

pub fn net_id(val: u32) -> NetIdField {
    val.into()
}

fn verify_len(input: &str, expected_len: usize) -> Result<(), ParseError> {
    match input.len() {
        len if len == expected_len => Ok(()),
        len => Err(ParseError::LengthMismatch(len, expected_len)),
    }
}

impl NetIdField {
    fn net_id_type(&self) -> u32 {
        const BIT_WIDTH: usize = 24;
        const TYPE_LEN: usize = 3;
        let net_id = self.0 as u32;
        net_id >> (BIT_WIDTH - TYPE_LEN)
    }

    pub fn nwk_id(&self) -> u32 {
        let prefix_length = self.net_id_type() + 1;

        let mut temp = self.0 as u32;
        const BIT32PAD: u32 = 8;

        // clear prefix
        temp <<= prefix_length + BIT32PAD;
        // shift to start
        temp >>= prefix_length + BIT32PAD;
        temp
    }

    fn devaddr_type_bits(id_type: u32) -> Result<u32, InvalidNetId> {
        match id_type {
            0 => Ok(0),
            1 => Ok(2 << (32 - 2)),
            2 => Ok(6 << (32 - 3)),
            3 => Ok(14 << (32 - 4)),
            4 => Ok(30 << (32 - 5)),
            5 => Ok(62 << (32 - 6)),
            6 => Ok(126 << (32 - 7)),
            7 => Ok(254 << (32 - 8)),
            other => Err(InvalidNetId(other)),
        }
    }

    fn nwk_id_bits(id_type: u32, nwk_id: u32) -> Result<u32, InvalidNetId> {
        match id_type {
            0 => Ok(nwk_id << 25),
            1 => Ok(nwk_id << 24),
            2 => Ok(nwk_id << 20),
            3 => Ok(nwk_id << 17),
            4 => Ok(nwk_id << 15),
            5 => Ok(nwk_id << 13),
            6 => Ok(nwk_id << 10),
            7 => Ok(nwk_id << 7),
            other => Err(InvalidNetId(other)),
        }
    }

    fn max_nwk_addr_bit(id_type: u32) -> Result<u32, InvalidNetId> {
        match id_type {
            0 => Ok(2u32.pow(25) - 1),
            1 => Ok(2u32.pow(24) - 1),
            2 => Ok(2u32.pow(20) - 1),
            3 => Ok(2u32.pow(17) - 1),
            4 => Ok(2u32.pow(15) - 1),
            5 => Ok(2u32.pow(13) - 1),
            6 => Ok(2u32.pow(10) - 1),
            7 => Ok(2u32.pow(7) - 1),
            other => Err(InvalidNetId(other)),
        }
    }

    pub fn range_start(&self) -> Result<DevAddrField, InvalidNetId> {
        let id_type = self.net_id_type();
        let nwk_id = self.nwk_id();

        let left = Self::devaddr_type_bits(id_type)?;
        let middle = Self::nwk_id_bits(id_type, nwk_id)?;

        let min_addr = left | middle;
        Ok(devaddr(min_addr))
    }

    pub fn range_end(&self) -> Result<DevAddrField, InvalidNetId> {
        let id_type = self.net_id_type();
        let nwk_id = self.nwk_id();

        let left = Self::devaddr_type_bits(id_type)?;
        let middle = Self::nwk_id_bits(id_type, nwk_id)?;
        let right = Self::max_nwk_addr_bit(id_type)?;

        let max_devaddr = left | middle | right;
        Ok(devaddr(max_devaddr))
    }

    pub fn full_range(&self) -> Result<DevAddrConstraint, InvalidNetId> {
        Ok(DevAddrConstraint {
            start_addr: self.range_start()?,
            end_addr: self.range_end()?,
        })
    }
}

impl DevAddrField {
    pub fn to_range(self, add: u64) -> DevAddrConstraint {
        let end = (self.0 + (add - 1)).into();
        DevAddrConstraint {
            start_addr: self,
            end_addr: end,
        }
    }

    pub fn to_net_id(self) -> Result<NetIdField, InvalidNetId> {
        let addr = self.0 as u32;
        let id = match addr.leading_ones() {
            0 => (addr & 0b01111110000000000000000000000000) >> 25, // 0b000000000000000000000000,
            1 => ((addr & 0b00111111000000000000000000000000) >> 24) | 0b001000000000000000000000,
            2 => ((addr & 0b00011111111100000000000000000000) >> 20) | 0b010000000000000000000000,
            3 => ((addr & 0b00001111111111100000000000000000) >> 17) | 0b011000000000000000000000,
            4 => ((addr & 0b00000111111111111000000000000000) >> 15) | 0b100000000000000000000000,
            5 => ((addr & 0b00000011111111111110000000000000) >> 13) | 0b101000000000000000000000,
            6 => ((addr & 0b00000001111111111111110000000000) >> 10) | 0b110000000000000000000000,
            7 => ((addr & 0b00000000111111111111111110000000) >> 7) | 0b111000000000000000000000,
            other => return Err(InvalidNetId(other)),
        };
        Ok(id.into())
    }
}

impl From<DevAddrConstraint> for proto::DevaddrConstraintV1 {
    fn from(constraint: DevAddrConstraint) -> Self {
        Self {
            start_addr: constraint.start_addr.into(),
            end_addr: constraint.end_addr.into(),
        }
    }
}

impl From<&proto::DevaddrConstraintV1> for DevAddrConstraint {
    fn from(constraint: &proto::DevaddrConstraintV1) -> Self {
        Self {
            start_addr: constraint.start_addr.into(),
            end_addr: constraint.end_addr.into(),
        }
    }
}

impl From<&DevAddrRange> for proto::DevaddrRangeV1 {
    fn from(range: &DevAddrRange) -> Self {
        Self {
            route_id: range.route_id.clone(),
            start_addr: range.start_addr.into(),
            end_addr: range.end_addr.into(),
        }
    }
}

impl From<DevAddrRange> for proto::DevaddrRangeV1 {
    fn from(range: DevAddrRange) -> Self {
        Self {
            route_id: range.route_id,
            start_addr: range.start_addr.into(),
            end_addr: range.end_addr.into(),
        }
    }
}

impl From<proto::DevaddrRangeV1> for DevAddrRange {
    fn from(range: proto::DevaddrRangeV1) -> Self {
        Self {
            route_id: range.route_id,
            start_addr: range.start_addr.into(),
            end_addr: range.end_addr.into(),
        }
    }
}

impl From<&proto::DevaddrRangeV1> for DevAddrRange {
    fn from(range: &proto::DevaddrRangeV1) -> Self {
        Self {
            route_id: range.route_id.clone(),
            start_addr: range.start_addr.into(),
            end_addr: range.end_addr.into(),
        }
    }
}

impl From<proto::EuiPairV1> for EuiPair {
    fn from(range: proto::EuiPairV1) -> Self {
        Self {
            route_id: range.route_id,
            app_eui: range.app_eui.into(),
            dev_eui: range.dev_eui.into(),
        }
    }
}

impl From<&proto::EuiPairV1> for EuiPair {
    fn from(range: &proto::EuiPairV1) -> Self {
        Self {
            route_id: range.route_id.clone(),
            app_eui: range.app_eui.into(),
            dev_eui: range.dev_eui.into(),
        }
    }
}

impl From<EuiPair> for proto::EuiPairV1 {
    fn from(range: EuiPair) -> Self {
        Self {
            route_id: range.route_id,
            app_eui: range.app_eui.into(),
            dev_eui: range.dev_eui.into(),
        }
    }
}

impl From<&EuiPair> for proto::EuiPairV1 {
    fn from(eui: &EuiPair) -> Self {
        Self {
            route_id: eui.route_id.clone(),
            app_eui: eui.app_eui.into(),
            dev_eui: eui.dev_eui.into(),
        }
    }
}

impl From<proto::SkfV1> for Skf {
    fn from(filter: proto::SkfV1) -> Self {
        Self {
            route_id: filter.route_id,
            devaddr: filter.devaddr.into(),
            session_key: filter.session_key,
            max_copies: filter.max_copies,
        }
    }
}

impl From<&proto::SkfV1> for Skf {
    fn from(filter: &proto::SkfV1) -> Self {
        Self {
            route_id: filter.route_id.to_owned(),
            devaddr: filter.devaddr.into(),
            session_key: filter.session_key.to_owned(),
            max_copies: filter.max_copies,
        }
    }
}

impl From<Skf> for proto::SkfV1 {
    fn from(filter: Skf) -> Self {
        Self {
            route_id: filter.route_id,
            devaddr: filter.devaddr.into(),
            session_key: filter.session_key,
            max_copies: filter.max_copies,
        }
    }
}

impl From<&Skf> for proto::SkfV1 {
    fn from(filter: &Skf) -> Self {
        Self {
            route_id: filter.route_id.to_owned(),
            devaddr: filter.devaddr.into(),
            session_key: filter.session_key.to_owned(),
            max_copies: filter.max_copies,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn range_from_net_id() {
        struct Test {
            net_id: u32,
            start_addr: u32,
            end_addr: u32,
            net_id_type: u32,
            nwk_id: u32,
        }
        let tests = vec![
            Test {
                net_id: 0xc00053,
                start_addr: 0xfc01_4c00,
                end_addr: 0xfc01_4fff,
                net_id_type: 6,
                nwk_id: 83,
            },
            Test {
                net_id: 12_582_995,
                start_addr: 4_227_943_424,
                end_addr: 4_227_944_447,
                net_id_type: 6,
                nwk_id: 83,
            },
            Test {
                net_id: 0x00001d,
                start_addr: 0x3a00_0000,
                end_addr: 0x3bff_ffff,
                net_id_type: 0,
                nwk_id: 29,
            },
            Test {
                net_id: 0x600020,
                start_addr: 0xe040_0000,
                end_addr: 0xe041_ffff,
                net_id_type: 3,
                nwk_id: 32,
            },
            Test {
                net_id: 0xe00040,
                start_addr: 0xfe00_2000,
                end_addr: 0xfe00_207f,
                net_id_type: 7,
                nwk_id: 64,
            },
        ];

        for test in tests {
            let net_id = net_id(test.net_id);
            assert_eq!(test.net_id_type, net_id.net_id_type());
            assert_eq!(test.nwk_id, net_id.nwk_id());
            assert_eq!(
                DevAddrConstraint::new(devaddr(test.start_addr), devaddr(test.end_addr))
                    .expect("invalid devaddr order"),
                net_id.full_range().expect("invalid net id")
            );
            assert_eq!(
                devaddr(test.start_addr)
                    .to_net_id()
                    .expect("invalid devaddr"),
                test.net_id.into()
            );
        }
    }

    #[test]
    fn net_id_field() {
        let field = &net_id(0xc00053);
        let val = serde_json::to_string(field).expect("serialize net id failed");
        // value includes quotes
        assert_eq!(6 + 2, val.len());
        assert_eq!(r#""C00053""#.to_string(), val);
    }

    #[test]
    fn devaddr_field() {
        let field = &devaddr(0x22ab);
        let val = serde_json::to_string(field).expect("serialize devaddr failed");
        // value includes quotes
        assert_eq!(8 + 2, val.len());
        assert_eq!(r#""000022AB""#.to_string(), val);
    }

    #[test]
    fn eui_field() {
        let field = &eui(0x0abd_68fd_e91e_e0db);
        let val = serde_json::to_string(field).expect("serialize eui failed");
        // value includes quotes
        assert_eq!(16 + 2, val.len());
        assert_eq!(r#""0ABD68FDE91EE0DB""#.to_string(), val);
    }

    #[test]
    fn wildcard_eui_field() {
        let val = EuiField::from_str("*").expect("direct from str failed");
        assert_eq!(0, val.0);
        let val: EuiField = serde_json::from_str(r#""*""#).expect("serialize from_str failed");
        assert_eq!(0, val.0);
    }
}
