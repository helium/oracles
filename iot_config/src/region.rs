use helium_proto::Region as ProtoRegion;
use serde::{de, Deserialize, Deserializer, Serialize};
use std::{fmt, str::FromStr};

#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum Region {
    Us915,
    Eu868,
    Eu433,
    Cn470,
    Cn779,
    Au915,
    As923_1,
    As923_1b,
    As923_2,
    As923_3,
    As923_4,
    Kr920,
    In865,
    Cd900_1a,
    Ru864,
}

#[derive(thiserror::Error, Debug)]
#[error("unsupported region {0}")]
pub struct UnsupportedRegionError(i32);

impl Region {
    pub fn from_i32(v: i32) -> Result<Self, UnsupportedRegionError> {
        ProtoRegion::from_i32(v)
            .map(|r| r.into())
            .ok_or_else(|| UnsupportedRegionError(v))
    }
}

impl Serialize for Region {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let a = ProtoRegion::from(self);
        serializer.serialize_str(&format!("{a}"))
    }
}

impl<'de> Deserialize<'de> for Region {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct RegionVisitor;

        impl<'de> de::Visitor<'de> for RegionVisitor {
            type Value = Region;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("region string")
            }

            fn visit_str<E>(self, value: &str) -> Result<Region, E>
            where
                E: de::Error,
            {
                let proto_region = ProtoRegion::from_str(value)
                    .map_err(|_| de::Error::custom(format!("unsupported region: {value}")))?;
                Ok(proto_region.into())
            }
        }

        deserializer.deserialize_str(RegionVisitor)
    }
}

impl From<Region> for ProtoRegion {
    fn from(region: Region) -> Self {
        ProtoRegion::from(&region)
    }
}

impl From<&Region> for ProtoRegion {
    fn from(sr: &Region) -> Self {
        match sr {
            Region::Us915 => ProtoRegion::Us915,
            Region::Eu868 => ProtoRegion::Eu868,
            Region::Eu433 => ProtoRegion::Eu433,
            Region::Cn470 => ProtoRegion::Cn470,
            Region::Cn779 => ProtoRegion::Cn779,
            Region::Au915 => ProtoRegion::Au915,
            Region::As923_1 => ProtoRegion::As9231,
            Region::As923_1b => ProtoRegion::As9231b,
            Region::As923_2 => ProtoRegion::As9232,
            Region::As923_3 => ProtoRegion::As9233,
            Region::As923_4 => ProtoRegion::As9234,
            Region::Kr920 => ProtoRegion::Kr920,
            Region::In865 => ProtoRegion::In865,
            Region::Cd900_1a => ProtoRegion::Cd9001a,
            Region::Ru864 => ProtoRegion::Ru864,
        }
    }
}

impl From<ProtoRegion> for Region {
    fn from(r: ProtoRegion) -> Self {
        match r {
            ProtoRegion::Us915 => Region::Us915,
            ProtoRegion::Eu868 => Region::Eu868,
            ProtoRegion::Eu433 => Region::Eu433,
            ProtoRegion::Cn470 => Region::Cn470,
            ProtoRegion::Cn779 => Region::Cn779,
            ProtoRegion::Au915 => Region::Au915,
            ProtoRegion::As9231 => Region::As923_1,
            ProtoRegion::As9231b => Region::As923_1b,
            ProtoRegion::As9232 => Region::As923_2,
            ProtoRegion::As9233 => Region::As923_3,
            ProtoRegion::As9234 => Region::As923_4,
            ProtoRegion::Kr920 => Region::Kr920,
            ProtoRegion::In865 => Region::In865,
            ProtoRegion::Cd9001a => Region::Cd900_1a,
            ProtoRegion::Ru864 => Region::Ru864,
        }
    }
}

impl From<Region> for i32 {
    fn from(region: Region) -> Self {
        ProtoRegion::from(region) as i32
    }
}
