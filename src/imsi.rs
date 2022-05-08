use serde::{
    de::{self, Deserializer, Visitor},
    Deserialize,
};
use std::fmt;

pub struct Imsi(Vec<u8>);

impl<'de> Deserialize<'de> for Imsi {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct HashVisitor;

        impl<'de> Visitor<'de> for HashVisitor {
            type Value = Imsi;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("base64 string")
            }

            fn visit_str<E>(self, value: &str) -> std::result::Result<Imsi, E>
            where
                E: de::Error,
            {
                match base64::decode(value) {
                    Ok(v) => Ok(Imsi(v)),
                    Err(_) => Err(de::Error::custom("invalid imsi hash")),
                }
            }
        }

        deserializer.deserialize_str(HashVisitor)
    }
}
