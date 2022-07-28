use serde::{
    de::{self, Deserializer},
    ser::Serializer,
    Deserialize, Serialize,
};
use std::ops::Deref;

#[derive(Debug)]
pub struct Imsi(Vec<u8>);

impl Deref for Imsi {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<'de> Deserialize<'de> for Imsi {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        match base64::decode(s) {
            Ok(v) => Ok(Imsi(v)),
            Err(_) => Err(de::Error::custom("invalid imsi hash")),
        }
    }
}

impl Serialize for Imsi {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&base64::encode(self.deref()))
    }
}
