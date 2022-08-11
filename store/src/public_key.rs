use crate::{Error, Result};
use serde::{
    de::{self, Deserializer},
    ser::Serializer,
    Deserialize, Serialize,
};
use std::{ops::Deref, str::FromStr};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PublicKey(pub helium_crypto::PublicKey);

impl Deref for PublicKey {
    type Target = helium_crypto::PublicKey;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl AsRef<helium_crypto::PublicKey> for PublicKey {
    fn as_ref(&self) -> &helium_crypto::PublicKey {
        &self.0
    }
}

impl<'de> Deserialize<'de> for PublicKey {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        match helium_crypto::PublicKey::from_str(&s) {
            Ok(v) => Ok(Self(v)),
            Err(_) => Err(de::Error::custom("invalid public_key")),
        }
    }
}

impl Serialize for PublicKey {
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&self.to_string())
    }
}

impl TryFrom<&[u8]> for PublicKey {
    type Error = Error;
    fn try_from(value: &[u8]) -> Result<Self> {
        Ok(Self(helium_crypto::PublicKey::try_from(value)?))
    }
}

impl TryFrom<Vec<u8>> for PublicKey {
    type Error = Error;
    fn try_from(value: Vec<u8>) -> Result<Self> {
        Ok(Self(helium_crypto::PublicKey::try_from(value)?))
    }
}

impl TryFrom<&Vec<u8>> for PublicKey {
    type Error = Error;
    fn try_from(value: &Vec<u8>) -> Result<Self> {
        Ok(Self(helium_crypto::PublicKey::try_from(value.as_ref())?))
    }
}

impl std::fmt::Display for PublicKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

impl std::str::FromStr for PublicKey {
    type Err = Error;
    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        Ok(Self(helium_crypto::PublicKey::from_str(s)?))
    }
}
