use serde::{
    de::{self, Deserializer},
    ser::Serializer,
    Deserialize, Serialize,
};
use std::ops::Deref;

#[derive(Debug, sqlx::Type, Default)]
#[sqlx(transparent)]
pub struct Uuid(sqlx::types::Uuid);

impl Deref for Uuid {
    type Target = sqlx::types::Uuid;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl Uuid {
    pub fn nil() -> Self {
        Self(sqlx::types::Uuid::nil())
    }
}

impl<'de> Deserialize<'de> for Uuid {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        match sqlx::types::Uuid::parse_str(&s) {
            Ok(v) => Ok(Uuid(v)),
            Err(_) => Err(de::Error::custom("invalid uuid")),
        }
    }
}

impl Serialize for Uuid {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&self.to_string())
    }
}
