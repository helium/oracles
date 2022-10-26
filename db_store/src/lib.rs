use std::str::FromStr;
mod error;
mod settings;

pub use error::{Error, Result};
pub use settings::Settings;

pub mod meta;

/// A key-value pair that is stored in the metadata table.
pub struct MetaValue<T> {
    key: String,
    value: T,
}

impl<T> MetaValue<T> {
    pub fn new(key: &str, value: T) -> Self {
        Self {
            key: key.to_string(),
            value,
        }
    }

    pub fn key(&self) -> &str {
        &self.key
    }

    pub fn value(&self) -> &T {
        &self.value
    }
}

impl<T> MetaValue<T>
where
    T: ToString,
{
    pub async fn insert<'c, E>(&self, exec: E) -> Result
    where
        E: sqlx::Executor<'c, Database = sqlx::Postgres>,
    {
        meta::store(exec, &self.key, &self.value.to_string()).await
    }
}

impl<T> MetaValue<T>
where
    T: ToString + FromStr,
{
    pub async fn fetch_or_insert_with<'c, E>(
        exec: E,
        key: &str,
        default_fn: impl FnOnce() -> T,
    ) -> Result<Self>
    where
        E: sqlx::Executor<'c, Database = sqlx::Postgres> + Copy,
    {
        let result: Result<String> = meta::fetch::<String>(exec, key).await;

        match result {
            Ok(str_val) => {
                let value = str_val.parse().map_err(|_| Error::DecodeError)?;
                Ok(Self {
                    key: key.to_string(),
                    value,
                })
            }
            Err(Error::NotFound(_)) => {
                let value = default_fn();
                let res = Self::new(key, value);
                res.insert(exec).await?;
                Ok(res)
            }
            Err(err) => Err(err),
        }
    }

    pub async fn update<'c, E>(&mut self, exec: E, new_val: T) -> Result<T>
    where
        E: sqlx::PgExecutor<'c>,
    {
        meta::store(exec, &self.key, new_val.to_string()).await?;
        Ok(std::mem::replace(&mut self.value, new_val))
    }
}
