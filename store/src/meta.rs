use std::str::FromStr;

/// A key-value pair that is stored in the metadata table.
pub struct MetaValue<T> {
    key: String,
    value: T,
}

#[derive(thiserror::Error, Debug)]
pub enum MetaError {
    #[error("Sql error")]
    SqlError(#[from] sqlx::Error),
    #[error("Failed to decode meta value")]
    DecodeError,
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
    pub async fn insert<'c, E>(&self, exec: E) -> Result<(), MetaError>
    where
        E: sqlx::Executor<'c, Database = sqlx::Postgres>,
    {
        sqlx::query(
            r#"
            insert into meta (key, value) 
            values ($1, $2) 
            on conflict (key) do update set 
            value = EXCLUDED.value;
            "#,
        )
        .bind(&self.key)
        .bind(self.value.to_string())
        .execute(exec)
        .await?;
        Ok(())
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
    ) -> Result<Self, MetaError>
    where
        E: sqlx::Executor<'c, Database = sqlx::Postgres> + Copy,
    {
        let str_val = sqlx::query_scalar::<_, String>(
            r#"
            select value from meta where key = $1;
            "#,
        )
        .bind(key)
        .fetch_optional(exec)
        .await?;

        match str_val {
            Some(str_val) => {
                let value = str_val.parse().map_err(|_| MetaError::DecodeError)?;
                Ok(Self {
                    key: key.to_string(),
                    value,
                })
            }
            None => {
                let value = default_fn();
                let res = Self::new(key, value);
                res.insert(exec).await?;
                Ok(res)
            }
        }
    }

    pub async fn update<'c, E>(&mut self, exec: E, new_val: T) -> Result<T, MetaError>
    where
        E: sqlx::Executor<'c, Database = sqlx::Postgres>,
    {
        sqlx::query(
            r#"
            insert into meta (key, value) 
            values ($1, $2) 
            on conflict (key) do update set 
            value = EXCLUDED.value;
            "#,
        )
        .bind(&self.key)
        .bind(new_val.to_string())
        .execute(exec)
        .await?;
        Ok(std::mem::replace(&mut self.value, new_val))
    }
}
