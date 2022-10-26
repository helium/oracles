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

macro_rules! query_exec_timed {
    ( $name:literal, $query:expr, $meth:ident, $exec:expr ) => {{
        match poc_metrics::record_duration!(concat!($name, "_duration"), $query.$meth($exec).await) {
            Ok(x) => {
                metrics::increment_counter!(concat!($name, "_count"), "status" => "ok");
                Ok(x)
            }
            Err(e) => {
                metrics::increment_counter!(concat!($name, "_count"), "status" => "error");
                Err(MetaError::SqlError(e))
            }
        }
    }};
}

impl<T> MetaValue<T>
where
    T: ToString,
{
    pub async fn insert<'c, E>(&self, exec: E) -> Result<(), MetaError>
    where
        E: sqlx::Executor<'c, Database = sqlx::Postgres>,
    {
        let query = sqlx::query(
            r#"
                insert into meta (key, value)
                values ($1, $2)
                on conflict (key) do update set
                value = EXCLUDED.value;
                "#,
        )
        .bind(&self.key)
        .bind(self.value.to_string());
        query_exec_timed!("db_store_metavalue_insert", query, execute, exec).map(|_| ())
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
        let query = sqlx::query_scalar::<_, String>(
            r#"
            select value from meta where key = $1;
            "#,
        )
        .bind(key);
        let str_val = query_exec_timed!("db_store_metavalue_fetch", query, fetch_optional, exec)?;

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
        E: sqlx::PgExecutor<'c>,
    {
        let query = sqlx::query(
            r#"
            insert into meta (key, value) 
            values ($1, $2) 
            on conflict (key) do update set 
            value = EXCLUDED.value;
            "#,
        )
        .bind(&self.key)
        .bind(new_val.to_string());
        let _ = query_exec_timed!("db_store_metavalue_update", query, execute, exec)?;
        Ok(std::mem::replace(&mut self.value, new_val))
    }
}
