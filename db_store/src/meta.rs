use std::str::FromStr;

use crate::{Error, Result};

macro_rules! query_exec_timed {
    ( $name:literal, $query:expr, $meth:ident, $exec:expr ) => {{
        match poc_metrics::record_duration!(concat!($name, "_duration"), $query.$meth($exec).await) {
            Ok(x) => {
                metrics::counter!(concat!($name, "_count"), "status" => "ok").increment(1);
                Ok(x)
            }
            Err(e) => {
                metrics::counter!(concat!($name, "_count"), "status" => "error").increment(1);
                Err(Error::SqlError(e))
            }
        }
    }};
}

pub async fn store<T>(exec: impl sqlx::PgExecutor<'_>, key: &str, value: T) -> Result
where
    T: ToString,
{
    let query = sqlx::query(
        r#"
            insert into meta(key, value)
            values ($1, $2)
            on conflict (key) do update set
            value = EXCLUDED.value
            "#,
    )
    .bind(key)
    .bind(value.to_string());
    query_exec_timed!("db_store_meta_store", query, execute, exec).map(|_| ())
}

pub async fn fetch<T>(exec: impl sqlx::PgExecutor<'_>, key: &str) -> Result<T>
where
    T: FromStr,
{
    let query = sqlx::query_scalar::<_, String>(
        r#"
            select value from meta where key = $1
            "#,
    )
    .bind(key);
    query_exec_timed!("db_store_meta_fetch", query, fetch_optional, exec)?
        .ok_or_else(|| Error::NotFound(key.to_string()))
        .and_then(|value| value.parse().map_err(|_| Error::DecodeError))
}
