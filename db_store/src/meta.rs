use std::str::FromStr;

use crate::MetaError;

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

pub async fn store<T>(exec: impl sqlx::PgExecutor<'_>, key: &str, value: T) -> Result<(), MetaError>
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

pub async fn fetch<T>(exec: impl sqlx::PgExecutor<'_>, key: &str) -> Result<T, MetaError>
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
        .ok_or_else(|| MetaError::NotFound(key.to_string()))
        .and_then(|value| value.parse().map_err(|_| MetaError::DecodeError))
}
