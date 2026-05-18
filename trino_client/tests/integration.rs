use chrono::{NaiveDate, TimeZone, Utc};
use serde::{Deserialize, Serialize};
use trino_client::{Client, Settings, SqlQuery, SqlStatement, Statement};
use trino_rust_client::Trino;

fn client() -> Client {
    let host = std::env::var("TRINO_HOST").unwrap_or_else(|_| "localhost".to_string());

    let settings = Settings {
        host,
        port: 8080,
        user: "admin".into(),
        catalog: None,
        schema: None,
        secure: false,
        ca_cert_path: None,
        insecure_skip_tls_verify: false,
        auth: None,
    };

    Client::from_settings(&settings).expect("build trino client")
}

#[derive(Debug, Clone, Trino, Deserialize, Serialize, PartialEq)]
struct Scalars {
    n: i64,
    s: String,
    b: bool,
    d: f64,
}

#[derive(Debug, Clone, Trino, Deserialize, Serialize, PartialEq)]
struct DateRow {
    d: NaiveDate,
}

#[derive(Debug, Clone, Trino, Deserialize, Serialize, PartialEq)]
struct StringRow {
    s: String,
}

#[derive(Debug, Clone, Trino, Deserialize, Serialize, PartialEq)]
struct EqRow {
    eq: bool,
}

#[tokio::test]
async fn parameterized_scalars_round_trip() -> anyhow::Result<()> {
    let sql = Statement::new("SELECT :n AS n, :s AS s, :b AS b, :d AS d")
        .bind("n", 42i64)
        .bind("s", "alice")
        .bind("b", true)
        .bind("d", 1.5_f64)
        .render()?;
    let rows: Vec<Scalars> = client().get_all_raw(sql).await?;
    assert_eq!(
        rows,
        vec![Scalars {
            n: 42,
            s: "alice".into(),
            b: true,
            d: 1.5,
        }]
    );
    Ok(())
}

#[tokio::test]
async fn parameterized_date_round_trip() -> anyhow::Result<()> {
    let d = NaiveDate::from_ymd_opt(2024, 1, 15).unwrap();
    let sql = Statement::new("SELECT :d AS d").bind("d", d).render()?;
    let rows: Vec<DateRow> = client().get_all_raw(sql).await?;
    assert_eq!(rows, vec![DateRow { d }]);
    Ok(())
}

#[tokio::test]
async fn naive_timestamp_matches_trino_literal() -> anyhow::Result<()> {
    let t = NaiveDate::from_ymd_opt(2024, 1, 15)
        .unwrap()
        .and_hms_milli_opt(12, 34, 56, 789)
        .unwrap();
    let sql = Statement::new("SELECT (TIMESTAMP '2024-01-15 12:34:56.789' = :t) AS eq")
        .bind("t", t)
        .render()?;
    let rows: Vec<EqRow> = client().get_all_raw(sql).await?;
    assert_eq!(rows, vec![EqRow { eq: true }]);
    Ok(())
}

#[tokio::test]
async fn utc_timestamp_matches_trino_literal() -> anyhow::Result<()> {
    let t = Utc
        .with_ymd_and_hms(2024, 1, 15, 12, 34, 56)
        .single()
        .unwrap();
    let sql = Statement::new("SELECT (TIMESTAMP '2024-01-15 12:34:56 UTC' = :t) AS eq")
        .bind("t", t)
        .render()?;
    let rows: Vec<EqRow> = client().get_all_raw(sql).await?;
    assert_eq!(rows, vec![EqRow { eq: true }]);
    Ok(())
}

#[tokio::test]
async fn string_with_apostrophe_round_trip() -> anyhow::Result<()> {
    let s = "O'Brien".to_string();
    let sql = Statement::new("SELECT :s AS s")
        .bind("s", s.clone())
        .render()?;
    let rows: Vec<StringRow> = client().get_all_raw(sql).await?;
    assert_eq!(rows, vec![StringRow { s }]);
    Ok(())
}

#[tokio::test]
async fn execute_runs_a_parameterized_statement() -> anyhow::Result<()> {
    let sql = Statement::new("SELECT :x").bind("x", 1i64).render()?;
    client().execute_raw(sql).await?;
    Ok(())
}

/// A struct that owns its bind values and implements SqlStatement —
/// proves the execute() path works through the trait.
struct Ping {
    value: i64,
}

impl SqlStatement for Ping {
    fn to_statement(&self) -> Statement {
        Statement::new("SELECT :v").bind("v", self.value)
    }
}

#[tokio::test]
async fn sql_statement_executes_via_client() -> anyhow::Result<()> {
    client().execute(&Ping { value: 7 }).await?;
    Ok(())
}

/// A typed query: SqlStatement + SqlQuery binds Row to the struct,
/// so callers don't need a turbofish on .get_all().
struct FindUser {
    user_id: i64,
}

#[derive(Debug, Clone, Trino, Deserialize, Serialize, PartialEq)]
struct User {
    id: i64,
    name: String,
}

impl SqlStatement for FindUser {
    fn to_statement(&self) -> Statement {
        Statement::new("SELECT :id AS id, 'user-' || CAST(:id AS VARCHAR) AS name")
            .bind("id", self.user_id)
    }
}

impl SqlQuery for FindUser {
    type Row = User;
}

#[tokio::test]
async fn sql_query_returns_typed_rows() -> anyhow::Result<()> {
    let rows = client().get_all(&FindUser { user_id: 42 }).await?;
    assert_eq!(
        rows,
        vec![User {
            id: 42,
            name: "user-42".into(),
        }]
    );
    Ok(())
}

#[tokio::test]
async fn none_option_binds_as_null() -> anyhow::Result<()> {
    let none: Option<i64> = None;
    let sql = Statement::new("SELECT (:v IS NULL) AS eq")
        .bind("v", none)
        .render()?;
    let rows: Vec<EqRow> = client().get_all_raw(sql).await?;
    assert_eq!(rows, vec![EqRow { eq: true }]);
    Ok(())
}

#[tokio::test]
async fn some_option_binds_as_inner_value() -> anyhow::Result<()> {
    let sql = Statement::new("SELECT (:v = CAST(42 AS BIGINT)) AS eq")
        .bind("v", Some(42i64))
        .render()?;
    let rows: Vec<EqRow> = client().get_all_raw(sql).await?;
    assert_eq!(rows, vec![EqRow { eq: true }]);
    Ok(())
}

#[tokio::test]
async fn u64_binds_as_bigint() -> anyhow::Result<()> {
    let sql = Statement::new("SELECT (:n = CAST(9000000000 AS BIGINT)) AS eq")
        .bind("n", 9_000_000_000u64)
        .render()?;
    let rows: Vec<EqRow> = client().get_all_raw(sql).await?;
    assert_eq!(rows, vec![EqRow { eq: true }]);
    Ok(())
}

#[tokio::test]
async fn bytes_bind_as_varbinary() -> anyhow::Result<()> {
    let bytes = vec![0xDEu8, 0xAD, 0xBE, 0xEF];
    let sql = Statement::new("SELECT (:b = X'deadbeef') AS eq")
        .bind("b", bytes)
        .render()?;
    let rows: Vec<EqRow> = client().get_all_raw(sql).await?;
    assert_eq!(rows, vec![EqRow { eq: true }]);
    Ok(())
}
