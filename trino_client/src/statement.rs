use crate::error::{Error, Result};
use crate::{SqlQuery, SqlStatement};
use chrono::{DateTime, NaiveDate, NaiveDateTime, Utc};
use std::marker::PhantomData;

#[derive(Debug, Clone, PartialEq)]
pub enum Param {
    Null,
    I32(i32),
    I64(i64),
    U64(u64),
    F64(f64),
    Bool(bool),
    String(String),
    Bytes(Vec<u8>),
    Date(NaiveDate),
    Timestamp(NaiveDateTime),
    TimestampTz(DateTime<Utc>),
}

impl From<i32> for Param {
    fn from(v: i32) -> Self {
        Param::I32(v)
    }
}

impl From<i64> for Param {
    fn from(v: i64) -> Self {
        Param::I64(v)
    }
}

impl From<u32> for Param {
    fn from(v: u32) -> Self {
        Param::I64(v as i64)
    }
}

impl From<u64> for Param {
    fn from(v: u64) -> Self {
        Param::U64(v)
    }
}

impl From<f32> for Param {
    fn from(v: f32) -> Self {
        Param::F64(v as f64)
    }
}

impl From<f64> for Param {
    fn from(v: f64) -> Self {
        Param::F64(v)
    }
}

impl From<bool> for Param {
    fn from(v: bool) -> Self {
        Param::Bool(v)
    }
}

impl From<&str> for Param {
    fn from(v: &str) -> Self {
        Param::String(v.to_string())
    }
}

impl From<String> for Param {
    fn from(v: String) -> Self {
        Param::String(v)
    }
}

impl From<Vec<u8>> for Param {
    fn from(v: Vec<u8>) -> Self {
        Param::Bytes(v)
    }
}

impl From<&[u8]> for Param {
    fn from(v: &[u8]) -> Self {
        Param::Bytes(v.to_vec())
    }
}

impl<T: Into<Param>> From<Option<T>> for Param {
    fn from(v: Option<T>) -> Self {
        match v {
            Some(inner) => inner.into(),
            None => Param::Null,
        }
    }
}

impl From<NaiveDate> for Param {
    fn from(v: NaiveDate) -> Self {
        Param::Date(v)
    }
}

impl From<NaiveDateTime> for Param {
    fn from(v: NaiveDateTime) -> Self {
        Param::Timestamp(v)
    }
}

impl From<DateTime<Utc>> for Param {
    fn from(v: DateTime<Utc>) -> Self {
        Param::TimestampTz(v)
    }
}

impl Param {
    fn to_literal(&self) -> Result<String> {
        match self {
            Param::Null => Ok("NULL".into()),
            Param::I32(n) => Ok(n.to_string()),
            Param::I64(n) => Ok(n.to_string()),
            Param::U64(n) => {
                if *n > i64::MAX as u64 {
                    Err(Error::InvalidParam(format!(
                        "u64 value {n} exceeds Trino BIGINT range"
                    )))
                } else {
                    Ok(n.to_string())
                }
            }
            Param::F64(n) => {
                if n.is_nan() || n.is_infinite() {
                    Err(Error::InvalidParam(format!("non-finite f64: {n}")))
                } else {
                    Ok(format!("{n:e}"))
                }
            }
            Param::Bool(b) => Ok(if *b { "TRUE".into() } else { "FALSE".into() }),
            Param::String(s) => Ok(format!("'{}'", s.replace('\'', "''"))),
            Param::Bytes(b) => {
                let hex: String = b.iter().map(|byte| format!("{byte:02x}")).collect();
                Ok(format!("X'{hex}'"))
            }
            Param::Date(d) => Ok(format!("DATE '{}'", d.format("%Y-%m-%d"))),
            Param::Timestamp(ts) => {
                Ok(format!("TIMESTAMP '{}'", ts.format("%Y-%m-%d %H:%M:%S%.f")))
            }
            Param::TimestampTz(ts) => Ok(format!(
                "TIMESTAMP '{}'",
                ts.format("%Y-%m-%d %H:%M:%S%.f UTC")
            )),
        }
    }
}

#[derive(Debug, Clone)]
pub struct Statement {
    sql: String,
    params: Vec<(String, Param)>,
}

impl Statement {
    pub fn new(sql: impl Into<String>) -> Self {
        Self {
            sql: sql.into(),
            params: Vec::new(),
        }
    }

    pub fn bind(mut self, name: impl Into<String>, value: impl Into<Param>) -> Self {
        self.params.push((name.into(), value.into()));
        self
    }

    pub fn render(&self) -> Result<String> {
        render_internal(&self.sql, &self.params)
    }

    /// Tag this statement with its row type so it can be passed to
    /// `Client::get_all` without a separate row struct.
    pub fn typed<R>(self) -> TypedStatement<R> {
        TypedStatement {
            stmt: self,
            _row: PhantomData,
        }
    }
}

impl SqlStatement for Statement {
    fn to_statement(&self) -> Statement {
        self.clone()
    }
}

/// A `Statement` paired with a row type, satisfying `SqlQuery` so it can be
/// used with `Client::get_all` directly.
pub struct TypedStatement<R> {
    stmt: Statement,
    _row: PhantomData<fn() -> R>,
}

impl<R> TypedStatement<R> {
    pub fn bind(mut self, name: impl Into<String>, value: impl Into<Param>) -> Self {
        self.stmt = self.stmt.bind(name, value);
        self
    }
}

impl<R> SqlStatement for TypedStatement<R> {
    fn to_statement(&self) -> Statement {
        self.stmt.clone()
    }
}

impl<R> SqlQuery for TypedStatement<R> {
    type Row = R;
}

enum ScanState {
    Normal,
    SingleString,
    DoubleString,
    LineComment,
    BlockComment,
}

fn render_internal(sql: &str, params: &[(String, Param)]) -> Result<String> {
    let mut out = String::with_capacity(sql.len());
    let mut chars = sql.chars().peekable();
    let mut placeholder_values: Vec<Param> = Vec::new();
    let mut state = ScanState::Normal;

    while let Some(c) = chars.next() {
        match state {
            ScanState::Normal => match c {
                '\'' => {
                    out.push(c);
                    state = ScanState::SingleString;
                }
                '"' => {
                    out.push(c);
                    state = ScanState::DoubleString;
                }
                '-' if chars.peek() == Some(&'-') => {
                    out.push(c);
                    out.push(chars.next().expect("peeked"));
                    state = ScanState::LineComment;
                }
                '/' if chars.peek() == Some(&'*') => {
                    out.push(c);
                    out.push(chars.next().expect("peeked"));
                    state = ScanState::BlockComment;
                }
                ':' => match chars.peek() {
                    Some(&n) if n.is_ascii_alphabetic() || n == '_' => {
                        let mut name = String::new();
                        while let Some(&n2) = chars.peek() {
                            if n2.is_ascii_alphanumeric() || n2 == '_' {
                                name.push(chars.next().expect("peeked"));
                            } else {
                                break;
                            }
                        }
                        let value = params
                            .iter()
                            .rev()
                            .find(|(n, _)| n == &name)
                            .map(|(_, v)| v.clone())
                            .ok_or_else(|| Error::MissingParameter(name.clone()))?;
                        out.push('?');
                        placeholder_values.push(value);
                    }
                    _ => out.push(c),
                },
                _ => out.push(c),
            },
            ScanState::SingleString => {
                out.push(c);
                if c == '\'' {
                    if chars.peek() == Some(&'\'') {
                        out.push(chars.next().expect("peeked"));
                    } else {
                        state = ScanState::Normal;
                    }
                }
            }
            ScanState::DoubleString => {
                out.push(c);
                if c == '"' {
                    if chars.peek() == Some(&'"') {
                        out.push(chars.next().expect("peeked"));
                    } else {
                        state = ScanState::Normal;
                    }
                }
            }
            ScanState::LineComment => {
                out.push(c);
                if c == '\n' {
                    state = ScanState::Normal;
                }
            }
            ScanState::BlockComment => {
                out.push(c);
                if c == '*' && chars.peek() == Some(&'/') {
                    out.push(chars.next().expect("peeked"));
                    state = ScanState::Normal;
                }
            }
        }
    }

    if placeholder_values.is_empty() {
        return Ok(out);
    }

    let escaped = out.replace('\'', "''");
    let using_clause = placeholder_values
        .iter()
        .map(Param::to_literal)
        .collect::<Result<Vec<_>>>()?
        .join(", ");

    Ok(format!(
        "EXECUTE IMMEDIATE '{escaped}' USING {using_clause}"
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn renders_i64_placeholder() {
        let r = Statement::new("SELECT * FROM foo WHERE id = :id")
            .bind("id", 42)
            .render()
            .unwrap();
        assert_eq!(
            r,
            "EXECUTE IMMEDIATE 'SELECT * FROM foo WHERE id = ?' USING 42"
        );
    }

    #[test]
    fn renders_i32_placeholder() {
        let r = Statement::new("SELECT * FROM foo WHERE n = :n")
            .bind("n", -7)
            .render()
            .unwrap();
        assert_eq!(
            r,
            "EXECUTE IMMEDIATE 'SELECT * FROM foo WHERE n = ?' USING -7"
        );
    }

    #[test]
    fn renders_f64_placeholder() {
        let r = Statement::new("SELECT :v").bind("v", 1.5).render().unwrap();
        assert_eq!(r, "EXECUTE IMMEDIATE 'SELECT ?' USING 1.5e0");
    }

    #[test]
    fn renders_f64_whole_value_with_exponent() {
        let r = Statement::new("SELECT :v").bind("v", 1.0).render().unwrap();
        assert_eq!(r, "EXECUTE IMMEDIATE 'SELECT ?' USING 1e0");
    }

    #[test]
    fn renders_bool_placeholder() {
        let r = Statement::new("SELECT * FROM foo WHERE active = :a")
            .bind("a", true)
            .render()
            .unwrap();
        assert_eq!(
            r,
            "EXECUTE IMMEDIATE 'SELECT * FROM foo WHERE active = ?' USING TRUE"
        );
    }

    #[test]
    fn renders_string_placeholder() {
        let r = Statement::new("SELECT * FROM foo WHERE name = :name")
            .bind("name", "alice")
            .render()
            .unwrap();
        assert_eq!(
            r,
            "EXECUTE IMMEDIATE 'SELECT * FROM foo WHERE name = ?' USING 'alice'"
        );
    }

    #[test]
    fn escapes_single_quote_in_string_param() {
        let r = Statement::new("SELECT * FROM foo WHERE name = :name")
            .bind("name", "O'Brien")
            .render()
            .unwrap();
        assert_eq!(
            r,
            "EXECUTE IMMEDIATE 'SELECT * FROM foo WHERE name = ?' USING 'O''Brien'"
        );
    }

    #[test]
    fn multiple_placeholders_in_order() {
        let r = Statement::new("INSERT INTO foo (a, b, c) VALUES (:a, :b, :c)")
            .bind("a", 1)
            .bind("b", "x")
            .bind("c", false)
            .render()
            .unwrap();
        assert_eq!(
            r,
            "EXECUTE IMMEDIATE 'INSERT INTO foo (a, b, c) VALUES (?, ?, ?)' USING 1, 'x', FALSE"
        );
    }

    #[test]
    fn reused_placeholder_name_repeats_value() {
        let r = Statement::new("SELECT :x, :x")
            .bind("x", 9)
            .render()
            .unwrap();
        assert_eq!(r, "EXECUTE IMMEDIATE 'SELECT ?, ?' USING 9, 9");
    }

    #[test]
    fn last_bind_wins_for_duplicate_name() {
        let r = Statement::new("SELECT :id")
            .bind("id", 1)
            .bind("id", 2)
            .render()
            .unwrap();
        assert_eq!(r, "EXECUTE IMMEDIATE 'SELECT ?' USING 2");
    }

    #[test]
    fn skips_colon_inside_single_quoted_string() {
        let r = Statement::new("SELECT 'foo:bar' AS s, :id")
            .bind("id", 7)
            .render()
            .unwrap();
        assert_eq!(r, "EXECUTE IMMEDIATE 'SELECT ''foo:bar'' AS s, ?' USING 7");
    }

    #[test]
    fn skips_colon_inside_double_quoted_identifier() {
        let r = Statement::new(r#"SELECT "weird:column" FROM foo WHERE id = :id"#)
            .bind("id", 7)
            .render()
            .unwrap();
        assert_eq!(
            r,
            r#"EXECUTE IMMEDIATE 'SELECT "weird:column" FROM foo WHERE id = ?' USING 7"#
        );
    }

    #[test]
    fn skips_colon_inside_line_comment() {
        let r = Statement::new("-- :not_a_param\nSELECT :id")
            .bind("id", 1)
            .render()
            .unwrap();
        assert_eq!(r, "EXECUTE IMMEDIATE '-- :not_a_param\nSELECT ?' USING 1");
    }

    #[test]
    fn skips_colon_inside_block_comment() {
        let r = Statement::new("/* :not_a_param */ SELECT :id")
            .bind("id", 1)
            .render()
            .unwrap();
        assert_eq!(r, "EXECUTE IMMEDIATE '/* :not_a_param */ SELECT ?' USING 1");
    }

    #[test]
    fn handles_doubled_single_quote_escape() {
        let r = Statement::new("SELECT 'it''s :inside' AS s, :id")
            .bind("id", 3)
            .render()
            .unwrap();
        assert_eq!(
            r,
            "EXECUTE IMMEDIATE 'SELECT ''it''''s :inside'' AS s, ?' USING 3"
        );
    }

    #[test]
    fn handles_doubled_double_quote_escape() {
        let r = Statement::new(r#"SELECT "a""b:c" FROM foo WHERE id = :id"#)
            .bind("id", 3)
            .render()
            .unwrap();
        assert_eq!(
            r,
            r#"EXECUTE IMMEDIATE 'SELECT "a""b:c" FROM foo WHERE id = ?' USING 3"#
        );
    }

    #[test]
    fn missing_parameter_errors() {
        let err = Statement::new("SELECT :missing").render().unwrap_err();
        assert!(matches!(err, Error::MissingParameter(ref n) if n == "missing"));
    }

    #[test]
    fn no_parameters_errors() {
        let err = Statement::new("SELECT 1").render().unwrap_err();
        assert!(matches!(err, Error::NoParameters));
    }

    #[test]
    fn nan_param_errors() {
        let err = Statement::new("SELECT :v")
            .bind("v", f64::NAN)
            .render()
            .unwrap_err();
        assert!(matches!(err, Error::InvalidParam(_)));
    }

    #[test]
    fn infinite_param_errors() {
        let err = Statement::new("SELECT :v")
            .bind("v", f64::INFINITY)
            .render()
            .unwrap_err();
        assert!(matches!(err, Error::InvalidParam(_)));
    }

    #[test]
    fn lone_colon_is_preserved() {
        let r = Statement::new("SELECT 'a' || ':' || 'b', :id")
            .bind("id", 1)
            .render()
            .unwrap();
        assert_eq!(
            r,
            "EXECUTE IMMEDIATE 'SELECT ''a'' || '':'' || ''b'', ?' USING 1"
        );
    }

    #[test]
    fn renders_date_placeholder() {
        let d = NaiveDate::from_ymd_opt(2024, 1, 15).unwrap();
        let r = Statement::new("SELECT :d").bind("d", d).render().unwrap();
        assert_eq!(r, "EXECUTE IMMEDIATE 'SELECT ?' USING DATE '2024-01-15'");
    }

    #[test]
    fn renders_naive_timestamp_without_fraction() {
        let ts = NaiveDate::from_ymd_opt(2024, 1, 15)
            .unwrap()
            .and_hms_opt(12, 34, 56)
            .unwrap();
        let r = Statement::new("SELECT :t").bind("t", ts).render().unwrap();
        assert_eq!(
            r,
            "EXECUTE IMMEDIATE 'SELECT ?' USING TIMESTAMP '2024-01-15 12:34:56'"
        );
    }

    #[test]
    fn renders_naive_timestamp_with_fractional_seconds() {
        let ts = NaiveDate::from_ymd_opt(2024, 1, 15)
            .unwrap()
            .and_hms_milli_opt(12, 34, 56, 789)
            .unwrap();
        let r = Statement::new("SELECT :t").bind("t", ts).render().unwrap();
        assert_eq!(
            r,
            "EXECUTE IMMEDIATE 'SELECT ?' USING TIMESTAMP '2024-01-15 12:34:56.789'"
        );
    }

    #[test]
    fn renders_utc_timestamp() {
        let ts = NaiveDate::from_ymd_opt(2024, 1, 15)
            .unwrap()
            .and_hms_opt(12, 34, 56)
            .unwrap()
            .and_utc();
        let r = Statement::new("SELECT :t").bind("t", ts).render().unwrap();
        assert_eq!(
            r,
            "EXECUTE IMMEDIATE 'SELECT ?' USING TIMESTAMP '2024-01-15 12:34:56 UTC'"
        );
    }

    #[test]
    fn from_impls_for_chrono_types_compile() {
        let _: Param = NaiveDate::from_ymd_opt(2024, 1, 1).unwrap().into();
        let _: Param = NaiveDate::from_ymd_opt(2024, 1, 1)
            .unwrap()
            .and_hms_opt(0, 0, 0)
            .unwrap()
            .into();
        let _: Param = Utc::now().into();
    }

    #[test]
    fn negative_integers_render_with_minus() {
        let r = Statement::new("SELECT :a, :b")
            .bind("a", -100i64)
            .bind("b", -2.5_f64)
            .render()
            .unwrap();
        assert_eq!(r, "EXECUTE IMMEDIATE 'SELECT ?, ?' USING -100, -2.5e0");
    }

    #[test]
    fn renders_u32_as_bigint() {
        let r = Statement::new("SELECT :n")
            .bind("n", 4_000_000_000u32)
            .render()
            .unwrap();
        assert_eq!(r, "EXECUTE IMMEDIATE 'SELECT ?' USING 4000000000");
    }

    #[test]
    fn renders_u64_in_bigint_range() {
        let r = Statement::new("SELECT :n")
            .bind("n", 9_000_000_000_000_000_000u64)
            .render()
            .unwrap();
        assert_eq!(r, "EXECUTE IMMEDIATE 'SELECT ?' USING 9000000000000000000");
    }

    #[test]
    fn u64_exceeding_bigint_errors() {
        let err = Statement::new("SELECT :n")
            .bind("n", u64::MAX)
            .render()
            .unwrap_err();
        assert!(matches!(err, Error::InvalidParam(_)));
    }

    #[test]
    fn renders_f32_via_f64_format() {
        let r = Statement::new("SELECT :v")
            .bind("v", 1.5_f32)
            .render()
            .unwrap();
        assert_eq!(r, "EXECUTE IMMEDIATE 'SELECT ?' USING 1.5e0");
    }

    #[test]
    fn renders_bytes_as_varbinary_hex() {
        let r = Statement::new("SELECT :b")
            .bind("b", vec![0xDEu8, 0xAD, 0xBE, 0xEF])
            .render()
            .unwrap();
        assert_eq!(r, "EXECUTE IMMEDIATE 'SELECT ?' USING X'deadbeef'");
    }

    #[test]
    fn renders_empty_bytes() {
        let r = Statement::new("SELECT :b")
            .bind("b", Vec::<u8>::new())
            .render()
            .unwrap();
        assert_eq!(r, "EXECUTE IMMEDIATE 'SELECT ?' USING X''");
    }

    #[test]
    fn renders_byte_slice() {
        let bytes: &[u8] = &[0x01, 0x02, 0x03];
        let r = Statement::new("SELECT :b")
            .bind("b", bytes)
            .render()
            .unwrap();
        assert_eq!(r, "EXECUTE IMMEDIATE 'SELECT ?' USING X'010203'");
    }

    #[test]
    fn renders_option_none_as_null() {
        let none: Option<i64> = None;
        let r = Statement::new("SELECT :v")
            .bind("v", none)
            .render()
            .unwrap();
        assert_eq!(r, "EXECUTE IMMEDIATE 'SELECT ?' USING NULL");
    }

    #[test]
    fn renders_option_some_as_inner_value() {
        let some: Option<i64> = Some(7);
        let r = Statement::new("SELECT :v")
            .bind("v", some)
            .render()
            .unwrap();
        assert_eq!(r, "EXECUTE IMMEDIATE 'SELECT ?' USING 7");
    }

    #[test]
    fn renders_option_string() {
        let r = Statement::new("SELECT :a, :b")
            .bind("a", Some("hi".to_string()))
            .bind("b", None::<String>)
            .render()
            .unwrap();
        assert_eq!(r, "EXECUTE IMMEDIATE 'SELECT ?, ?' USING 'hi', NULL");
    }

    #[test]
    fn statement_satisfies_sql_statement() {
        let stmt = Statement::new("SELECT :x").bind("x", 1i64);
        let copy = stmt.to_statement();
        assert_eq!(
            copy.render().unwrap(),
            "EXECUTE IMMEDIATE 'SELECT ?' USING 1"
        );
    }

    #[test]
    fn typed_wraps_statement() {
        struct DummyRow;
        let typed: TypedStatement<DummyRow> = Statement::new("SELECT :x").bind("x", 1i64).typed();
        let rendered = typed.to_statement().render().unwrap();
        assert_eq!(rendered, "EXECUTE IMMEDIATE 'SELECT ?' USING 1");
    }

    #[test]
    fn typed_allows_late_binds() {
        struct DummyRow;
        let typed: TypedStatement<DummyRow> = Statement::new("SELECT :a, :b")
            .bind("a", 1i64)
            .typed()
            .bind("b", "x");
        let rendered = typed.to_statement().render().unwrap();
        assert_eq!(rendered, "EXECUTE IMMEDIATE 'SELECT ?, ?' USING 1, 'x'");
    }

    #[test]
    fn typed_statement_exposes_row_type() {
        struct DummyRow;
        let typed: TypedStatement<DummyRow> = Statement::new("SELECT :x").bind("x", 1i64).typed();
        fn assert_row_is_dummy<S: SqlQuery<Row = DummyRow>>(_: &S) {}
        assert_row_is_dummy(&typed);
    }
}
