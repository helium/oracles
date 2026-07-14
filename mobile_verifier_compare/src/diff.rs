use std::collections::BTreeMap;
use std::fmt::Display;

/// Default absolute epsilon for floating-point parity checks.
pub const DEFAULT_F64_EPSILON: f64 = 1e-9;

/// Status of a single key in the side-by-side diff.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DiffStatus {
    Match,
    Mismatch,
    PgOnly,
    TrinoOnly,
}

impl Display for DiffStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let s = match self {
            DiffStatus::Match => "MATCH",
            DiffStatus::Mismatch => "MISMATCH",
            DiffStatus::PgOnly => "PG_ONLY",
            DiffStatus::TrinoOnly => "TRINO_ONLY",
        };
        f.pad(s)
    }
}

/// Trait for comparing two values with an epsilon. Implemented for the common
/// numeric types we care about; blanket impls cover Vec and slice equality.
pub trait FloatEq {
    fn float_eq(&self, other: &Self, epsilon: f64) -> bool;
}

impl FloatEq for i64 {
    fn float_eq(&self, other: &Self, _: f64) -> bool {
        self == other
    }
}

impl FloatEq for u64 {
    fn float_eq(&self, other: &Self, _: f64) -> bool {
        self == other
    }
}

impl FloatEq for f64 {
    fn float_eq(&self, other: &Self, epsilon: f64) -> bool {
        (self - other).abs() <= epsilon
    }
}

impl FloatEq for String {
    fn float_eq(&self, other: &Self, _: f64) -> bool {
        self == other
    }
}

impl<T: FloatEq> FloatEq for Vec<T> {
    fn float_eq(&self, other: &Self, epsilon: f64) -> bool {
        self.len() == other.len()
            && self
                .iter()
                .zip(other.iter())
                .all(|(a, b)| a.float_eq(b, epsilon))
    }
}

impl<T: FloatEq> FloatEq for Option<T> {
    fn float_eq(&self, other: &Self, epsilon: f64) -> bool {
        match (self, other) {
            (Some(a), Some(b)) => a.float_eq(b, epsilon),
            (None, None) => true,
            _ => false,
        }
    }
}

/// One row in the diff table.
pub struct DiffRow {
    pub key: String,
    pub pg: String,
    pub trino: String,
    pub delta: String,
    pub status: DiffStatus,
}

/// Build and print a side-by-side comparison between two keyed result sets.
pub struct DiffTable {
    pub title: String,
    pub header_note: Option<String>,
    pub key_column: String,
    pub value_column: String,
    pub rows: Vec<DiffRow>,
    pub show_all: bool,
    pub limit: usize,
}

impl DiffTable {
    pub fn new(
        title: impl Into<String>,
        key_column: impl Into<String>,
        value_column: impl Into<String>,
    ) -> Self {
        Self {
            title: title.into(),
            header_note: None,
            key_column: key_column.into(),
            value_column: value_column.into(),
            rows: Vec::new(),
            show_all: false,
            limit: 0,
        }
    }

    pub fn with_note(mut self, note: impl Into<String>) -> Self {
        self.header_note = Some(note.into());
        self
    }

    pub fn with_options(mut self, show_all: bool, limit: usize) -> Self {
        self.show_all = show_all;
        self.limit = limit;
        self
    }

    /// Push a pre-built row directly. Kept on the public surface for callers
    /// that don't go through `fill` (none today). Binary-crate dead-code lint
    /// would otherwise flag this — `#[allow]` documents the intent.
    #[allow(dead_code)]
    pub fn push_row(&mut self, row: DiffRow) {
        self.rows.push(row);
    }

    /// Convenience: feed two `BTreeMap`s, a stringifier for the value, and a
    /// FloatEq comparator. Produces one `DiffRow` per key in the union.
    pub fn fill<K, V, FmtK, FmtV, Sub>(
        &mut self,
        pg_rows: BTreeMap<K, V>,
        trino_rows: BTreeMap<K, V>,
        epsilon: f64,
        fmt_key: FmtK,
        fmt_value: FmtV,
        delta: Sub,
    ) where
        K: Ord + Clone,
        V: FloatEq + Clone,
        FmtK: Fn(&K) -> String,
        FmtV: Fn(&V) -> String,
        Sub: Fn(&V, &V) -> String,
    {
        let mut keys: Vec<K> = pg_rows.keys().chain(trino_rows.keys()).cloned().collect();
        keys.sort();
        keys.dedup();

        for key in keys {
            let pg = pg_rows.get(&key);
            let trino = trino_rows.get(&key);
            let (pg_str, trino_str, delta_str, status) = match (pg, trino) {
                (Some(p), Some(t)) => {
                    let status = if p.float_eq(t, epsilon) {
                        DiffStatus::Match
                    } else {
                        DiffStatus::Mismatch
                    };
                    (fmt_value(p), fmt_value(t), delta(p, t), status)
                }
                (Some(p), None) => (
                    fmt_value(p),
                    String::new(),
                    String::new(),
                    DiffStatus::PgOnly,
                ),
                (None, Some(t)) => (
                    String::new(),
                    fmt_value(t),
                    String::new(),
                    DiffStatus::TrinoOnly,
                ),
                (None, None) => unreachable!("key came from one of the two maps"),
            };
            self.rows.push(DiffRow {
                key: fmt_key(&key),
                pg: pg_str,
                trino: trino_str,
                delta: delta_str,
                status,
            });
        }
    }

    pub fn print(&self) {
        let mut matched = 0usize;
        let mut mismatched = 0usize;
        let mut pg_only = 0usize;
        let mut trino_only = 0usize;
        for row in &self.rows {
            match row.status {
                DiffStatus::Match => matched += 1,
                DiffStatus::Mismatch => mismatched += 1,
                DiffStatus::PgOnly => pg_only += 1,
                DiffStatus::TrinoOnly => trino_only += 1,
            }
        }

        println!();
        println!("=== {} ===", self.title);
        if let Some(note) = &self.header_note {
            println!("note: {note}");
        }

        let mut to_render: Vec<&DiffRow> = if self.show_all {
            self.rows.iter().collect()
        } else {
            self.rows
                .iter()
                .filter(|r| !matches!(r.status, DiffStatus::Match))
                .collect()
        };
        if self.limit > 0 && to_render.len() > self.limit {
            to_render.truncate(self.limit);
        }

        let key_w = to_render
            .iter()
            .map(|r| r.key.chars().count())
            .max()
            .unwrap_or(0)
            .max(self.key_column.chars().count());
        let pg_w = to_render
            .iter()
            .map(|r| r.pg.chars().count())
            .max()
            .unwrap_or(0)
            .max(("pg ".to_string() + &self.value_column).chars().count());
        let trino_w = to_render
            .iter()
            .map(|r| r.trino.chars().count())
            .max()
            .unwrap_or(0)
            .max(("trino ".to_string() + &self.value_column).chars().count());
        let delta_w = to_render
            .iter()
            .map(|r| r.delta.chars().count())
            .max()
            .unwrap_or(0)
            .max("delta".len());
        let status_w = "TRINO_ONLY".len();

        if to_render.is_empty() {
            println!("(no rows to display)");
        } else {
            println!(
                "{:key_w$}  {:pg_w$}  {:trino_w$}  {:delta_w$}  {:status_w$}",
                self.key_column,
                format!("pg {}", self.value_column),
                format!("trino {}", self.value_column),
                "delta",
                "status",
                key_w = key_w,
                pg_w = pg_w,
                trino_w = trino_w,
                delta_w = delta_w,
                status_w = status_w,
            );
            println!(
                "{}  {}  {}  {}  {}",
                "-".repeat(key_w),
                "-".repeat(pg_w),
                "-".repeat(trino_w),
                "-".repeat(delta_w),
                "-".repeat(status_w),
            );
            for row in &to_render {
                println!(
                    "{:key_w$}  {:pg_w$}  {:trino_w$}  {:delta_w$}  {:status_w$}",
                    row.key,
                    row.pg,
                    row.trino,
                    row.delta,
                    row.status,
                    key_w = key_w,
                    pg_w = pg_w,
                    trino_w = trino_w,
                    delta_w = delta_w,
                    status_w = status_w,
                );
            }
        }
        let hidden = self.rows.len().saturating_sub(to_render.len());
        println!(
            "totals: match={matched} mismatch={mismatched} pg_only={pg_only} trino_only={trino_only} (hidden={hidden})"
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn diff_table_categorises_all_four_statuses() {
        let mut pg = BTreeMap::new();
        pg.insert("a", 1u64);
        pg.insert("b", 2);
        pg.insert("c", 3);
        let mut trino = BTreeMap::new();
        trino.insert("a", 1u64); // match
        trino.insert("b", 4); // mismatch
        trino.insert("d", 5); // trino only
                              // c -> pg only

        let mut table = DiffTable::new("test", "key", "value");
        table.fill(
            pg,
            trino,
            DEFAULT_F64_EPSILON,
            |k| k.to_string(),
            |v| v.to_string(),
            |a, b| (*a as i128 - *b as i128).to_string(),
        );

        let by_key: std::collections::HashMap<_, _> = table
            .rows
            .iter()
            .map(|r| (r.key.clone(), r.status))
            .collect();
        assert_eq!(by_key["a"], DiffStatus::Match);
        assert_eq!(by_key["b"], DiffStatus::Mismatch);
        assert_eq!(by_key["c"], DiffStatus::PgOnly);
        assert_eq!(by_key["d"], DiffStatus::TrinoOnly);
    }

    #[test]
    fn float_eq_respects_epsilon() {
        assert!(1.0f64.float_eq(&1.0000000001, 1e-9));
        assert!(!1.0f64.float_eq(&1.01, 1e-9));
    }

    #[test]
    fn vec_float_eq_requires_same_length() {
        let a: Vec<f64> = vec![1.0, 2.0];
        let b: Vec<f64> = vec![1.0, 2.0, 3.0];
        assert!(!a.float_eq(&b, 1e-9));
    }
}
