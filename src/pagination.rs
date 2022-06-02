use chrono::{DateTime, Utc};
use serde::Deserialize;

#[derive(Deserialize)]
pub struct Since {
    pub since: Option<DateTime<Utc>>,
    pub count: Option<usize>,
}
