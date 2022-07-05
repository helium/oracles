use crate::{
    api::{gateway::Gateway, internal_error, DatabaseConnection},
    datetime_from_epoch, Error, PublicKey, Result,
};
use axum::{http::StatusCode, Json};
use chrono::{DateTime, Utc};
use helium_proto::services::poc_mobile::SpeedtestReqV1;
use serde::Deserialize;
use serde_json::{json, Value};

pub async fn create_cell_speedtest(
    Json(event): Json<CellSpeedtest>,
    DatabaseConnection(mut conn): DatabaseConnection,
) -> std::result::Result<Json<Value>, (StatusCode, String)> {
    Gateway::update_last_speedtest(&mut conn, event.pubkey, event.timestamp)
        .await
        .map(|pubkey: Option<PublicKey>| {
            json!({
                "pubkey": pubkey,
            })
        })
        .map(Json)
        .map_err(internal_error)
}

#[derive(Deserialize)]
pub struct CellSpeedtest {
    #[serde(alias = "pubKey")]
    pub pubkey: PublicKey,
    pub serial: String,
    pub timestamp: DateTime<Utc>,
    #[serde(alias = "uploadSpeed")]
    pub upload_speed: i64,
    #[serde(alias = "downloadSpeed")]
    pub download_speed: i64,
    pub latency: i32,
}

impl TryFrom<SpeedtestReqV1> for CellSpeedtest {
    type Error = Error;
    fn try_from(v: SpeedtestReqV1) -> Result<Self> {
        Ok(Self {
            pubkey: PublicKey::try_from(v.pub_key.as_ref())?,
            serial: v.serial,
            timestamp: datetime_from_epoch(v.timestamp as i64),
            upload_speed: v.upload_speed as i64,
            download_speed: v.download_speed as i64,
            latency: v.latency as i32,
        })
    }
}
