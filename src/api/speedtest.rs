use crate::{
    api::{gateway::Gateway, internal_error, DatabaseConnection},
    Error, EventId, PublicKey, Result,
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
    Gateway::update_last_speedtest(&mut conn, &event.pubkey, &event.timestamp)
        .await
        .and_then(|_| EventId::try_from(event))
        .map(|id| json!({ "id": id }))
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
    pub upload_speed: u64,
    #[serde(alias = "downloadSpeed")]
    pub download_speed: u64,
    pub latency: u32,
}

impl TryFrom<CellSpeedtest> for SpeedtestReqV1 {
    type Error = Error;
    fn try_from(v: CellSpeedtest) -> Result<Self> {
        Ok(SpeedtestReqV1 {
            pub_key: v.pubkey.to_vec(),
            serial: v.serial,
            timestamp: v.timestamp.timestamp() as u64,
            upload_speed: v.upload_speed,
            download_speed: v.download_speed,
            latency: v.latency,
            signature: vec![],
        })
    }
}
