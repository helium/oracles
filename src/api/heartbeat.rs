use crate::{
    api::{api_error, gateway::Gateway, DatabaseConnection},
    Error, EventId, PublicKey, Result,
};
use axum::{http::StatusCode, Json};
use chrono::{DateTime, Utc};
use helium_proto::services::poc_mobile::CellHeartbeatReqV1;
use serde::Deserialize;
use serde_json::{json, Value};

pub async fn create_cell_heartbeat(
    Json(event): Json<CellHeartbeat>,
    DatabaseConnection(mut conn): DatabaseConnection,
) -> std::result::Result<Json<Value>, (StatusCode, String)> {
    Gateway::update_last_heartbeat(&mut conn, &event.pubkey, &event.timestamp)
        .await
        .and_then(|_| EventId::try_from(event))
        .map(|id| json!({ "id": id }))
        .map(Json)
        .map_err(api_error)
}

#[derive(Deserialize)]
pub struct CellHeartbeat {
    #[serde(alias = "pubKey")]
    pub pubkey: PublicKey,
    pub hotspot_type: String,
    pub cell_id: u32,
    pub timestamp: DateTime<Utc>,
    #[serde(alias = "longitude")]
    pub lon: f64,
    #[serde(alias = "latitude")]
    pub lat: f64,
    pub operation_mode: bool,
    pub cbsd_category: String,
    pub cbsd_id: String,
}

impl TryFrom<CellHeartbeat> for CellHeartbeatReqV1 {
    type Error = Error;
    fn try_from(v: CellHeartbeat) -> Result<Self> {
        Ok(Self {
            pub_key: v.pubkey.to_vec(),
            hotspot_type: v.hotspot_type,
            cell_id: v.cell_id,
            timestamp: v.timestamp.timestamp() as u64,
            lon: v.lon,
            lat: v.lat,
            operation_mode: v.operation_mode,
            cbsd_category: v.cbsd_category,
            cbsd_id: v.cbsd_id,
            signature: vec![],
        })
    }
}
