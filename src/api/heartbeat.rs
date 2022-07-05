use crate::{
    api::{gateway::Gateway, internal_error, DatabaseConnection},
    datetime_from_epoch, Error, PublicKey, Result,
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
    Gateway::update_last_heartbeat(&mut conn, event.pubkey, event.timestamp)
        .await
        .map(|pubkey: Option<PublicKey>| json!({ "pubkey": pubkey }))
        .map(Json)
        .map_err(internal_error)
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

impl TryFrom<CellHeartbeatReqV1> for CellHeartbeat {
    type Error = Error;
    fn try_from(v: CellHeartbeatReqV1) -> Result<Self> {
        Ok(Self {
            pubkey: PublicKey::try_from(v.pub_key.as_ref())?,
            hotspot_type: v.hotspot_type,
            cell_id: v.cell_id,
            timestamp: datetime_from_epoch(v.timestamp as i64),
            lon: v.lon,
            lat: v.lat,
            operation_mode: v.operation_mode,
            cbsd_category: v.cbsd_category,
            cbsd_id: v.cbsd_id,
        })
    }
}
