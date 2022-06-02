use crate::{
    api::{internal_error, not_found_error, DatabaseConnection},
    CellAttachEvent, Uuid,
};
use axum::{extract::Path, http::StatusCode, Json};
use serde_json::{json, Value};

pub async fn create_cell_attach_event(
    Json(event): Json<CellAttachEvent>,
    DatabaseConnection(mut conn): DatabaseConnection,
) -> std::result::Result<Json<Value>, (StatusCode, String)> {
    event
        .insert_into(&mut conn)
        .await
        .map(|id: Uuid| {
            json!({
                "id": id,
            })
        })
        .map(Json)
        .map_err(internal_error)
}

pub async fn get_cell_attach_event(
    Path(id): Path<Uuid>,
    DatabaseConnection(mut conn): DatabaseConnection,
) -> std::result::Result<Json<Value>, (StatusCode, String)> {
    let event = CellAttachEvent::get(&mut conn, &id)
        .await
        .map_err(internal_error)?;
    if let Some(event) = event {
        let json = serde_json::to_value(event).map_err(internal_error)?;
        Ok(Json(json))
    } else {
        Err(not_found_error())
    }
}
