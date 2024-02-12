use chrono::{DateTime, Utc};
use futures::StreamExt;
use mobile_config::{
    boosted_hex_info::BoostedHexInfo,
    client::{hex_boosting_client::HexBoostingInfoResolver, ClientError},
};
use std::collections::HashMap;

#[derive(Debug, Clone, Default)]
pub struct BoostedHexes {
    pub hexes: HashMap<u64, BoostedHexInfo>,
}

#[derive(PartialEq, Debug, Clone)]
pub struct BoostedHex {
    pub location: u64,
    pub multiplier: u32,
}

impl BoostedHexes {
    pub async fn new(
        hex_service_client: &impl HexBoostingInfoResolver<Error = ClientError>,
    ) -> anyhow::Result<Self> {
        tracing::info!("getting boosted hexes");
        let mut map = HashMap::new();
        let mut stream = hex_service_client
            .clone()
            .stream_boosted_hexes_info()
            .await?;
        while let Some(info) = stream.next().await {
            map.insert(info.location, info);
        }
        Ok(Self { hexes: map })
    }

    pub fn is_hex_boosted(&self, location: u64) -> bool {
	self.hexes.contains_key(&location)
    }

    pub fn get_current_multiplier(&self, location: u64, ts: DateTime<Utc>) -> Option<u32> {
        self.hexes
            .get(&location)
            .and_then(|info| info.current_multiplier(ts).ok()?)
    }
}
