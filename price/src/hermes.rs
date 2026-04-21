use anyhow::{anyhow, Result};
use rust_decimal::Decimal;
use serde::Deserialize;

pub const HNT_DECIMALS: i32 = 8;

#[derive(Debug, Deserialize)]
pub struct HermesResponse {
    pub parsed: Vec<HermesParsedPrice>,
}

#[derive(Debug, Deserialize)]
pub struct HermesParsedPrice {
    pub id: String,
    pub price: HermesPrice,
}

#[derive(Debug, Deserialize)]
pub struct HermesPrice {
    pub price: String,
    pub conf: String,
    pub expo: i32,
    pub publish_time: i64,
}

impl HermesPrice {
    pub fn scaled_u64(&self) -> Result<u64> {
        let raw = self
            .price
            .parse::<Decimal>()
            .map_err(|err| anyhow!("failed to parse price {:?}: {err}", self.price))?;
        let scale = HNT_DECIMALS + self.expo;
        let scaled = if scale >= 0 {
            raw * Decimal::from(10_u64.pow(scale as u32))
        } else {
            raw / Decimal::from(10_u64.pow((-scale) as u32))
        };
        scaled.try_into().map_err(anyhow::Error::from)
    }
}

pub async fn fetch(client: &reqwest::Client, url: &str) -> Result<HermesParsedPrice> {
    let response: HermesResponse = client
        .get(url)
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;

    response
        .parsed
        .into_iter()
        .next()
        .ok_or_else(|| anyhow!("hermes response had no parsed price entries"))
}
