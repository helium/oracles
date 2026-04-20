use anyhow::{anyhow, Result};
use chrono::{TimeZone, Utc};
use rust_decimal::Decimal;
use serde::Deserialize;

const HNT_DECIMALS: i32 = 8;

#[derive(Debug, Deserialize)]
struct HermesResponse {
    parsed: Vec<HermesParsedPrice>,
}

#[derive(Debug, Deserialize)]
struct HermesParsedPrice {
    id: String,
    price: HermesPrice,
}

#[derive(Debug, Deserialize)]
struct HermesPrice {
    price: String,
    conf: String,
    expo: i32,
    publish_time: i64,
}

pub async fn run(url: String) -> Result<()> {
    let response: HermesResponse = reqwest::get(&url).await?.error_for_status()?.json().await?;

    let parsed = response
        .parsed
        .into_iter()
        .next()
        .ok_or_else(|| anyhow!("hermes response had no parsed price entries"))?;

    let raw = parsed
        .price
        .price
        .parse::<Decimal>()
        .map_err(|err| anyhow!("failed to parse price {:?}: {err}", parsed.price.price))?;
    let scale = HNT_DECIMALS + parsed.price.expo;
    let scaled = if scale >= 0 {
        raw * Decimal::from(10_u64.pow(scale as u32))
    } else {
        raw / Decimal::from(10_u64.pow((-scale) as u32))
    };
    let scaled_u64: u64 = scaled.try_into()?;
    let timestamp = Utc
        .timestamp_opt(parsed.price.publish_time, 0)
        .single()
        .ok_or_else(|| anyhow!("invalid publish_time {}", parsed.price.publish_time))?;

    println!("URL: {url}");
    println!("Feed ID: {}", parsed.id);
    println!("Raw price: {}", parsed.price.price);
    println!("Confidence: {}", parsed.price.conf);
    println!("Exponent: {}", parsed.price.expo);
    println!("Publish time: {timestamp} ({})", parsed.price.publish_time);
    println!();
    println!("Scaled integer price (as emitted to S3): {scaled_u64}");

    Ok(())
}
