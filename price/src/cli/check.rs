use crate::hermes;
use anyhow::{anyhow, Result};
use chrono::{TimeZone, Utc};

pub async fn run(url: String) -> Result<()> {
    let client = reqwest::Client::new();
    let parsed = hermes::fetch(&client, &url).await?;
    let scaled = parsed.price.scaled_u64()?;
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
    println!("Scaled integer price (as emitted to S3): {scaled}");

    Ok(())
}
