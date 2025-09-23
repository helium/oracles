use anyhow::Result;
use chrono::{DateTime, TimeZone, Utc};
use solana::{
    helium_lib::{
        anchor_lang::AccountDeserialize, programs::price_oracle::accounts::PriceOracleV0,
    },
    RpcClient, SolPubkey,
};
use std::str::FromStr;

const SECONDS_PER_DAY: i64 = 24 * 60 * 60;

#[derive(Debug)]
pub enum Mode {
    Iot,
    Mobile,
}

pub async fn run(mode: Mode) -> Result<()> {
    let client = RpcClient::new("https://api.mainnet-beta.solana.com".to_string());

    let pubkey = match mode {
        Mode::Iot => SolPubkey::from_str("iortGU2NMgWc256XDBz2mQnmjPfKUMezJ4BWfayEZY3")?,
        Mode::Mobile => SolPubkey::from_str("moraMdsjyPFz8Lp1RJGoW4bQriSF5mHE7Evxt7hytSF")?,
    };

    let price_oracle_v0_data = client.get_account_data(&pubkey).await?;
    let mut price_oracle_v0_data = price_oracle_v0_data.as_ref();
    let price_oracle_v0 = PriceOracleV0::try_deserialize(&mut price_oracle_v0_data)?;

    let curr_ts = Utc::now().timestamp();
    let total_length = price_oracle_v0.oracles.len();
    let required_valid = total_length / 2 + 1;

    let prices: Vec<(DateTime<Utc>, u64)> = price_oracle_v0
        .oracles
        .into_iter()
        .filter(|oracle| {
            oracle.last_submitted_price.is_some()
                && oracle.last_submitted_timestamp.is_some()
                && curr_ts - oracle.last_submitted_timestamp.unwrap() <= SECONDS_PER_DAY
        })
        .map(|oracle| {
            (
                Utc.timestamp_opt(oracle.last_submitted_timestamp.unwrap(), 0)
                    .single()
                    .unwrap(),
                oracle.last_submitted_price.unwrap(),
            )
        })
        .collect();

    println!("Total number of prices: {total_length}");
    println!("Number of valid prices: {}", prices.len());

    if prices.len() >= required_valid {
        println!("\nPrice is currently VALID");
    } else {
        println!("\nPrice is currently INVALID");
    }

    println!("\nValid prices");
    println!("----------------------------");
    for (ts, price) in prices {
        println!("{ts}\t {price}");
    }

    Ok(())
}
