use std::path::PathBuf;

use chrono::Utc;
use clap::Parser;
use csv::ReaderBuilder;
use sqlx::{postgres::PgPoolOptions, Postgres, Transaction};

#[derive(clap::Parser, Debug, Clone)]
struct Args {
    #[arg(long)]
    db_url: String,
    #[arg(long)]
    csv: PathBuf,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();

    let db = PgPoolOptions::new()
        .max_connections(5)
        .connect(&args.db_url)
        .await?;

    let now = Utc::now();

    let mut tx = db.begin().await?;
    let all_good = get_all_good(&mut tx).await?;

    let rdr = ReaderBuilder::new()
        .has_headers(false)
        .from_path(args.csv)?;

    let mut processed = 0;

    for result in rdr.into_records() {
        processed += 1;
        let row = result?;

        let radioserial = row.get(0).unwrap();
        let already_marked_good = all_good.iter().any(|x| x == radioserial);

        if !already_marked_good {
            sqlx::query("INSERT INTO hip131_good(serialnumber, marked_good_ts) VALUES($1, $2)")
                .bind(radioserial)
                .bind(now)
                .execute(&mut *tx)
                .await?;
        }

        println!("{}", processed);
    }

    tx.commit().await?;

    Ok(())
}

async fn get_all_good(tx: &mut Transaction<'_, Postgres>) -> anyhow::Result<Vec<String>> {
    sqlx::query_scalar("SELECT serialnumber FROM hip131_good")
        .fetch_all(tx)
        .await
        .map_err(anyhow::Error::from)
}
