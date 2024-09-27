use std::path::PathBuf;

use chrono::Utc;
use sqlx::{postgres::PgPoolOptions, Postgres, Transaction};

use crate::{DbArgs, RadioType};

#[derive(clap::Args, Debug)]
pub struct TrackGood {
    #[arg(long)]
    csv: PathBuf,
    #[command(flatten)]
    db: DbArgs,
    #[arg(long)]
    radio_type: RadioType,
}

impl TrackGood {
    pub async fn run(self) -> anyhow::Result<()> {
        let pool = PgPoolOptions::new()
            .max_connections(5)
            .connect_with(self.db.connect_options()?)
            .await?;

        let mut tx = pool.begin().await?;

        match self.radio_type {
            RadioType::Wifi => {
                clean_reasserted(&mut tx).await?;
                process_wifi_csv(self.csv, &mut tx).await?;
            }
            RadioType::Cbrs => {
                process_cbrs_csv(self.csv, &mut tx).await?;
            }
        }

        tx.commit().await?;

        Ok(())
    }
}

async fn process_cbrs_csv(csv: PathBuf, tx: &mut Transaction<'_, Postgres>) -> anyhow::Result<()> {
    let all_good = get_all_good(tx).await?;
    let mut rdr = csv::Reader::from_path(csv)?;
    let mut wtr = csv::Writer::from_writer(std::io::stdout());

    let now = Utc::now();

    let headers = rdr.headers()?.into_iter().collect::<Vec<&str>>();
    wtr.write_record(headers)?;

    let mut idx = 0;

    for result in rdr.into_records() {
        let row = result?;
        idx += 1;

        let cbsd_id = row.get(0).unwrap();
        let radio_serial = row.get(1).unwrap();
        let analysis = row.get(2).unwrap();
        let difference_m = row.get(3).unwrap();

        let already_marked_good = all_good.iter().any(|x| x == radio_serial);

        if analysis == "good" && !already_marked_good {
            sqlx::query("INSERT INTO hip131_good(serialnumber, marked_good_ts) VALUES($1, $2)")
                .bind(radio_serial)
                .bind(now)
                .execute(&mut *tx)
                .await?;
        }

        if analysis != "good" && !already_marked_good {
            wtr.write_record(vec![cbsd_id, radio_serial, analysis, difference_m])?;
        }

        eprintln!("{}", idx);
    }

    wtr.flush()?;

    Ok(())
}

async fn process_wifi_csv(csv: PathBuf, tx: &mut Transaction<'_, Postgres>) -> anyhow::Result<()> {
    let all_good = get_all_good(tx).await?;
    let mut rdr = csv::Reader::from_path(csv)?;
    let mut wtr = csv::Writer::from_writer(std::io::stdout());

    let now = Utc::now();

    let headers = rdr.headers()?.into_iter().collect::<Vec<&str>>();

    wtr.write_record(vec![headers[0], headers[2], headers[3]])?;

    let mut idx = 0;

    for result in rdr.into_records() {
        let row = result?;
        idx += 1;

        let pubkey = row.get(0).unwrap();
        let serialnum = row.get(1).unwrap();
        let analysis = row.get(2).unwrap();
        let difference_m = row.get(3).unwrap();
        let _radius_m = row.get(4).unwrap();

        let already_marked_good = all_good.iter().any(|x| x == serialnum);

        if analysis == "good" && !already_marked_good {
            sqlx::query("INSERT INTO hip131_good(serialnumber, marked_good_ts) VALUES($1, $2)")
                .bind(serialnum)
                .bind(now)
                .execute(&mut *tx)
                .await?;
        }

        if analysis != "good" && !already_marked_good {
            wtr.write_record(vec![pubkey, analysis, difference_m])?;
        }

        eprintln!("{}", idx);
    }

    wtr.flush()?;

    Ok(())
}

async fn get_all_good(tx: &mut Transaction<'_, Postgres>) -> anyhow::Result<Vec<String>> {
    sqlx::query_scalar("SELECT serialnumber FROM hip131_good")
        .fetch_all(tx)
        .await
        .map_err(anyhow::Error::from)
}

async fn clean_reasserted(tx: &mut Transaction<'_, Postgres>) -> anyhow::Result<()> {
    sqlx::query(
        r#"
            WITH latest_assertions AS (
            	SELECT DISTINCT ON (serialnumber) *
            	FROM hotspot_assertions ha 
            	ORDER BY serialnumber, time desc
            )
            DELETE FROM hip131_good hg WHERE serialnumber IN (
            	SELECT hg.serialnumber
            	FROM hip131_good hg 
            		INNER JOIN latest_assertions la ON hg.serialnumber = la.serialnumber
            	WHERE la.time > hg.marked_good_ts
            )
    "#,
    )
    .execute(tx)
    .await?;

    Ok(())
}
