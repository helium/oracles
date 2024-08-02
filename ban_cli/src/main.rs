use std::{collections::HashMap, path::PathBuf, str::FromStr};

use chrono::{DateTime, Duration, Utc};
use clap::Parser;
use helium_crypto::{Keypair, PublicKey, Sign};
use helium_proto::services::{
    poc_mobile::{
        poc_mobile_client::PocMobileClient, service_provider_boosted_rewards_banned_radio_req_v1,
        ServiceProviderBoostedRewardsBannedRadioReqV1,
    },
    Channel,
};
use helium_proto::Message;
use sqlx::{
    postgres::{PgConnectOptions, PgPoolOptions},
    Pool, Postgres, Row,
};
use tonic::{
    metadata::{Ascii, MetadataValue},
    transport::Endpoint,
};

#[derive(Debug, clap::Args)]
pub struct DbArgs {
    #[arg(long)]
    host: String,
    #[arg(long)]
    database: Option<String>,
    #[arg(long)]
    port: Option<u16>,
    #[arg(long)]
    username: String,
}

impl DbArgs {
    fn connect_options(&self) -> anyhow::Result<PgConnectOptions> {
        let password = rpassword::prompt_password("DB password: ")?;

        Ok(PgConnectOptions::new()
            .host(&self.host)
            .port(self.port.unwrap_or(5432))
            .database(self.database.as_deref().unwrap_or("mobile_verifier"))
            .username(&self.username)
            .password(&password))
    }
}

#[derive(Debug, clap::Args)]
pub struct IngestorArgs {
    #[arg(long)]
    url: String,
    #[arg(long)]
    signing_key: PathBuf,
}

impl IngestorArgs {
    fn to_mobile_ingestor(&self) -> anyhow::Result<MobileIngestor> {
        let token = rpassword::prompt_password("Mobile Ingestor auth token: ")?;

        let channel = Endpoint::try_from(self.url.clone())?.connect_lazy();

        let data = std::fs::read(&self.signing_key)?;
        let keypair = Keypair::try_from(&data[..])?;

        Ok(MobileIngestor {
            client: PocMobileClient::new(channel),
            signing_key: keypair,
            authorization: format!("Bearer {}", token).try_into()?,
        })
    }
}

#[derive(Debug, Clone, clap::ValueEnum, Copy, sqlx::Type)]
#[sqlx(type_name = "radio_type")]
#[sqlx(rename_all = "lowercase")]
enum RadioType {
    Wifi,
    Cbrs,
}

#[derive(Debug, PartialEq, Eq, Hash, Clone)]
enum RadioKey {
    Wifi(PublicKey),
    Cbrs(String),
}

impl From<RadioKey> for service_provider_boosted_rewards_banned_radio_req_v1::KeyType {
    fn from(value: RadioKey) -> Self {
        match value {
            RadioKey::Wifi(pk) => {
                service_provider_boosted_rewards_banned_radio_req_v1::KeyType::HotspotKey(pk.into())
            }
            RadioKey::Cbrs(cbsd_id) => {
                service_provider_boosted_rewards_banned_radio_req_v1::KeyType::CbsdId(cbsd_id)
            }
        }
    }
}

#[derive(clap::Parser, Debug)]
enum Cli {
    File(FileArgs),
    Unban(SingleArgs),
    Ban(SingleArgs),
}

#[derive(clap::Args, Debug)]
struct FileArgs {
    #[arg(long)]
    csv: PathBuf,
    #[command(flatten)]
    db: DbArgs,
    #[command(flatten)]
    ingestor: IngestorArgs,
    #[arg(long)]
    radio_type: RadioType,
}

#[derive(clap::Args, Debug)]
struct SingleArgs {
    #[command(flatten)]
    ingestor: IngestorArgs,
    #[arg(long)]
    radio_type: RadioType,
    #[arg(long)]
    radio_key: String,
}

#[derive(Debug)]
struct MobileIngestor {
    client: PocMobileClient<Channel>,
    signing_key: Keypair,
    authorization: MetadataValue<Ascii>,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();
    match cli {
        Cli::File(args) => ban_file(args).await?,
        Cli::Unban(args) => {
            let radio_key = into_radiokey(args.radio_type, args.radio_key)?;
            println!("Unbanning: {radio_key:?}\n");
            print!("Continue [Y/n]: ");
            let response: String = text_io::read!();

            match response.to_lowercase().as_str() {
                "yes" | "y" => {
                    let mut mobile_ingestor = args.ingestor.to_mobile_ingestor()?;
                    unban_radios(&mut mobile_ingestor, vec![radio_key]).await?;
                }
                _ => {
                    println!("Bailing");
                }
            }
        }
        Cli::Ban(args) => {
            let radio_key = into_radiokey(args.radio_type, args.radio_key)?;
            println!("Banning: {radio_key:?}\n");
            print!("Continue [Y/n]: ");
            let response: String = text_io::read!();

            match response.to_lowercase().as_str() {
                "yes" | "y" => {
                    let mut mobile_ingestor = args.ingestor.to_mobile_ingestor()?;
                    ban_radios(&mut mobile_ingestor, vec![radio_key]).await?;
                }
                _ => {
                    println!("Bailing");
                }
            }
        }
    }

    Ok(())
}

async fn ban_file(args: FileArgs) -> anyhow::Result<()> {
    //let args = Args::parse();
    let radio_type = args.radio_type;

    let pool = PgPoolOptions::new()
        .max_connections(5)
        .connect_with(args.db.connect_options()?)
        .await?;

    let csv_keys = read_csv_keys(args.csv, radio_type)?;
    let banned_radios = get_banned_radios(&pool, radio_type).await?;

    let keys_to_ban: Vec<RadioKey> = csv_keys
        .clone()
        .into_iter()
        .filter(|rk| match banned_radios.get(rk) {
            Some(until) => until.to_owned() < Utc::now() + Duration::days(10),
            None => true,
        })
        .collect();

    if keys_to_ban.len() == 0 {
        println!("no radios to ban");
    } else {
        println!("Keys to ban: {}", keys_to_ban.len());
        print!("Ban radios?: ");
        let response: String = text_io::read!("{}");

        if response == "yes" || response == "y" {
            println!("\nsending radios to ban to mobile ingestor");
            let mut mobile_ingestor = args.ingestor.to_mobile_ingestor()?;
            ban_radios(&mut mobile_ingestor, keys_to_ban).await?;
            println!("\n all radios to ban sent to ingestor");
        }
    }

    let keys_to_unban: Vec<RadioKey> = banned_radios
        .into_keys()
        .filter(|rk| !csv_keys.contains(rk))
        .collect();

    if keys_to_unban.len() == 0 {
        println!("no radios to unban");
    } else {
        println!("Keys to unban: {} ", keys_to_unban.len());
        print!("Unban radios?: ");
        let response: String = text_io::read!("{}");

        if response == "yes" || response == "y" {
            println!("\nsending radios to unban to mobile ingestor");
            let mut mobile_ingestor = args.ingestor.to_mobile_ingestor()?;
            unban_radios(&mut mobile_ingestor, keys_to_unban).await?;
            println!("\n all radios to unban sent to ingestor");
        }
    }

    Ok(())
}

async fn ban_radios(
    mobile_ingestor: &mut MobileIngestor,
    radios: Vec<RadioKey>,
) -> anyhow::Result<()> {
    for radio_key in radios {
        let mut request = ServiceProviderBoostedRewardsBannedRadioReqV1 {
            pubkey: mobile_ingestor.signing_key.public_key().into(),
            reason: service_provider_boosted_rewards_banned_radio_req_v1::SpBoostedRewardsBannedRadioReason::NoNetworkCorrelation as i32,
            until: (Utc::now() + Duration::days(15)).timestamp() as u64,
            signature: vec![],
            key_type: Some(radio_key.into()),
        };

        request.signature = mobile_ingestor.signing_key.sign(&request.encode_to_vec())?;

        let mut request = tonic::Request::new(request);
        let metadata = request.metadata_mut();

        metadata.insert("authorization", mobile_ingestor.authorization.clone());

        mobile_ingestor
            .client
            .submit_sp_boosted_rewards_banned_radio(request)
            .await?;
        print!(".");
    }
    Ok(())
}

async fn unban_radios(
    mobile_ingestor: &mut MobileIngestor,
    radios: Vec<RadioKey>,
) -> anyhow::Result<()> {
    for radio_key in radios {
        let mut request = ServiceProviderBoostedRewardsBannedRadioReqV1 {
            pubkey: mobile_ingestor.signing_key.public_key().into(),
            reason: service_provider_boosted_rewards_banned_radio_req_v1::SpBoostedRewardsBannedRadioReason::Unbanned as i32,
            until: Utc::now().timestamp() as u64,
            signature: vec![],
            key_type: Some(radio_key.into()),
        };

        request.signature = mobile_ingestor.signing_key.sign(&request.encode_to_vec())?;

        let mut request = tonic::Request::new(request);
        let metadata = request.metadata_mut();

        metadata.insert("authorization", mobile_ingestor.authorization.clone());

        mobile_ingestor
            .client
            .submit_sp_boosted_rewards_banned_radio(request)
            .await?;
        print!(".");
    }
    Ok(())
}

fn into_radiokey(radio_type: RadioType, s: String) -> anyhow::Result<RadioKey> {
    match radio_type {
        RadioType::Wifi => PublicKey::from_str(&s)
            .map(|pk| RadioKey::Wifi(pk))
            .map_err(anyhow::Error::from),
        RadioType::Cbrs => Ok(RadioKey::Cbrs(s)),
    }
}

async fn get_banned_radios(
    pool: &Pool<Postgres>,
    radio_type: RadioType,
) -> anyhow::Result<HashMap<RadioKey, DateTime<Utc>>> {
    let result = sqlx::query(
        r#"
            SELECT radio_key, until
            FROM sp_boosted_rewards_bans
            WHERE radio_type = $1
                AND until >= $2
                AND invalidated_at IS NULL
        "#,
    )
    .bind(radio_type)
    .bind(Utc::now())
    .fetch_all(pool)
    .await?;

    result.into_iter().try_fold(HashMap::new(), |mut acc, row| {
        let key = row.get::<String, &str>("radio_key");
        let until = row.get::<DateTime<Utc>, &str>("until");
        acc.insert(into_radiokey(radio_type, key)?, until);
        Ok(acc)
    })
}

fn read_csv_keys(path: PathBuf, radio_type: RadioType) -> anyhow::Result<Vec<RadioKey>> {
    let mut reader = csv::Reader::from_path(path)?;
    let mut csv_radio_keys = Vec::new();

    for result in reader.records() {
        let record = result?;
        let radio_key = record
            .get(0)
            .map(|s| s.to_owned())
            .ok_or_else(|| anyhow::anyhow!("no key in csv"))
            .and_then(|s| into_radiokey(radio_type, s))?;

        csv_radio_keys.push(radio_key);
    }

    Ok(csv_radio_keys)
}

// async fn is_already_banned(pool: &Pool<Postgres>, public_key: &PublicKey) -> anyhow::Result<bool> {
//     sqlx::query_scalar::<_, i64>(
//         r#"
//             SELECT count(*)
//             FROM sp_boosted_rewards_bans
//             WHERE radio_type = 'wifi'
//                 AND radio_key = $1
//                 AND invalidated_at IS NULL
//                 AND until > $2
//         "#,
//     )
//     .bind(public_key.to_string())
//     .bind(Utc::now() + Duration::days(7))
//     .fetch_one(pool)
//     .await
//     .map(|count| count > 0)
//     .map_err(anyhow::Error::from)
// }
