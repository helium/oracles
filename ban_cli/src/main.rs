use std::{collections::HashSet, path::PathBuf, str::FromStr};

use ban_cli::{tracker::TrackGood, DbArgs, RadioType};
use chrono::{Duration, Utc};
use clap::Parser;
use futures::TryStreamExt;
use helium_crypto::{Keypair, PublicKey, Sign};
use helium_proto::services::{
    poc_mobile::{
        poc_mobile_client::PocMobileClient, service_provider_boosted_rewards_banned_radio_req_v1,
        ServiceProviderBoostedRewardsBannedRadioReqV1,
    },
    Channel,
};
use helium_proto::Message;
use sqlx::{postgres::PgPoolOptions, Pool, Postgres, Row};
use tonic::{
    metadata::{Ascii, MetadataValue},
    transport::Endpoint,
};

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

impl From<&RadioKey> for service_provider_boosted_rewards_banned_radio_req_v1::KeyType {
    fn from(value: &RadioKey) -> Self {
        match value {
            RadioKey::Wifi(pk) => {
                service_provider_boosted_rewards_banned_radio_req_v1::KeyType::HotspotKey(pk.into())
            }
            RadioKey::Cbrs(cbsd_id) => {
                service_provider_boosted_rewards_banned_radio_req_v1::KeyType::CbsdId(
                    cbsd_id.clone(),
                )
            }
        }
    }
}

#[derive(Debug, Clone, clap::ValueEnum, Copy)]
enum BanType {
    BoostedHex,
    Poc,
}

impl From<BanType>
    for service_provider_boosted_rewards_banned_radio_req_v1::SpBoostedRewardsBannedRadioBanType
{
    fn from(value: BanType) -> Self {
        match value {
            BanType::BoostedHex => service_provider_boosted_rewards_banned_radio_req_v1::SpBoostedRewardsBannedRadioBanType::BoostedHex,
            BanType::Poc => service_provider_boosted_rewards_banned_radio_req_v1::SpBoostedRewardsBannedRadioBanType::Poc,
        }
    }
}

impl BanType {
    fn as_str(&self) -> &'static str {
        match self {
            BanType::BoostedHex => "boosted_hex",
            BanType::Poc => "poc",
        }
    }
}

#[derive(clap::Parser, Debug)]
enum Cli {
    File(FileArgs),
    Unban(SingleArgs),
    Ban(SingleArgs),
    TrackGood(TrackGood),
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
    #[arg(long)]
    ban_type: BanType,
}

#[derive(clap::Args, Debug)]
struct SingleArgs {
    #[command(flatten)]
    ingestor: IngestorArgs,
    #[arg(long)]
    ban_type: BanType,
    #[arg(long)]
    radio_type: RadioType,
    #[arg(long)]
    csv: PathBuf,
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
            let csv_keys = read_csv_keys(args.csv, args.radio_type)?;
            println!("Unbanning {} radios?: \n", csv_keys.len());
            print!("Continue [Y/n]: ");
            let response: String = text_io::read!();

            match response.to_lowercase().as_str() {
                "yes" | "y" => {
                    let mut mobile_ingestor = args.ingestor.to_mobile_ingestor()?;
                    unban_radios(
                        &mut mobile_ingestor,
                        csv_keys.iter().collect(),
                        args.ban_type.into(),
                    )
                    .await?;
                }
                _ => {
                    println!("Bailing");
                }
            }
        }
        Cli::Ban(args) => {
            let csv_keys = read_csv_keys(args.csv, args.radio_type)?;
            println!("Banning {} radios?: \n", csv_keys.len());
            print!("Continue [Y/n]: ");
            let response: String = text_io::read!();

            match response.to_lowercase().as_str() {
                "yes" | "y" => {
                    let mut mobile_ingestor = args.ingestor.to_mobile_ingestor()?;
                    ban_radios(
                        &mut mobile_ingestor,
                        csv_keys.iter().collect(),
                        args.ban_type.into(),
                    )
                    .await?;
                }
                _ => {
                    println!("Bailing");
                }
            }
        }
        Cli::TrackGood(tracker) => {
            tracker.run().await?;
        }
    }

    Ok(())
}

async fn ban_file(args: FileArgs) -> anyhow::Result<()> {
    let radio_type = args.radio_type;

    let pool = PgPoolOptions::new()
        .max_connections(5)
        .connect_with(args.db.connect_options()?)
        .await?;

    let csv_keys = read_csv_keys(args.csv, radio_type)?;
    let banned_radios = get_banned_radios(&pool, radio_type, args.ban_type).await?;

    let mut need_banned: Vec<_> = csv_keys.difference(&banned_radios).collect();
    let mut already_banned: Vec<_> = csv_keys.intersection(&banned_radios).collect();
    let need_unbanned: Vec<_> = banned_radios.difference(&csv_keys).collect();

    println!("Already Banned: {}", already_banned.len());
    println!("New Banned: {}", need_banned.len());
    println!("Unbanned: {}", need_unbanned.len());

    print!("Continue? [y/n]");
    let response: String = text_io::read!("{}");
    if response == "yes" || response == "y" {
        let mut mobile_ingestor = args.ingestor.to_mobile_ingestor()?;
        need_banned.append(&mut already_banned);

        if need_banned.len() > 0 {
            println!("Banning {} radios ...", need_banned.len());
            ban_radios(&mut mobile_ingestor, need_banned, args.ban_type.into()).await?;
        }

        if need_unbanned.len() > 0 {
            println!("Unbanning {} radios ...", need_unbanned.len());
            unban_radios(&mut mobile_ingestor, need_unbanned, args.ban_type.into()).await?;
        }
    }

    Ok(())
}

async fn ban_radios(
    mobile_ingestor: &mut MobileIngestor,
    radios: Vec<&RadioKey>,
    ban_type: service_provider_boosted_rewards_banned_radio_req_v1::SpBoostedRewardsBannedRadioBanType,
) -> anyhow::Result<()> {
    for radio_key in radios {
        let mut request = ServiceProviderBoostedRewardsBannedRadioReqV1 {
            pubkey: mobile_ingestor.signing_key.public_key().into(),
            reason: service_provider_boosted_rewards_banned_radio_req_v1::SpBoostedRewardsBannedRadioReason::NoNetworkCorrelation as i32,
            until: (Utc::now() + Duration::days(15)).timestamp() as u64,
            signature: vec![],
            key_type: Some(radio_key.into()),
            ban_type: ban_type as i32,
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
    radios: Vec<&RadioKey>,
    ban_type: service_provider_boosted_rewards_banned_radio_req_v1::SpBoostedRewardsBannedRadioBanType,
) -> anyhow::Result<()> {
    for radio_key in radios {
        let mut request = ServiceProviderBoostedRewardsBannedRadioReqV1 {
            pubkey: mobile_ingestor.signing_key.public_key().into(),
            reason: service_provider_boosted_rewards_banned_radio_req_v1::SpBoostedRewardsBannedRadioReason::Unbanned as i32,
            until: Utc::now().timestamp() as u64,
            signature: vec![],
            key_type: Some(radio_key.into()),
            ban_type: ban_type as i32,
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
    ban_type: BanType,
) -> anyhow::Result<HashSet<RadioKey>> {
    sqlx::query(
        r#"
            SELECT distinct radio_key
            FROM sp_boosted_rewards_bans
            WHERE radio_type = $1
                AND ban_type = $2
                AND until >= $3
                AND invalidated_at IS NULL
        "#,
    )
    .bind(radio_type)
    .bind(ban_type.as_str())
    .bind(Utc::now())
    .fetch(pool)
    .map_err(anyhow::Error::from)
    .and_then(|row| async move {
        let key = row.get::<String, &str>("radio_key");
        into_radiokey(radio_type, key)
    })
    .try_collect()
    .await
}

fn read_csv_keys(path: PathBuf, radio_type: RadioType) -> anyhow::Result<HashSet<RadioKey>> {
    let mut reader = csv::Reader::from_path(path)?;
    let mut csv_radio_keys = HashSet::new();

    for result in reader.records() {
        let record = result?;
        let radio_key = record
            .get(0)
            .map(|s| s.to_owned())
            .ok_or_else(|| anyhow::anyhow!("no key in csv"))
            .and_then(|s| into_radiokey(radio_type, s))?;

        csv_radio_keys.insert(radio_key);
    }

    Ok(csv_radio_keys)
}
