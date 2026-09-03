//! Submit HIP-150 data transfer multiplier tickets to ingest.
//!
//! A ticket grants one hotspot a multiplier on the data credits its rewardable
//! bytes convert to. It must be signed by a key on ingest's
//! `data_transfer_multiplier` allow-list.
//!
//! Submitting is not reversible. A ticket that verifies changes what a payer
//! burns and what a deployer earns from the next burn on, and the only way back
//! is another ticket. So nothing is sent without `--commit`.

use std::path::PathBuf;

use anyhow::Context;
use chrono::Utc;
use clap::{Parser, Subcommand};
use file_store_oracles::mobile::data_transfer_multiplier::DataTransferMultiplier;
use helium_crypto::{Keypair, PublicKeyBinary, Sign};
use helium_proto::services::poc_mobile::{
    Client as PocMobileClient, DataTransferMultiplierTicketReqV1,
};
use prost::Message;
use rust_decimal::Decimal;
use serde_json::json;

#[derive(Debug, Parser)]
#[command(name = "multiplier-ticket-cli", version, about, long_about = None)]
struct Cli {
    /// Ingest gRPC endpoint, e.g. http://localhost:9080
    #[arg(global = true, long, env = "INGEST_URL")]
    ingest_url: Option<String>,

    /// Keypair to sign with. Must be on ingest's multiplier allow-list.
    #[arg(
        global = true,
        long,
        env = "MULTIPLIER_TICKET_KEYPAIR",
        default_value = "./keypair.bin"
    )]
    keypair: PathBuf,

    /// Actually send the ticket. Without it the ticket is printed and nothing
    /// else — a ticket cannot be withdrawn, so the default is the harmless half.
    #[arg(global = true, long)]
    commit: bool,

    #[command(subcommand)]
    command: Commands,
}

#[derive(Debug, Subcommand)]
enum Commands {
    /// Grant a hotspot a multiplier. Submitting 1 revokes an existing grant.
    Submit {
        /// Hotspot to grant it to, base58.
        #[arg(long)]
        hotspot: PublicKeyBinary,

        /// HIP-150 accepts 1 to 5 inclusive, up to 6 decimal places.
        #[arg(long)]
        multiplier: Decimal,

        /// Free text recorded with the ticket. Use it to say why.
        #[arg(long, default_value = "")]
        message: String,
    },
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();
    let Commands::Submit {
        hotspot,
        multiplier,
        message,
    } = &cli.command;

    // Checked here as well as in the packet verifier. The verifier is what
    // decides, but a ticket refused there is refused *on the record* — it still
    // gets a verified report and a history row. Catching a typo locally keeps
    // the audit log a record of real decisions.
    let multiplier = DataTransferMultiplier::new(*multiplier)
        .with_context(|| format!("{multiplier} is not a multiplier the oracles will accept"))?;

    let keypair = load_keypair(&cli.keypair)?;
    let signer = PublicKeyBinary::from(keypair.public_key().to_owned());

    // Truncated to milliseconds because that is what the ticket carries on the
    // wire, so the timestamp printed here is the one that gets stored.
    let timestamp = chrono::DateTime::from_timestamp_millis(Utc::now().timestamp_millis())
        .context("clock is outside the representable range")?;

    let mut out = json!({
        "hotspot_pubkey": hotspot.to_string(),
        "multiplier": multiplier.to_string(),
        "signed_timestamp": timestamp.to_rfc3339_opts(chrono::SecondsFormat::Millis, true),
        "signer": signer.to_string(),
        "message": message,
        "committed": false,
    });

    if cli.commit {
        let url = cli
            .ingest_url
            .as_deref()
            .context("--ingest-url (or INGEST_URL) is required to send")?;

        let resp = send(&keypair, hotspot, multiplier, timestamp, message, url).await?;

        out["committed"] = json!(true);
        out["received_timestamp_ms"] = json!(resp);
    }

    println!("{}", serde_json::to_string_pretty(&out)?);
    if !cli.commit {
        eprintln!("not sent. re-run with --commit to issue this ticket");
    }
    Ok(())
}

/// Sign the ticket and hand it to ingest, returning ingest's received timestamp.
///
/// The signature covers the encoded request with the signature field still
/// empty, which is the form ingest reconstructs to verify. Get that wrong and
/// ingest rejects it with `invalid signature` and writes nothing at all — no
/// report, no history row — so it looks like nothing happened.
async fn send(
    keypair: &Keypair,
    hotspot: &PublicKeyBinary,
    multiplier: DataTransferMultiplier,
    timestamp: chrono::DateTime<Utc>,
    message: &str,
    url: &str,
) -> anyhow::Result<u64> {
    let req = signed_ticket(keypair, hotspot, multiplier, timestamp, message)?;

    let mut client = PocMobileClient::connect(url.to_string())
        .await
        .with_context(|| format!("connecting to ingest at {url}"))?;

    let resp = client
        .submit_data_transfer_multiplier_ticket(req)
        .await
        .map_err(|status| anyhow::anyhow!("ingest refused the ticket: {status}"))?
        .into_inner();

    Ok(resp.timestamp_ms)
}

fn signed_ticket(
    keypair: &Keypair,
    hotspot: &PublicKeyBinary,
    multiplier: DataTransferMultiplier,
    timestamp: chrono::DateTime<Utc>,
    message: &str,
) -> anyhow::Result<DataTransferMultiplierTicketReqV1> {
    let mut req = DataTransferMultiplierTicketReqV1 {
        hotspot_pubkey: hotspot.clone().into(),
        multiplier: Some(helium_proto::Decimal {
            value: multiplier.to_string(),
        }),
        timestamp_ms: timestamp.timestamp_millis() as u64,
        message: message.to_string(),
        signer_pubkey: PublicKeyBinary::from(keypair.public_key().to_owned()).into(),
        signature: vec![],
    };
    req.signature = keypair
        .sign(&req.encode_to_vec())
        .context("signing the ticket")?;
    Ok(req)
}

fn load_keypair(path: &PathBuf) -> anyhow::Result<Keypair> {
    let data =
        std::fs::read(path).with_context(|| format!("reading keypair from {}", path.display()))?;
    Keypair::try_from(&data[..]).context("parsing keypair")
}

#[cfg(test)]
mod tests {
    use super::*;
    use helium_proto_crypto::MsgVerify;
    use rust_decimal::dec;

    /// The signature has to verify the way ingest verifies it. A mismatch is
    /// rejected at the gRPC boundary with no record of the attempt, so it would
    /// look like nothing happened at all.
    #[test]
    fn a_signed_ticket_verifies_the_way_ingest_checks_it() {
        let keypair = Keypair::generate(
            helium_crypto::KeyTag {
                network: helium_crypto::Network::MainNet,
                key_type: helium_crypto::KeyType::Ed25519,
            },
            &mut rand::rngs::OsRng,
        );

        let req = signed_ticket(
            &keypair,
            &PublicKeyBinary::from(vec![1]),
            DataTransferMultiplier::new(dec!(1.5)).unwrap(),
            Utc::now(),
            "why",
        )
        .unwrap();

        req.verify(keypair.public_key())
            .expect("ingest must accept this signature");
    }
}
