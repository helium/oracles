//! Download a day's reward manifest(s) and all of their written reward-share
//! files, so a past day's rewards can be inspected locally.
//!
//! Lists `network_reward_manifest_v1` files in the given window from the
//! rewarder's **output** bucket, decodes each to read its epoch + `written_files`,
//! then downloads the raw manifest `.gz` plus every share file it references into
//! `--out`. Inspect the downloaded shares with the file-store-oracles dump tools
//! (e.g. `dump_mobile_rewards`).
//!
//! A manifest is written at reward time (epoch end + the reward offset), so give
//! a window that extends a bit past the day's end. Manifests are ~one per epoch,
//! so a 1–2 day window yields just a few; each manifest's epoch is printed so you
//! can tell them apart.
//!
//! Usage:
//!   cargo run -p mobile-verifier --example download_reward_manifest -- \
//!     --config <settings.toml> \
//!     --after 2026-06-10T00:00:00Z --before 2026-06-12T00:00:00Z \
//!     --out ./rewards-2026-06-10
//!
//! Needs `settings.buckets.output` pointed at the bucket and the matching S3 env
//! (AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY / AWS_REGION, + endpoint if set in
//! settings).

use std::path::{Path, PathBuf};

use chrono::{DateTime, Utc};
use clap::Parser;
use file_store::{traits::MsgDecode, BucketClient};
use file_store_oracles::network_common::reward_manifest::RewardManifest;
use futures::StreamExt;
use mobile_verifier::Settings;

const MANIFEST_PREFIX: &str = "network_reward_manifest_v1";

#[derive(Parser)]
struct Args {
    #[clap(short = 'c', long)]
    config: PathBuf,
    /// List manifests written after this time (RFC3339).
    #[clap(long)]
    after: String,
    /// List manifests written before this time (RFC3339).
    #[clap(long)]
    before: String,
    /// Local directory to download into.
    #[clap(long, default_value = "reward-manifest-dump")]
    out: PathBuf,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    let after = DateTime::parse_from_rfc3339(&args.after)?.with_timezone(&Utc);
    let before = DateTime::parse_from_rfc3339(&args.before)?.with_timezone(&Utc);

    let settings = Settings::new(Some(&args.config))?;
    let bucket = settings.buckets.output.connect().await;
    tokio::fs::create_dir_all(&args.out).await?;

    let manifests = bucket
        .list_all_files(MANIFEST_PREFIX, after, before)
        .await?;
    if manifests.is_empty() {
        println!("no manifests found under '{MANIFEST_PREFIX}' in {after} .. {before}");
        return Ok(());
    }
    println!("found {} manifest file(s) in window\n", manifests.len());

    for info in manifests {
        // Decode the manifest (one record per file) to read its written_files.
        let mut stream = bucket.stream_single_file(info.clone()).await?;
        let Some(bytes) = stream.next().await else {
            println!("{}: empty, skipping", info.key);
            continue;
        };
        let manifest = RewardManifest::decode(bytes?)?;

        println!(
            "manifest {}\n  epoch {}  period {} .. {}  price {}  written_files {}",
            info.key,
            manifest.epoch,
            manifest.start_timestamp,
            manifest.end_timestamp,
            manifest.price,
            manifest.written_files.len(),
        );

        // Raw manifest + every reward-share file it references.
        download(&bucket, &info.key, &args.out).await?;
        for file_name in &manifest.written_files {
            download(&bucket, file_name, &args.out).await?;
        }
        println!(
            "  downloaded manifest + {} share file(s)\n",
            manifest.written_files.len()
        );
    }

    println!("done -> {}", args.out.display());
    Ok(())
}

async fn download(bucket: &BucketClient, key: &str, out: &Path) -> anyhow::Result<()> {
    let data = bucket
        .get_raw_file(key)
        .await?
        .collect()
        .await?
        .into_bytes();
    let path = out.join(key);
    if let Some(parent) = path.parent() {
        tokio::fs::create_dir_all(parent).await?;
    }
    tokio::fs::write(&path, &data).await?;
    Ok(())
}
