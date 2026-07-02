//! Run the NEW data-transfer allocation for a past day and compare it to that
//! day's ACTUAL rewards, read from locally-downloaded reward-manifest files (see
//! the `download_reward_manifest` example).
//!
//! For each manifest in `--manifest-dir`:
//!   * decode it + its `written_files` (the `MobileRewardShare` files) to recover
//!     the day's actual `GatewayReward` (DC) amounts, plus the (usually tiny)
//!     `RadioRewardV2` and `UnallocatedReward(Poc)` amounts;
//!   * determine the data-transfer pool: by default resolve the epoch's on-chain
//!     `epoch_emissions` from mobile-config (`config_client`) and use the exact
//!     `0.7 × emissions`; fall back to the reconstructed `DC + PoC + unallocated(Poc)`
//!     (a floored integer) when emissions can't be resolved. `--emissions` overrides;
//!   * load that day's rewardable data from the Trino/iceberg `burned_sessions`;
//!   * run `data_transfer::allocate(pool, ..)` and diff the new per-hotspot DC
//!     rewards against the historical ones.
//!
//! With the exact pool the integer-pool-reconstruction artifact is gone, so any
//! remaining per-hotspot diff isolates Trino-vs-Postgres input divergence: the
//! replay feeds the new algorithm the *Trino* aggregate, whereas the historical run
//! rewarded from *Postgres*. With the reconstructed (floored) pool, expect a few
//! boundary hotspots 1 bone low. See mobile_verifier/docs/remove-poc-from-rewarder.md.
//!
//! Usage:
//!   cargo run -p mobile-verifier --example compare_new_dc_vs_manifest -- \
//!     --config <settings.toml> --manifest-dir ./rewards-2026-06-10 \
//!     [--emissions <epoch_emissions_bones>]   # else resolved from config_client

use std::{
    collections::{HashMap, HashSet},
    io::Read,
    path::{Path, PathBuf},
};

use clap::Parser;
use file_store::traits::MsgDecode;
use file_store_oracles::network_common::reward_manifest::RewardManifest;
use helium_crypto::PublicKeyBinary;
use helium_proto::services::poc_mobile::{
    mobile_reward_share::Reward, MobileRewardShare, UnallocatedRewardType,
};
use mobile_config::client::sub_dao_client::{SubDaoClient, SubDaoEpochRewardInfoResolver};
use mobile_verifier::{
    iceberg::burned_session,
    resolve_subdao_pubkey,
    reward_shares::data_transfer::{self, GatewayDataTransfer},
    Settings,
};
use prost::Message;
use rust_decimal::Decimal;
use rust_decimal_macros::dec;

const MANIFEST_PREFIX: &str = "network_reward_manifest_v1";

#[derive(Parser)]
struct Args {
    #[clap(short = 'c', long)]
    config: PathBuf,
    /// Directory of downloaded manifest + share files (from `download_reward_manifest`).
    #[clap(long)]
    manifest_dir: PathBuf,
    /// Override epoch emissions (bones); the exact pool is `0.7 × this`. If omitted,
    /// emissions are resolved per epoch from `config_client` (mobile-config), falling
    /// back to the reconstructed integer pool when the lookup fails.
    #[clap(long)]
    emissions: Option<u64>,
}

/// Read a local file-store `.gz` file and return each framed message's bytes.
/// Format: gzip + length-delimited frames (u32 big-endian length prefix) — the
/// same format `file_store::stream_source` reads.
fn decode_frames(path: &Path) -> anyhow::Result<Vec<Vec<u8>>> {
    let raw = std::fs::read(path)?;
    let mut buf = Vec::new();
    flate2::read::GzDecoder::new(&raw[..]).read_to_end(&mut buf)?;

    let mut frames = Vec::new();
    let mut pos = 0;
    while pos + 4 <= buf.len() {
        let len = u32::from_be_bytes(buf[pos..pos + 4].try_into().unwrap()) as usize;
        pos += 4;
        anyhow::ensure!(
            pos + len <= buf.len(),
            "truncated frame in {}",
            path.display()
        );
        frames.push(buf[pos..pos + len].to_vec());
        pos += len;
    }
    Ok(frames)
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    let settings = Settings::new(Some(&args.config))?;
    let trino = settings
        .trino
        .as_ref()
        .map(trino_client::Client::from_settings)
        .transpose()?
        .ok_or_else(|| anyhow::anyhow!("settings.trino must be configured"))?;

    // Resolver for the exact pool (0.7 × on-chain epoch_emissions). Skipped when
    // --emissions is supplied; otherwise built once and queried per epoch below.
    let sub_dao = resolve_subdao_pubkey().to_string();
    let sub_dao_client = match args.emissions {
        Some(_) => None,
        None => Some(SubDaoClient::from_settings(&settings.config_client)?),
    };

    let mut manifest_paths = Vec::new();
    for entry in std::fs::read_dir(&args.manifest_dir)? {
        let path = entry?.path();
        let is_manifest = path
            .file_name()
            .and_then(|n| n.to_str())
            .map(|n| n.starts_with(MANIFEST_PREFIX))
            .unwrap_or(false);
        if is_manifest {
            manifest_paths.push(path);
        }
    }
    anyhow::ensure!(
        !manifest_paths.is_empty(),
        "no '{MANIFEST_PREFIX}*' files in {}",
        args.manifest_dir.display()
    );

    for manifest_path in manifest_paths {
        let frames = decode_frames(&manifest_path)?;
        let Some(payload) = frames.first() else {
            continue;
        };
        let manifest = RewardManifest::decode(&payload[..])?;
        let epoch = manifest.start_timestamp..manifest.end_timestamp;
        println!(
            "\n=== {} | epoch {} | {} .. {} ===",
            manifest_path.file_name().unwrap().to_string_lossy(),
            manifest.epoch,
            manifest.start_timestamp,
            manifest.end_timestamp,
        );

        // ---- historical rewards, from the manifest's share files ----
        let mut hist_dc: HashMap<Vec<u8>, u64> = HashMap::new();
        let mut hist_poc_total = 0u64;
        let mut hist_poc_count = 0u64;
        let mut hist_unalloc_poc = 0u64;
        for file_name in &manifest.written_files {
            let share_path = args.manifest_dir.join(file_name);
            if !share_path.exists() {
                println!("  WARN missing share file {file_name}; skipping");
                continue;
            }
            for payload in decode_frames(&share_path)? {
                match MobileRewardShare::decode(&payload[..])?.reward {
                    Some(Reward::GatewayReward(g)) => {
                        *hist_dc.entry(g.hotspot_key).or_default() += g.dc_transfer_reward;
                    }
                    Some(Reward::RadioRewardV2(r)) => {
                        hist_poc_total += r.base_poc_reward + r.boosted_poc_reward;
                        hist_poc_count += 1;
                    }
                    Some(Reward::UnallocatedReward(u))
                        if u.reward_type == UnallocatedRewardType::Poc as i32 =>
                    {
                        hist_unalloc_poc += u.amount;
                    }
                    _ => {}
                }
            }
        }
        let hist_dc_total: u64 = hist_dc.values().sum();
        // The DC+PoC pool the old run distributed (= the floored 70% allocation).
        let pool_bones = hist_dc_total + hist_poc_total + hist_unalloc_poc;

        // Prefer the EXACT pool (0.7 × on-chain epoch_emissions) so we don't lose the
        // sub-bone fraction that flooring the reconstructed pool drops — that fraction
        // is what makes boundary hotspots come up 1 short. Fall back to the
        // reconstructed integer pool if emissions can't be obtained.
        let (pool, pool_src) = match args.emissions {
            Some(em) => (
                // Reconstruct the historical (pre-HIP-149) 70% pool; this tool
                // compares the new DC allocator against old 70%-era manifests.
                Decimal::from(em) * dec!(0.7),
                format!("0.7 × emissions {em} (--emissions)"),
            ),
            None => match sub_dao_client
                .as_ref()
                .unwrap()
                .resolve_info(&sub_dao, manifest.epoch)
                .await
            {
                Ok(Some(info)) => (
                    info.epoch_emissions * dec!(0.7),
                    format!("0.7 × emissions {} (mobile-config)", info.epoch_emissions),
                ),
                Ok(None) => {
                    println!("  WARN epoch {} not found in mobile-config", manifest.epoch);
                    (
                        Decimal::from(pool_bones),
                        "reconstructed (epoch not found)".to_string(),
                    )
                }
                Err(e) => {
                    println!("  WARN resolve epoch {} failed: {e}", manifest.epoch);
                    (
                        Decimal::from(pool_bones),
                        "reconstructed (resolve failed)".to_string(),
                    )
                }
            },
        };

        // ---- new allocation over the day's Trino input ----
        let rewardable =
            burned_session::aggregate_hotspot_data_sessions_to_dc(&trino, &epoch).await?;
        let alloc = data_transfer::allocate(
            pool,
            rewardable.iter().map(|(k, r)| GatewayDataTransfer {
                hotspot_key: k.clone(),
                rewardable_dc: r.rewardable_dc,
                rewardable_bytes: r.rewardable_bytes,
            }),
        );
        let new_dc: HashMap<Vec<u8>, u64> = alloc
            .rewards
            .iter()
            .filter(|r| r.reward != 0)
            .map(|r| (r.hotspot_key.clone().into(), r.reward))
            .collect();
        let new_dc_total: u64 = new_dc.values().sum();

        // ---- diff (new - historical), per hotspot ----
        let mut keys: HashSet<&Vec<u8>> = hist_dc.keys().collect();
        keys.extend(new_dc.keys());
        let mut diffs: Vec<(Vec<u8>, i128)> = keys
            .into_iter()
            .filter_map(|k| {
                let h = hist_dc.get(k).copied().unwrap_or(0) as i128;
                let n = new_dc.get(k).copied().unwrap_or(0) as i128;
                (h != n).then(|| (k.clone(), n - h))
            })
            .collect();
        diffs.sort_by_key(|(_, d)| std::cmp::Reverse(d.abs()));
        let sum_abs: i128 = diffs.iter().map(|(_, d)| d.abs()).sum();
        let max_abs = diffs.first().map(|(_, d)| d.abs()).unwrap_or(0);

        println!(
            "  historical : {} DC hotspots, total {hist_dc_total}; PoC records {hist_poc_count} (total {hist_poc_total}); unallocated(Poc) {hist_unalloc_poc}",
            hist_dc.len()
        );
        let pool_delta = pool - Decimal::from(pool_bones);
        println!(
            "  pool: {pool} [{pool_src}]  (reconstructed {pool_bones}; exact − reconstructed = {pool_delta})"
        );
        if pool_delta < Decimal::ZERO || pool_delta >= Decimal::ONE {
            println!(
                "  ⚠ exact − reconstructed = {pool_delta} is outside [0,1): reconstructed should \
                 equal floor(0.7 × emissions) — investigate the pool accounting"
            );
        }
        println!(
            "  new        : {} DC hotspots, total {new_dc_total}; unallocated {}",
            new_dc.len(),
            alloc.unallocated
        );
        println!("  trino input: {} hotspots", rewardable.len());
        println!(
            "  hotspots differing : {}  | max |Δ| {max_abs}  | sum |Δ| {sum_abs}",
            diffs.len()
        );
        if !diffs.is_empty() {
            println!("  largest deltas (new - historical):");
            for (k, d) in diffs.iter().take(10) {
                println!("    {}  {d:+}", PublicKeyBinary::from(k.clone()));
            }
        }
    }

    Ok(())
}
