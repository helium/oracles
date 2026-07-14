use anyhow::{Context, Result};
use chrono::Utc;

mod data_sessions;
mod heartbeats;
mod iceberg_bootstrap;
mod next_steps;
mod pg_bootstrap;
mod plan;
mod speedtests;

pub use plan::Plan;

/// Default admin DB URL for `CREATE DATABASE` operations on the local
/// docker-compose Postgres. The admin connection has to land somewhere that
/// already exists; `postgres` is the conventional default DB created at
/// initdb time.
const DEFAULT_ADMIN_DATABASE_URL: &str = "postgres://postgres:postgres@127.0.0.1:5432/postgres";
const DEFAULT_MV_DATABASE_URL: &str = "postgres://postgres:postgres@127.0.0.1:5432/mobile_verifier";
const DEFAULT_MPV_DATABASE_URL: &str =
    "postgres://postgres:postgres@127.0.0.1:5432/mobile_packet_verifier";
const DEFAULT_TRINO_HOST: &str = "127.0.0.1";
const DEFAULT_TRINO_PORT: u16 = 8080;
const DEFAULT_TRINO_USER: &str = "admin";
const DEFAULT_TRINO_CATALOG: &str = "iceberg";

/// Seed is a bootstrap step — it runs *before* any settings file is needed,
/// so it takes every endpoint as a flag with docker-compose-friendly defaults
/// instead of reading `compare_trino::Settings`. That lets `compare-trino
/// seed` work with zero config out of the box.
#[derive(Debug, clap::Args)]
pub struct Cmd {
    /// DROP DATABASE both mobile_verifier and mobile_packet_verifier before
    /// recreating + migrating. Use this on the first run or whenever the
    /// migrations change.
    #[clap(long)]
    pub reset: bool,
    /// Number of hotspots to seed. Split across four buckets to exercise
    /// every diff status. Default 10.
    #[clap(long, default_value = "10")]
    pub hotspots: usize,
    /// Postgres URL that has CREATE DATABASE privileges. The seeder connects
    /// here to create the per-app databases; once those exist, this URL is
    /// not used again.
    #[clap(long, default_value = DEFAULT_ADMIN_DATABASE_URL)]
    pub admin_database_url: String,
    /// Postgres URL for the mobile_verifier database. Will be created if missing.
    #[clap(long, default_value = DEFAULT_MV_DATABASE_URL)]
    pub mv_database_url: String,
    /// Postgres URL for the mobile_packet_verifier database (used by the
    /// pending-burns comparison). Will be created if missing.
    #[clap(long, default_value = DEFAULT_MPV_DATABASE_URL)]
    pub mpv_database_url: String,
    /// Trino coordinator host. Default targets docker-compose.
    #[clap(long, default_value = DEFAULT_TRINO_HOST)]
    pub trino_host: String,
    #[clap(long, default_value_t = DEFAULT_TRINO_PORT)]
    pub trino_port: u16,
    #[clap(long, default_value = DEFAULT_TRINO_USER)]
    pub trino_user: String,
    #[clap(long, default_value = DEFAULT_TRINO_CATALOG)]
    pub trino_catalog: String,
    /// RNG seed for deterministic hotspot pubkey generation. Defaults to 42 so
    /// repeated runs produce the same fixture set.
    #[clap(long, default_value = "42")]
    pub seed: u64,
}

pub async fn run(args: Cmd) -> Result<()> {
    // 1) Build the deterministic plan up-front so all seeders agree on the
    //    window and the bucket assignments.
    let plan = Plan::build(args.hotspots, args.seed, Utc::now());
    print_plan_header(&plan);

    // 2) Bootstrap both Postgres databases (create + migrate).
    let pools = pg_bootstrap::run(&args)
        .await
        .context("postgres bootstrap")?;

    // 3) Connect to Trino and bootstrap the Iceberg schemas + tables.
    let trino =
        trino_client::ClientBuilder::new(&args.trino_host, args.trino_port, &args.trino_user)
            .catalog(&args.trino_catalog)
            .build()
            .context("building Trino client")?;
    iceberg_bootstrap::run(&trino)
        .await
        .context("iceberg bootstrap")?;

    // 4) Seed each comparison's fixtures.
    heartbeats::seed(&pools.mv, &trino, &plan)
        .await
        .context("seeding heartbeats")?;
    speedtests::seed(&pools.mv, &trino, &plan)
        .await
        .context("seeding speedtests")?;
    data_sessions::seed(&pools.mv, &pools.mpv, &trino, &plan)
        .await
        .context("seeding data sessions")?;

    // 5) Tell the user what to run next.
    next_steps::print(&plan, &args).context("writing next-steps file")?;
    Ok(())
}

fn print_plan_header(plan: &Plan) {
    println!("=== compare-trino seed ===");
    println!(
        "window: {} → {}",
        plan.epoch_start.to_rfc3339(),
        plan.epoch_end.to_rfc3339(),
    );
    let counts = plan.counts();
    print!("buckets:");
    for (bucket, n) in counts {
        print!(" {}={}", bucket.as_str(), n);
    }
    println!();
}
