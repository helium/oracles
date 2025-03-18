use sqlx::PgPool;

use crate::{db, settings::Mode, Settings};

#[derive(Debug, clap::Args)]
pub struct Escrow {
    #[clap(subcommand)]
    cmd: EscrowCmds,
}

impl Escrow {
    pub async fn run(self, settings: &Settings) -> anyhow::Result<()> {
        self.cmd.run(settings).await
    }
}

#[derive(Debug, clap::Subcommand)]
pub enum EscrowCmds {
    /// Take all known radios in the rewards table and give them an escrow duration of 0 (zero) days.
    Migrate(EscrowMigrate),
    /// Get the current escrow duration for an address
    Get(EscrowGet),
    /// Set the escrow duration for an address
    Set(EscrowSet),
    /// Delete the escrow duration for an address
    Delete(EscrowDelete),
}

impl EscrowCmds {
    pub async fn run(self, settings: &Settings) -> anyhow::Result<()> {
        anyhow::ensure!(
            matches!(settings.mode, Mode::Mobile | Mode::MobileEscrowed),
            "escrow commands are only available for 'mobile' and 'mobile_escrowed'"
        );

        let app_name = format!("{}_{}", settings.mode, env!("CARGO_PKG_NAME"));
        let pool = settings.database.connect(&app_name).await?;
        sqlx::migrate!().run(&pool).await?;

        match self {
            EscrowCmds::Migrate(cmd) => cmd.run(pool).await?,
            EscrowCmds::Get(cmd) => cmd.run(pool, settings.escrow.default_days).await?,
            EscrowCmds::Set(cmd) => cmd.run(pool).await?,
            EscrowCmds::Delete(cmd) => cmd.run(pool, settings.escrow.default_days).await?,
        }

        Ok(())
    }
}

#[derive(Debug, clap::Args)]
pub struct EscrowMigrate {
    /// Date on which to expire the grandfathered escrow_duration of 0 days.
    #[arg(long)]
    pub expires_on: chrono::NaiveDate,
}

impl EscrowMigrate {
    async fn run(self, pool: PgPool) -> anyhow::Result<()> {
        let EscrowMigrate { expires_on } = self;

        let migrated_addresses =
            db::escrow_duration::migrate_known_radios(&pool, expires_on).await?;
        println!("Migrated {migrated_addresses} radios");

        Ok(())
    }
}

#[derive(Debug, clap::Args)]
pub struct EscrowGet {
    #[arg(short, long)]
    pub address: String,
}

impl EscrowGet {
    async fn run(self, pool: PgPool, default_days: u32) -> anyhow::Result<()> {
        let EscrowGet { address } = self;

        let duration = db::escrow_duration::get(&pool, &address).await?;

        match duration {
            Some((days, Some(expiration))) => {
                println!("{address} escrow period is {days} days, expring on {expiration}");
            }
            Some((days, None)) => {
                println!("{address} escrow period is {days} days, forever");
            }
            None => {
                println!("{address} uses the default escrow period: {default_days} days",);
            }
        }
        Ok(())
    }
}

#[derive(Debug, clap::Args)]
pub struct EscrowSet {
    #[arg(long)]
    pub address: String,

    #[arg(long)]
    pub days: u32,

    #[arg(long)]
    pub expires_on: Option<chrono::NaiveDate>,
}

impl EscrowSet {
    async fn run(self, pool: PgPool) -> anyhow::Result<()> {
        let EscrowSet {
            address,
            days,
            expires_on,
        } = self;

        let _inserted = db::escrow_duration::insert(&pool, &address, days, expires_on).await?;

        match expires_on {
            Some(expiration) => {
                println!("{address} escrow period is {days} days, expiring on {expiration}");
            }
            None => {
                println!("{address} escrow period is {days} days, forever");
            }
        }
        Ok(())
    }
}

#[derive(Debug, clap::Args)]
pub struct EscrowDelete {
    #[arg(long)]
    pub address: String,
}

impl EscrowDelete {
    async fn run(self, pool: PgPool, default_days: u32) -> anyhow::Result<()> {
        let EscrowDelete { address } = self;

        db::escrow_duration::delete(&pool, &address).await?;
        println!("{address} now uses the default escrow duration: {default_days} days");

        Ok(())
    }
}
