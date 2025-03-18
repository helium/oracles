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
    Migrate {
        /// Date on which to expire the grandfathered escrow_duration of 0 days.
        #[arg(long)]
        expires_on: chrono::NaiveDate,
    },
    /// Get the current escrow duration for an address
    Get {
        #[arg(short, long)]
        address: String,
    },
    /// Set the escrow duration for an address
    Set {
        #[arg(long)]
        address: String,

        #[arg(long)]
        days: u32,

        #[arg(long)]
        expires_on: Option<chrono::NaiveDate>,
    },
    /// Delete the escrow duration for an address
    Delete {
        #[arg(long)]
        address: String,
    },
}

impl EscrowCmds {
    pub async fn run(self, settings: &Settings) -> anyhow::Result<()> {
        anyhow::ensure!(
            matches!(settings.mode, Mode::Mobile | Mode::MobileEscrowed),
            "migration is only available for 'mobile' and 'mobile_escrowed'"
        );

        let app_name = format!("{}_{}", settings.mode, env!("CARGO_PKG_NAME"));
        let pool = settings.database.connect(&app_name).await?;
        sqlx::migrate!().run(&pool).await?;

        match self {
            EscrowCmds::Migrate { expires_on } => {
                let migrated_addresses =
                    db::escrow_duration::migrate_known_radios(&pool, expires_on).await?;
                tracing::info!(migrated_addresses, "done");
            }
            EscrowCmds::Get { address } => {
                let duration = db::escrow_duration::get(&pool, &address).await?;
                match duration {
                    Some((days, Some(expiration))) => {
                        println!("{address} has an escrow period of {days} days, expring on {expiration}");
                    }
                    Some((days, None)) => {
                        println!("{address} has an escrow period of {days} days, forever");
                    }
                    None => {
                        println!(
                            "{address} uses the default escrow duration: {} days",
                            settings.escrow.default_days
                        );
                    }
                }
            }
            EscrowCmds::Set {
                address,
                days,
                expires_on,
            } => {
                let _inserted =
                    db::escrow_duration::insert(&pool, &address, days, expires_on).await?;

                match expires_on {
                    Some(expiration) => {
                        println!("{address} now has escrow period of {days} days, expiring on {expiration}");
                    }
                    None => {
                        println!("{address} now has escrow period of {days} days, forever");
                    }
                }
            }
            EscrowCmds::Delete { address } => {
                db::escrow_duration::delete(&pool, &address).await?;
                println!(
                    "{address} is now using the default escrow duration: {} days",
                    settings.escrow.default_days
                )
            }
        }

        Ok(())
    }
}
