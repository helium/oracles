use crate::{
    service_provider::promotions::funds::{
        delete_promotion_fund, fetch_promotion_funds, save_promotion_fund,
    },
    Settings,
};

#[derive(Debug, clap::Args)]
pub struct Cmd {
    #[clap(subcommand)]
    sub_command: SubCommand,
}

#[derive(Debug, clap::Subcommand)]
enum SubCommand {
    /// Print Service Provider promotions in mobile-verifier db
    List,
    /// Set Service Provider promotion in mobile-verifier db
    Set {
        service_provider_id: i32,
        basis_points: u16,
    },
    /// Remove Service Provider promotion allocation from mobile-verifier db
    Unset { service_provider_id: i32 },
}

impl Cmd {
    pub async fn run(&self, settings: &Settings) -> anyhow::Result<()> {
        let pool = settings.database.connect(env!("CARGO_PKG_NAME")).await?;

        match self.sub_command {
            SubCommand::List => {
                let funds = fetch_promotion_funds(&pool).await?;
                println!("{funds:?}");
            }
            SubCommand::Set {
                service_provider_id,
                basis_points,
            } => {
                let mut txn = pool.begin().await?;
                save_promotion_fund(&mut txn, service_provider_id, basis_points).await?;
                txn.commit().await?;

                let funds = fetch_promotion_funds(&pool).await?;
                println!("{funds:?}");
            }
            SubCommand::Unset {
                service_provider_id,
            } => {
                delete_promotion_fund(&pool, service_provider_id).await?;

                let funds = fetch_promotion_funds(&pool).await?;
                println!("{funds:?}");
            }
        }

        Ok(())
    }
}
