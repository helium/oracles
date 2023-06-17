use clap::Parser;
use mobile_config_cli::{
    cmds::{self, admin, authorization, entity, env, gateway, Cli, Commands},
    Msg, Result,
};

#[tokio::main]
async fn main() -> Result {
    let cli = Cli::parse();

    if cli.print_command {
        println!("cli:#?");
    }

    let msg = handle_cli(cli).await?;
    println!("{msg}");

    Ok(())
}

pub async fn handle_cli(cli: Cli) -> Result<Msg> {
    match cli.command {
        Commands::Env { command } => match command {
            cmds::EnvCommands::Init => env::env_init().await,
            cmds::EnvCommands::Info(args) => env::env_info(args),
            cmds::EnvCommands::GenerateKeypair(args) => env::generate_keypair(args),
        },
        Commands::Admin { command } => match command {
            cmds::AdminCommands::AddKey(args) => admin::add_key(args).await,
            cmds::AdminCommands::RemoveKey(args) => admin::remove_key(args).await,
        },
        Commands::Authorization { command } => match command {
            cmds::AuthCommands::VerifyKey(args) => authorization::verify_key_role(args).await,
            cmds::AuthCommands::ListKeys(args) => authorization::list_keys_role(args).await,
        },
        Commands::Entity { command } => match command {
            cmds::EntityCommands::VerifyEntity(args) => entity::verify_entity(args).await,
        },
        Commands::Gateway { command } => match command {
            cmds::GatewayCommands::Info(args) => gateway::info(args).await,
        },
    }
}
