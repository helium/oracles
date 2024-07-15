use sqlx::postgres::PgConnectOptions;

pub mod tracker;

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
    pub fn connect_options(&self) -> anyhow::Result<PgConnectOptions> {
        let password = rpassword::prompt_password("DB password: ")?;

        Ok(PgConnectOptions::new()
            .host(&self.host)
            .port(self.port.unwrap_or(5432))
            .database(self.database.as_deref().unwrap_or("mobile_verifier"))
            .username(&self.username)
            .password(&password))
    }
}
