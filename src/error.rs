use thiserror::Error;

pub type Result<T = ()> = std::result::Result<T, Error>;

#[derive(Error, Debug)]
pub enum Error {
    #[error("environment error")]
    Env(#[from] std::env::VarError),
    #[error("sql error")]
    Sql(#[from] sqlx::Error),
    #[error("migration error")]
    Migrate(#[from] sqlx::migrate::MigrateError),
    #[error("http error")]
    Http(#[from] hyper::Error),
}
