mod cell_type;
mod heartbeats;
mod mobile;
mod reward_share;
mod speedtests;

pub mod cli;
pub mod scheduler;
pub mod subnetwork_rewards;
pub mod verifier;

pub fn env_var<T>(key: &str, default: T) -> anyhow::Result<T>
where
    T: std::str::FromStr,
    <T as std::str::FromStr>::Err: std::fmt::Debug + Send + Sync + std::error::Error + 'static,
{
    match dotenv::var(key) {
        Ok(v) => Ok(v.parse::<T>()?),
        Err(dotenv::Error::EnvVar(std::env::VarError::NotPresent)) => Ok(default),
        Err(err) => Err(anyhow::Error::from(err)),
    }
}
