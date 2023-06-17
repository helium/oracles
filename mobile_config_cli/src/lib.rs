pub mod client;
pub mod cmds;

use anyhow::Error;
use serde::Serialize;
use std::{
    fmt::Display,
    time::{SystemTime, UNIX_EPOCH},
};

pub mod proto {
    pub use helium_proto::services::mobile_config::{AdminKeyRole, NetworkKeyRole};
}

pub type Result<T = (), E = Error> = anyhow::Result<T, E>;

#[derive(Debug, Serialize)]
pub enum Msg {
    DryRun(String),
    Success(String),
    Error(String),
}

impl Msg {
    pub fn ok(msg: String) -> Result<Self> {
        Ok(Self::Success(msg))
    }

    pub fn err(msg: String) -> Result<Self> {
        Ok(Self::Error(msg))
    }

    pub fn dry_run(msg: String) -> Result<Self> {
        Ok(Self::DryRun(msg))
    }

    pub fn into_inner(self) -> String {
        match self {
            Msg::DryRun(s) => s,
            Msg::Success(s) => s,
            Msg::Error(s) => s,
        }
    }
}

impl Display for Msg {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Msg::DryRun(msg) => write!(f, "== DRY RUN == (pass `--commit`)\n{msg}"),
            Msg::Success(msg) => write!(f, "{msg}"),
            Msg::Error(msg) => write!(f, "\u{2717} {msg}"),
        }
    }
}

pub fn current_timestamp() -> Result<u64> {
    Ok(SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis() as u64)
}

pub trait PrettyJson {
    fn print_pretty_json(&self) -> Result;
    fn pretty_json(&self) -> Result<String>;
}

impl<S: ?Sized + serde::Serialize> PrettyJson for S {
    fn print_pretty_json(&self) -> Result {
        println!("{}", self.pretty_json()?);
        Ok(())
    }

    fn pretty_json(&self) -> Result<String> {
        serde_json::to_string_pretty(&self).map_err(|e| e.into())
    }
}

#[derive(Debug, clap::ValueEnum, Clone, Copy, Serialize)]
pub enum KeyRole {
    Administrator,
    Carrier,
    Router,
    Oracle,
}

impl From<KeyRole> for proto::AdminKeyRole {
    fn from(value: KeyRole) -> Self {
        match value {
            KeyRole::Administrator => Self::Administrator,
            KeyRole::Carrier => Self::Carrier,
            KeyRole::Router => Self::Router,
            KeyRole::Oracle => Self::Oracle,
        }
    }
}

impl From<KeyRole> for i32 {
    fn from(value: KeyRole) -> Self {
        proto::AdminKeyRole::from(value) as i32
    }
}

impl Display for KeyRole {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            KeyRole::Administrator => write!(f, "Administrator"),
            KeyRole::Carrier => write!(f, "Carrier"),
            KeyRole::Oracle => write!(f, "Oracle"),
            KeyRole::Router => write!(f, "Router"),
        }
    }
}

#[derive(Debug, clap::ValueEnum, Clone, Copy, Serialize)]
pub enum NetworkKeyRole {
    #[value(alias("carrier"))]
    MobileCarrier,
    #[value(alias("router"))]
    MobileRouter,
}

impl From<NetworkKeyRole> for proto::NetworkKeyRole {
    fn from(value: NetworkKeyRole) -> Self {
        match value {
            NetworkKeyRole::MobileRouter => Self::MobileRouter,
            NetworkKeyRole::MobileCarrier => Self::MobileCarrier,
        }
    }
}

impl From<NetworkKeyRole> for i32 {
    fn from(value: NetworkKeyRole) -> Self {
        proto::NetworkKeyRole::from(value) as i32
    }
}

impl Display for NetworkKeyRole {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            NetworkKeyRole::MobileCarrier => write!(f, "Carrier"),
            NetworkKeyRole::MobileRouter => write!(f, "Router"),
        }
    }
}
