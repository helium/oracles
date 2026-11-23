//! `icicle server`: build the route and hand it to a `TaskManager`.

use crate::{data::mobile_hotspot_history, Settings};
use anyhow::Context;
use glacon_helium::task_manager::TaskManager;

#[derive(Debug, clap::Args)]
pub struct Server;

impl Server {
    pub async fn run(&self, settings: &Settings) -> anyhow::Result<()> {
        let mut tasks = TaskManager::builder();

        let history = &settings.mobile_hotspot_history;
        let inputs: Vec<_> = history.inputs.keys().cloned().collect();
        tracing::info!(
            route = mobile_hotspot_history::ROUTE,
            ?inputs,
            catalog = %history.output.catalog.catalog_name,
            namespace = %history.table.namespace,
            table = %history.table.name,
            "adding route"
        );
        tasks = tasks.add_task(
            mobile_hotspot_history::route(history)
                .context("building the mobile_hotspot_history route")?,
        );

        // glacon owns everything below this line: it supervises each route's
        // own tasks and orders startup and shutdown.
        tasks.build().start().await.map_err(anyhow::Error::from)
    }
}
