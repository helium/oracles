use anyhow::Result;
use notify::{event::DataChange, Config, RecommendedWatcher, RecursiveMode, Watcher};
use std::{fs, path::Path};
use tracing::Span;
use tracing_subscriber::{
    layer::SubscriberExt,
    reload::{self, Handle},
    util::SubscriberInitExt,
    EnvFilter, Registry,
};

#[cfg(feature = "grpc")]
pub mod grpc_layer;
#[cfg(feature = "http-1")]
pub mod http_layer;

pub async fn init(og_filter: String, tracing_config_file: String) -> Result<()> {
    let (filtered_layer, reload_handle) =
        reload::Layer::new(tracing_subscriber::EnvFilter::new(og_filter.clone()));

    tracing_subscriber::registry()
        .with(filtered_layer)
        .with(tracing_subscriber::fmt::layer())
        .init();

    tokio::spawn(async move {
        let state = State {
            og_filter: og_filter.clone(),
            tracing_config_file,
            reload_handle,
        };
        if let Err(err) = state.watch().await {
            tracing::warn!(?err, "tracing error watching configuration for update")
        }
    });

    tracing::info!("custom tracing installed");

    Ok(())
}

pub fn record<T>(field: &str, value: T)
where
    T: std::fmt::Display,
{
    Span::current().record(field, &tracing::field::display(value));
}

#[derive(Clone)]
pub struct State {
    pub og_filter: String,
    pub tracing_config_file: String,
    pub reload_handle: Handle<EnvFilter, Registry>,
}

impl State {
    async fn watch(&self) -> Result<()> {
        let (tx, mut rx) = tokio::sync::mpsc::channel(10);

        let mut watcher = RecommendedWatcher::new(
            move |res| {
                tx.blocking_send(res).expect("Failed to send event");
            },
            Config::default(),
        )?;

        watcher.watch(".".as_ref(), RecursiveMode::NonRecursive)?;

        while let Some(res) = rx.recv().await {
            match res {
                Err(err) => tracing::warn!(?err, "tracing config watcher configuration file error"),
                Ok(event) => match event.kind {
                    notify::EventKind::Modify(notify::event::ModifyKind::Data(
                        DataChange::Content,
                    )) => {
                        if let Some(event_path) = event.paths.first() {
                            if Path::new(event_path).exists() {
                                match fs::read_to_string(event_path) {
                                    Err(_err) => tracing::warn!(
                                        ?_err,
                                        "tracing config watcher failed to read file"
                                    ),
                                    Ok(content) => {
                                        if file_match(event_path, self.tracing_config_file.clone())
                                        {
                                            self.handle_change(content)?;
                                        }
                                    }
                                }
                            }
                        }
                    }
                    notify::EventKind::Remove(notify::event::RemoveKind::File) => {
                        if let Some(event_path) = event.paths.first() {
                            if file_match(event_path, self.tracing_config_file.clone()) {
                                self.handle_delete()?;
                            }
                        }
                    }
                    _event => {
                        tracing::debug!(?_event, "tracing config watcher ignored unhandled message")
                    }
                },
            }
        }

        Ok(())
    }

    fn handle_change(&self, content: String) -> Result<()> {
        if content.is_empty() {
            self.handle_delete()
        } else {
            match tracing_subscriber::EnvFilter::try_new(content.clone()) {
                Err(_err) => tracing::warn!(
                    filter = content,
                    ?_err,
                    "tracing config watcher failed to parse filter"
                ),
                Ok(new_filter) => {
                    self.reload_handle.modify(|filter| *filter = new_filter)?;
                    tracing::info!(filter = content, "custom tracing config updated");
                }
            }
            Ok(())
        }
    }

    fn handle_delete(&self) -> Result<()> {
        let new_filter = self.og_filter.clone();

        self.reload_handle
            .modify(|filter| *filter = tracing_subscriber::EnvFilter::new(new_filter.clone()))?;

        tracing::info!(
            filter = new_filter,
            "tracing config watcher file deleted, reverting to rustlog filter"
        );
        Ok(())
    }
}

fn file_match(event_path: &Path, file: String) -> bool {
    event_path
        .to_str()
        .map(|path| path.split('/'))
        .map(|path| path.last().is_some_and(|file_name| file_name == file))
        .unwrap_or_default()
}
