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

pub async fn init(og_filter: String, file: String) -> Result<()> {
    let (filtered_layer, reload_handle) =
        reload::Layer::new(tracing_subscriber::EnvFilter::new(og_filter.clone()));

    tracing_subscriber::registry()
        .with(filtered_layer)
        .with(tracing_subscriber::fmt::layer())
        .init();

    tokio::spawn(async move {
        let state = State {
            og_filter: og_filter.clone(),
            file,
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
    T: ToString,
{
    Span::current().record(field, &tracing::field::display(value.to_string()));
}

#[derive(Clone)]
pub struct State {
    pub og_filter: String,
    pub file: String,
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
                Err(e) => tracing::warn!("watch error: {:?}", e),
                Ok(event) => match event.kind {
                    notify::EventKind::Modify(notify::event::ModifyKind::Data(
                        DataChange::Content,
                    )) => {
                        let event_path = event.paths.first().unwrap();

                        if Path::new(event_path).exists() {
                            match fs::read_to_string(event_path) {
                                Err(_e) => tracing::warn!("failed to read file {:?}", _e),
                                Ok(content) => {
                                    if file_match(event_path, self.file.clone()) {
                                        self.handle_change(content)?;
                                    }
                                }
                            }
                        }
                    }
                    notify::EventKind::Remove(notify::event::RemoveKind::File) => {
                        let event_path = event.paths.first().unwrap();
                        if file_match(event_path, self.file.clone()) {
                            self.handle_delete()?;
                        }
                    }
                    _e => tracing::debug!("ignored {:?}", _e),
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
                Err(_e) => tracing::warn!(filter = content, "failed to parse filter {:?}", _e),
                Ok(new_filter) => {
                    self.reload_handle.modify(|filter| *filter = new_filter)?;
                    tracing::info!(filter = content, "updated");
                }
            }
            Ok(())
        }
    }

    fn handle_delete(&self) -> Result<()> {
        let new_filter = self.og_filter.clone();

        self.reload_handle
            .modify(|filter| *filter = tracing_subscriber::EnvFilter::new(new_filter.clone()))?;

        tracing::info!(filter = new_filter, "deleted tracing file, updated");
        Ok(())
    }
}

fn file_match(event_path: &Path, file: String) -> bool {
    let split = event_path.to_str().unwrap().split('/');
    *split.last().unwrap() == file
}
