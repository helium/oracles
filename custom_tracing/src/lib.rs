use anyhow::Result;
use futures::{channel::mpsc::channel, SinkExt, StreamExt};
use notify::{event::DataChange, Config, RecommendedWatcher, RecursiveMode, Watcher};
use std::{fs, path::Path};
use tracing_subscriber::{
    layer::SubscriberExt,
    reload::{self, Handle},
    util::SubscriberInitExt,
    EnvFilter, Registry,
};

pub async fn init(og_filter: String, file: String) -> Result<()> {
    let (filtered_layer, reload_handle) =
        reload::Layer::new(tracing_subscriber::EnvFilter::new(og_filter.clone()));

    tracing_subscriber::registry()
        .with(filtered_layer)
        .with(tracing_subscriber::fmt::layer())
        .init();

    let state = State {
        og_filter: og_filter.clone(),
        reload_handle,
    };

    tokio::spawn(async move {
        if let Err(e) = watch_file(state, file).await {
            tracing::warn!("error: {:?}", e)
        }
    });

    Ok(())
}

#[derive(Clone)]
pub struct State {
    pub og_filter: String,
    pub reload_handle: Handle<EnvFilter, Registry>,
}

impl State {
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

async fn watch_file(state: State, file: String) -> Result<()> {
    let (mut tx, mut rx) = channel(1);

    let mut watcher = RecommendedWatcher::new(
        move |res| {
            futures::executor::block_on(async {
                tx.send(res).await.unwrap();
            })
        },
        Config::default(),
    )?;

    watcher.watch(".".as_ref(), RecursiveMode::NonRecursive)?;

    while let Some(res) = rx.next().await {
        match res {
            Err(e) => tracing::warn!("watch error: {:?}", e),
            Ok(event) => match event.kind {
                notify::EventKind::Modify(notify::event::ModifyKind::Data(DataChange::Content)) => {
                    let event_path = event.paths.first().unwrap();

                    if Path::new(event_path).exists() {
                        match fs::read_to_string(event_path) {
                            Err(_e) => tracing::warn!("failed to read file {:?}", _e),
                            Ok(content) => {
                                if file_match(event_path, file.clone()) {
                                    state.handle_change(content)?;
                                }
                            }
                        }
                    }
                }
                notify::EventKind::Remove(notify::event::RemoveKind::File) => {
                    let event_path = event.paths.first().unwrap();
                    if file_match(event_path, file.clone()) {
                        state.handle_delete()?;
                    }
                }
                _e => tracing::debug!("ignored {:?}", _e),
            },
        }
    }

    Ok(())
}

fn file_match(event_path: &Path, file: String) -> bool {
    let split = event_path.to_str().unwrap().split('/');
    *split.last().unwrap() == file
}
