//! A task started with `task_manager::spawn` keeps the span that was current
//! when it was spawned.

use std::sync::{Arc, Mutex};
use task_manager::{ManagedTask, TaskFuture, TaskManager};
use tracing_subscriber::{layer::SubscriberExt, registry::LookupSpan};

/// An event's message and the names of the spans it was in, outermost first.
type Captured = (String, Vec<String>);

#[tokio::test]
async fn spawned_task_keeps_the_current_span() {
    let events = Arc::new(Mutex::new(Vec::new()));
    let subscriber = tracing_subscriber::registry().with(SpanCapture {
        events: events.clone(),
    });
    let _guard = tracing::subscriber::set_default(subscriber);

    TaskManager::builder()
        .add_task(InSpan(Box::new(Spawning)))
        .build()
        .start()
        .await
        .expect("task manager");

    let events = events.lock().unwrap();
    let (_, spans) = events
        .iter()
        .find(|(message, _)| message == "inside spawned task")
        .unwrap_or_else(|| panic!("no event from the spawned task: {events:?}"));
    assert_eq!(spans, &["outer"]);
}

/// Records every event.
struct SpanCapture {
    events: Arc<Mutex<Vec<Captured>>>,
}

impl<S> tracing_subscriber::Layer<S> for SpanCapture
where
    S: tracing::Subscriber + for<'a> LookupSpan<'a>,
{
    fn on_event(&self, event: &tracing::Event<'_>, ctx: tracing_subscriber::layer::Context<'_, S>) {
        let mut message = Message::default();
        event.record(&mut message);
        let spans = ctx
            .event_scope(event)
            .map(|scope| {
                scope
                    .from_root()
                    .map(|span| span.name().to_string())
                    .collect()
            })
            .unwrap_or_default();
        self.events.lock().unwrap().push((message.0, spans));
    }
}

#[derive(Default)]
struct Message(String);

impl tracing::field::Visit for Message {
    fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
        if field.name() == "message" {
            self.0 = format!("{value:?}");
        }
    }
}

/// Spawns its work in `start_task`, like a poller or a writer does.
struct Spawning;

impl ManagedTask for Spawning {
    fn start_task(self: Box<Self>, _shutdown: triggered::Listener) -> TaskFuture {
        task_manager::spawn(async {
            tracing::info!("inside spawned task");
            Ok::<_, std::io::Error>(())
        })
    }
}

/// Enters its span around the inner task's `start_task`.
struct InSpan(Box<dyn ManagedTask>);

impl ManagedTask for InSpan {
    fn start_task(self: Box<Self>, shutdown: triggered::Listener) -> TaskFuture {
        let _entered = tracing::info_span!("outer").entered();
        self.0.start_task(shutdown)
    }
}
