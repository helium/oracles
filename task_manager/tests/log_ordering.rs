//! Integration test to verify log message ordering when a task fails.

use std::sync::{Arc, Mutex};
use task_manager::{ManagedTask, TaskError, TaskFuture, TaskManager};
use tokio::sync::mpsc;
use tracing_subscriber::layer::SubscriberExt;

/// A tracing layer that captures log messages in order for testing.
struct LogCapture {
    logs: Arc<Mutex<Vec<String>>>,
}

impl<S: tracing::Subscriber> tracing_subscriber::Layer<S> for LogCapture {
    fn on_event(
        &self,
        event: &tracing::Event<'_>,
        _ctx: tracing_subscriber::layer::Context<'_, S>,
    ) {
        let mut visitor = MessageVisitor::default();
        event.record(&mut visitor);
        let level = event.metadata().level();
        let message = format!("({}) {} {}", visitor.task, level, visitor.message);
        self.logs.lock().unwrap().push(message);
    }
}

#[derive(Default)]
struct MessageVisitor {
    task: String,
    message: String,
}

impl tracing::field::Visit for MessageVisitor {
    fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
        match field.name() {
            "message" => self.message = format!("{:?}", value),
            "task" => self.task = format!("{:?}", value),
            _ => {}
        }
    }
}

#[derive(Debug, thiserror::Error)]
#[error("Test error: {0}")]
struct TestError(&'static str);

struct TestTask {
    name: &'static str,
    delay: u64,
    result: Result<(), TaskError>,
    sender: mpsc::Sender<&'static str>,
}

impl ManagedTask for TestTask {
    fn start_task(self: Box<Self>, shutdown_listener: triggered::Listener) -> TaskFuture {
        use futures::FutureExt;

        let handle = tokio::spawn(async move {
            tokio::select! {
                _ = shutdown_listener.clone() => (),
                _ = tokio::time::sleep(std::time::Duration::from_millis(self.delay)) => (),
            }
            self.sender.send(self.name).await.expect("unable to send");
            self.result
        });

        Box::pin(handle.map(|result| match result {
            Ok(inner) => inner,
            Err(e) => Err(TaskError::from_err(e)),
        }))
    }
}

#[tokio::test]
async fn logs_error_before_shutdown_messages() {
    let logs = Arc::new(Mutex::new(Vec::new()));
    let layer = LogCapture { logs: logs.clone() };

    let subscriber = tracing_subscriber::registry().with(layer);
    let _guard = tracing::subscriber::set_default(subscriber);

    let (sender, mut receiver) = mpsc::channel(5);

    let _result = TaskManager::builder()
        .add_task(TestTask {
            name: "1",
            delay: 1000,
            result: Ok(()),
            sender: sender.clone(),
        })
        .add_task(TestTask {
            name: "2",
            delay: 50,
            result: Err(TaskError::from_err(TestError("test error"))),
            sender: sender.clone(),
        })
        .add_task(TestTask {
            name: "3",
            delay: 1000,
            result: Ok(()),
            sender: sender.clone(),
        })
        .add_named("special", |_shutdown| async { Ok(()) })
        .add_task(|_shutdown| async { Ok(()) })
        .build()
        .start()
        .await;

    // Drain the receiver
    while receiver.try_recv().is_ok() {}

    let captured_logs = logs.lock().unwrap();
    // for log in captured_logs.iter() {
    //     println!("-- {}", log);
    // }

    // Verify we have logs
    assert!(
        captured_logs.len() >= 3,
        "Expected at least 3 log messages, got: {:?}",
        *captured_logs
    );

    // Find the indices of the error and shutdown messages
    let error_idx = captured_logs
        .iter()
        .position(|log| log.contains("ERROR") && log.contains("task failed"))
        .expect("Expected to find error log");

    let shutdown_indices: Vec<usize> = captured_logs
        .iter()
        .enumerate()
        .filter(|(_, log)| log.contains("INFO") && log.contains("shutting down"))
        .map(|(i, _)| i)
        .collect();

    // Verify the error comes before all shutdown messages
    for shutdown_idx in &shutdown_indices {
        assert!(
            error_idx < *shutdown_idx,
            "Error log (index {}) should appear before shutdown log (index {}). Logs: {:?}",
            error_idx,
            shutdown_idx,
            *captured_logs
        );
    }
}
