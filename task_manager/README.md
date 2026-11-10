# task-manager

A lightweight async task lifecycle management crate built on Tokio. It provides coordinated startup and graceful shutdown for multiple long-running tasks with proper error handling.

## Features

- Run multiple async tasks concurrently
- Graceful shutdown on SIGTERM or Ctrl+C
- Reverse-order task shutdown (LIFO) for proper cleanup
- Error propagation with automatic shutdown of remaining tasks
- Composable task managers (nest managers within managers)
- Builder pattern for fluent task registration

## Key Types

- **`TaskManager`** - Core orchestrator that manages task lifecycle
- **`TaskManagerBuilder`** - Builder for fluent task registration
- **`ManagedTask`** - Trait for types that can be managed as tasks
- **`TaskLocalBoxFuture`** - Type alias for task return type (`LocalBoxFuture<'static, anyhow::Result<()>>`)

## Helper Functions

- **`spawn(fut)`** - Spawns a `Send + 'static` future into its own Tokio task, in the current span (see [Tracing Spans](#tracing-spans))
- **`run(fut)`** - Boxes a non-`Send` future for local execution

## Usage

### Builder Pattern (Recommended)

```rust
use task_manager::TaskManager;

TaskManager::builder()
    .add_task(my_server)
    .add_task(my_worker)
    .add_task(my_sink)
    .build()
    .start()
    .await?;
```

### Direct Construction

```rust
use task_manager::TaskManager;

let mut task_manager = TaskManager::new();
task_manager.add(my_server);
task_manager.add(my_worker);

// Dynamically add tasks
for config in configs {
    task_manager.add(Worker::new(config));
}

task_manager.start().await?;
```

### Implementing ManagedTask

Implement the `ManagedTask` trait for your types:

```rust
use task_manager::{ManagedTask, TaskFuture};

struct MyDaemon {
    // fields...
}

impl ManagedTask for MyDaemon {
    fn start_task(
        self: Box<Self>,
        shutdown: triggered::Listener,
    ) -> TaskFuture {
        task_manager::spawn(self.run(shutdown))
    }
}

impl MyDaemon {
    async fn run(self, shutdown: triggered::Listener) -> anyhow::Result<()> {
        loop {
            tokio::select! {
                _ = shutdown.clone() => break,
                // handle other events...
            }
        }
        Ok(())
    }
}
```

### Closure Tasks

Closures that accept a shutdown listener are automatically `ManagedTask`:

```rust
use task_manager::TaskManager;

TaskManager::builder()
    .add_task(my_server)
    .add_task(|shutdown| async move {
        loop {
            tokio::select! {
                _ = shutdown.clone() => break,
                _ = do_work() => {}
            }
        }
        Ok(())
    })
    .build()
    .start()
    .await?;
```

## Tracing Spans

`spawn(fut)` runs the spawned task in the span that is current when `spawn`
is called (`fut.in_current_span()`). Tokio doesn't do this on its own: a task
it polls runs outside any span, even one the caller instrumented its own
future with.

So a caller can give a task's logs context by entering a span around its
`start_task`, without changing the task:

```rust
use task_manager::{ManagedTask, TaskFuture};

struct WithSpan {
    span: tracing::Span,
    task: Box<dyn ManagedTask>,
}

impl ManagedTask for WithSpan {
    fn start_task(self: Box<Self>, shutdown: triggered::Listener) -> TaskFuture {
        // A task that calls `task_manager::spawn` in `start_task` (a
        // FileInfoPoller, a BatchedWriter) runs in `span`.
        let WithSpan { span, task } = *self;
        let started = span.in_scope(|| task.start_task(shutdown));
        // A task that doesn't spawn is polled here: instrument it too.
        Box::pin(tracing::Instrument::instrument(started, span))
    }
}

let poller = WithSpan {
    span: tracing::info_span!("poller", prefix = "speedtest_report"),
    task: Box::new(file_info_poller),
};
```

Its logs then carry the span:

```text
INFO poller{prefix="speedtest_report"}: file_store::file_info_poller: starting FileInfoPoller …
```

### Targeting a span in `RUST_LOG`

With `tracing_subscriber`'s `EnvFilter`, a `[span{field=value}]=level`
directive sets the level of every log in a matching span, whichever crate
logs it:

```bash
# Everything logged in the speedtest_report poller's span, at debug
RUST_LOG='info,[poller{prefix=speedtest_report}]=debug'
# Every span with that field, whatever its name
RUST_LOG='info,[{prefix=speedtest_report}]=debug'
```

- A directive only matches the span a log is directly in, not its parents:
  for a task in a span nested in yours, the directive must match the inner
  span.
- One field per directive: a comma inside `{…}` breaks `RUST_LOG`, which is
  split on commas.
- A target before `[…]` is matched against the span's target, not the log's:
  `file_store[poller]=debug` matches only if the span was created in
  `file_store`, so a directive can't keep only one crate's logs in a span.

## Shutdown Behavior

- **On SIGTERM or Ctrl+C**: All tasks are shut down in reverse order (LIFO)
- **On task error**: Remaining tasks are shut down in reverse order, then the error is returned
- **Normal completion**: When all tasks complete successfully, `start()` returns `Ok(())`

The reverse-order shutdown ensures that dependent tasks (added later) are stopped before their dependencies (added earlier).
