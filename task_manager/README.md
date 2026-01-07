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

- **`spawn(fut)`** - Spawns a `Send + 'static` future into its own Tokio task
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

## Shutdown Behavior

- **On SIGTERM or Ctrl+C**: All tasks are shut down in reverse order (LIFO)
- **On task error**: Remaining tasks are shut down in reverse order, then the error is returned
- **Normal completion**: When all tasks complete successfully, `start()` returns `Ok(())`

The reverse-order shutdown ensures that dependent tasks (added later) are stopped before their dependencies (added earlier).
