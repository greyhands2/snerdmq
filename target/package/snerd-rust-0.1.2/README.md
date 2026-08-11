# ⚙️ snerd-rust

> *A blazingly fast, brutally simple, zero-dependency async background job engine for Rust.*

[![Crates.io](https://img.shields.io/crates/v/snerd-rust.svg)](https://crates.io/crates/snerd-rust)
[![Documentation](https://docs.rs/snerd-rust/badge.svg)](https://docs.rs/snerd-rust)
[![CI](https://github.com/greyhands2/snerd-rust/actions/workflows/ci.yml/badge.svg)](https://github.com/greyhands2/snerd-rust/actions/workflows/ci.yml)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

If you are tired of wrestling with heavy, bloated background job frameworks like Redis, Postgres tables, or RabbitMQ just to send a few emails in the background... well, you are in the right place. 

`snerd-rust` is an embedded, high-performance background task queue that lives entirely in a single, perfectly OS-locked, append-only `.log` file on your file system. It was designed to bring the aggressive concurrency and lightweight footprint of Golang's `snerd` over to Rust's heavily optimized asynchronous ecosystem.

No databases. No external daemons. No nonsense.

---

## 🔥 Features
* **Zero External Infrastructure**: You don't need a Redis cluster. Your tasks are persisted directly to `.snerdata/tasks/tasks.log` using standard filesystem I/O.
* **Bulletproof File Locks**: Safely scales across multiple processes! We utilize OS-level file-locking boundaries (`flock`) to guarantee that your tasks are never corrupted, even if multiple instances of your app try to write simultaneously.
* **Asynchronous Tokio Core**: Built natively on top of `tokio`. Background workers process the queue without starving your main event loop.
* **Aggressive Compaction**: Deleted tasks don't bloat your system. `snerd-rust` runs background log compactions automatically once your queue hits safe threshold limits.
* **Dead-Letter Queue (DLQ)**: Built-in `maxRetries` limits and hooks to elegantly catch and bury poison-pill tasks.

---

## 📦 Installation

Just add `snerd-rust` to your `Cargo.toml`:

```toml
[dependencies]
snerd-rust = "0.1.0"
```

*Note: You will also need `tokio` (with full features) since snerd is entirely async.*

---

## 🚀 Quickstart

It takes roughly 3 lines of code to spin up a queue and start firing background jobs. 

```rust
use snerd_rust::queue::SnerdQueue;
use snerd_rust::file_store::FileStore;
use snerd_rust::task::RetryableTask;
use std::time::Duration;

#[tokio::main]
async fn main() {
    // 1. Initialize the Persistence Store
    let file_store = FileStore::new(".snerdata/tasks/tasks.log").unwrap();
    
    // 2. Create the Queue
    let queue = SnerdQueue::new("my-fast-queue", file_store);

    // 3. Register your Task Handler (The closure that does the actual work)
    queue.register_task_handler("send_email", |data| {
        println!("Sending email with payload: {}", data);
        // ... do your heavy lifting here!
        Ok(()) // Return Err("...") to trigger a retry!
    }).await;
    
    // 4. (Optional) Register a Dead-Letter Handler for when retries run out
    queue.register_max_retry_handler("send_email", |data| {
        println!("Task permanently failed! Payload: {}", data);
        Ok(())
    }).await;

    // 5. Boot the background processor polling loop
    queue.start_processor(Duration::from_secs(2)).await;

    // 6. Enqueue a task!
    let task = RetryableTask::new(
        "unique-task-id-123".to_string(),
        "send_email".to_string(), // Matches your handler string
        r#"{"to": "john.wick@continental.com"}"#.to_string(), // JSON string payload
        3, // Max retries
        1.0, // Delay in hours for retries (e.g. 1.0 = wait 1 hour between failures)
    );

    queue.enqueue(task).unwrap();
    
    // Keep your app alive (or drop it into an Axum/Actix web framework!)
    tokio::time::sleep(Duration::from_secs(10)).await;
}
```

---

## 🧠 Architecture Details

`snerd-rust` utilizes an **Append-Only Log Model** to achieve massive write speeds.
Instead of updating rows in a database, every time a task is enqueued, updated, or deleted, a brand new JSON line is instantly appended to the end of the log file.

When the `SnerdQueue` wakes up on its polling interval, it scans the log, maps out the absolute latest state of every task, and spawns parallel Tokio tasks for anything that is currently due (`retry_after_time <= now`). 

If your file ever grows too large (default `20MB` or >10k operations), `snerd-rust` atomically clones, shrinks, and replaces the file in the background (Log Compaction) to keep disk space minimal.

---

## 🤝 License

MIT License. Do whatever you want with it, just don't let your tasks die unhandled.
