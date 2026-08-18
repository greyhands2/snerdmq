pub mod file_store;
pub mod protocol;
pub mod queue;
pub mod rate_limiter;
pub mod task;

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};
use std::fs::{self, OpenOptions};
use std::io::Write;
use fs3::FileExt;
use tokio::io::{AsyncBufReadExt, BufReader, stdin};
use tokio::sync::{RwLock, oneshot};

use crate::file_store::FileStore;
use crate::rate_limiter::RateLimiter;
type PendingExecutions = Arc<RwLock<HashMap<String, oneshot::Sender<Result<(), String>>>>>;
use crate::protocol::{IncomingMessage, OutgoingMessage};
use crate::queue::SnerdQueue;
use crate::task::RetryableTask;

/// Metrics counters for the daemon
struct Metrics {
    total_enqueued: AtomicU64,
    total_executed: AtomicU64,
    total_failed: AtomicU64,
    total_dlq: AtomicU64,
    start_time: Instant,
}

#[tokio::main]
async fn main() {
    let storage_dir_str = std::env::args()
        .nth(1)
        .unwrap_or_else(|| ".snerdata".to_string());

    // Create storage directory if it doesn't exist
    let storage_dir = std::path::PathBuf::from(&storage_dir_str);
    fs::create_dir_all(&storage_dir).expect("Failed to create storage directory");

    // Acquire exclusive lock file to prevent multiple daemons on same storage
    let lock_path = storage_dir.join(".lock");
    let lock_file = OpenOptions::new()
        .create(true)
        .write(true)
        .open(&lock_path)
        .expect("Failed to open lock file");

    if lock_file.try_lock_exclusive().is_err() {
        eprintln!("[Snerd] ERROR: Another daemon is already running on storage '{}'.", storage_dir_str);
        eprintln!("[Snerd] Lock file: {}", lock_path.display());
        std::process::exit(1);
    }

    // Write PID to lock file for debugging
    let pid = std::process::id();
    let mut lf = lock_file;
    lf.set_len(0).ok();
    lf.write_all(format!("{}", pid).as_bytes()).ok();
    lf.flush().ok();
    // lf is intentionally leaked (not dropped) to hold the lock for the process lifetime
    std::mem::forget(lf);

    let store_path = storage_dir.join("tasks").join("tasks.log");
    let file_store = FileStore::new(&store_path).unwrap();
    let rate_limiter = RateLimiter::new(&store_path);
    let queue = Arc::new(SnerdQueue::new("snerdmq-daemon", file_store, rate_limiter));
    queue.start_processor(Duration::from_secs(2)).await;

    let pending_executions: PendingExecutions = Arc::new(RwLock::new(HashMap::new()));
    let metrics = Arc::new(Metrics {
        total_enqueued: AtomicU64::new(0),
        total_executed: AtomicU64::new(0),
        total_failed: AtomicU64::new(0),
        total_dlq: AtomicU64::new(0),
        start_time: Instant::now(),
    });
    let stdin_stream = stdin();
    let mut reader = BufReader::new(stdin_stream).lines();

    while let Ok(Some(line)) = reader.next_line().await {
        if line.trim().is_empty() {
            continue;
        }

        let msg_res: Result<IncomingMessage, _> = serde_json::from_str(&line);
        match msg_res {
            Ok(IncomingMessage::Register { task_type }) => {
                let q_clone = queue.clone();
                let pending_clone = pending_executions.clone();
                let metrics_clone = metrics.clone();
                let t_type = task_type.clone();

                q_clone
                    .register_task_handler(&task_type, move |task: RetryableTask| {
                        let pending = pending_clone.clone();
                        let t_type = t_type.clone();
                        let met = metrics_clone.clone();
                        async move {
                            let (tx, rx) = oneshot::channel();
                            pending.write().await.insert(task.task_id.clone(), tx);
                            let out_msg = OutgoingMessage::Execute {
                                task_id: task.task_id.clone(),
                                task_type: t_type,
                                task_data: task.task_data.clone(),
                                max_execution_seconds: task.max_execution_seconds,
                            };
                            println!("{}", serde_json::to_string(&out_msg).unwrap());
                            
                            let rx_result = if let Some(secs) = task.max_execution_seconds {
                                match tokio::time::timeout(std::time::Duration::from_secs(secs), rx).await {
                                    Ok(res) => res,
                                    Err(_) => {
                                        pending.write().await.remove(&task.task_id);
                                        return Err(format!("Task execution timed out after {} seconds", secs));
                                    }
                                }
                            } else {
                                rx.await
                            };

                            match rx_result {
                                Ok(res) => {
                                    match &res {
                                        Ok(_) => { met.total_executed.fetch_add(1, Ordering::Relaxed); }
                                        Err(_) => { met.total_failed.fetch_add(1, Ordering::Relaxed); }
                                    }
                                    res
                                }
                                Err(e) => {
                                    met.total_failed.fetch_add(1, Ordering::Relaxed);
                                    Err(e.to_string())
                                }
                            }
                        }
                    })
                    .await;

                let t_type_dlq = task_type.clone();
                let metrics_dlq = metrics.clone();
                queue
                    .register_max_retry_handler(&task_type, move |task: RetryableTask| {
                        let t_type = t_type_dlq.clone();
                        let met = metrics_dlq.clone();
                        async move {
                            met.total_dlq.fetch_add(1, Ordering::Relaxed);
                            let out_msg = OutgoingMessage::MaxRetriesReached {
                                task_id: task.task_id.clone(),
                                task_type: t_type,
                                task_data: task.task_data.clone(),
                            };
                            println!("{}", serde_json::to_string(&out_msg).unwrap());
                            Ok(())
                        }
                    })
                    .await;

                println!(
                    "{}",
                    serde_json::to_string(&OutgoingMessage::Ack {
                        task_id: None, message: format!("Registered handler for {}", task_type)
                    })
                    .unwrap()
                );
            }

            Ok(IncomingMessage::Enqueue {
                task_id,
                task_type,
                task_data,
                max_retries,
                retry_after_hours,
                rate_limit_group,
                max_per_minute,
                auto_dedupe,
                urgency_score,
                execute_at,
                cron,
                webhook_url,
                max_execution_seconds,
            }) => {
                let t = RetryableTask::new(
                    task_id.clone(),
                    task_type.clone(),
                    task_data.clone(),
                    max_retries,
                    retry_after_hours,
                    rate_limit_group,
                    max_per_minute,
                    auto_dedupe,
                    urgency_score,
                    execute_at,
                    cron,
                    webhook_url,
                    max_execution_seconds,
                );
                if let Err(e) = queue.enqueue(t) {
                    println!(
                        "{}",
                        serde_json::to_string(&OutgoingMessage::Error {
                            task_id: Some(task_id.clone()), message: format!("Failed to enqueue: {}", e)
                        })
                        .unwrap()
                    );
                } else {
                    metrics.total_enqueued.fetch_add(1, Ordering::Relaxed);
                    println!(
                        "{}",
                        serde_json::to_string(&OutgoingMessage::Ack {
                            task_id: Some(task_id.clone()), message: "Enqueued successfully".to_string()
                        })
                        .unwrap()
                    );
                }
            }

            Ok(IncomingMessage::Progress { task_id, data }) => {
                let out_msg = OutgoingMessage::Progress { task_id, data };
                println!("{}", serde_json::to_string(&out_msg).unwrap());
            }

            Ok(IncomingMessage::Result {
                task_id,
                status,
                error_msg,
            }) => {
                if let Some(tx) = pending_executions.write().await.remove(&task_id) {
                    let res = if status == "success" {
                        Ok(())
                    } else {
                        Err(error_msg.unwrap_or_else(|| "Unknown error".to_string()))
                    };
                    let _ = tx.send(res);
                } else {
                    println!(
                        "{}",
                        serde_json::to_string(&OutgoingMessage::Error { task_id: None, message: format!(
                                "Received result for unknown/expired task_id {}",
                                task_id
                            )
                        })
                        .unwrap()
                    );
                }
            }

            Ok(IncomingMessage::Stats) => {
                let queue_depth = queue.file_store.read_tasks()
                    .map(|tasks| tasks.iter().filter(|t| t.deleted_at.is_none()).count())
                    .unwrap_or(0);
                println!(
                    "{}",
                    serde_json::to_string(&OutgoingMessage::Stats {
                        total_enqueued: metrics.total_enqueued.load(Ordering::Relaxed),
                        total_executed: metrics.total_executed.load(Ordering::Relaxed),
                        total_failed: metrics.total_failed.load(Ordering::Relaxed),
                        total_dlq: metrics.total_dlq.load(Ordering::Relaxed),
                        queue_depth,
                        uptime_secs: metrics.start_time.elapsed().as_secs(),
                    })
                    .unwrap()
                );
            }

            Err(e) => {
                println!(
                    "{}",
                    serde_json::to_string(&OutgoingMessage::Error { task_id: None, message: format!("Invalid JSON: {}", e)
                    })
                    .unwrap()
                );
            }
        }
    }
}
