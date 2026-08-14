pub mod file_store;
pub mod protocol;
pub mod queue;
pub mod rate_limiter;
pub mod task;

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncBufReadExt, BufReader, stdin};
use tokio::sync::{RwLock, oneshot};

use crate::file_store::FileStore;
use crate::rate_limiter::RateLimiter;
type PendingExecutions = Arc<RwLock<HashMap<String, oneshot::Sender<Result<(), String>>>>>;
use crate::protocol::{IncomingMessage, OutgoingMessage};
use crate::queue::SnerdQueue;
use crate::task::RetryableTask;

#[tokio::main]
async fn main() {
    let store_path_str = std::env::args()
        .nth(1)
        .unwrap_or_else(|| ".snerdata/tasks/tasks.log".to_string());
    let store_path = std::path::PathBuf::from(&store_path_str);
    let file_store = FileStore::new(&store_path).unwrap();
    let rate_limiter = RateLimiter::new(&store_path);
    let queue = Arc::new(SnerdQueue::new("snerdmq-daemon", file_store, rate_limiter));
    queue.start_processor(Duration::from_secs(2)).await;

    let pending_executions: PendingExecutions = Arc::new(RwLock::new(HashMap::new()));
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
                let t_type = task_type.clone();

                q_clone
                    .register_task_handler(&task_type, move |task: RetryableTask| {
                        let pending = pending_clone.clone();
                        let t_type = t_type.clone();
                        async move {
                            let (tx, rx) = oneshot::channel();
                            pending.write().await.insert(task.task_id.clone(), tx);
                            let out_msg = OutgoingMessage::Execute {
                                task_id: task.task_id.clone(),
                                task_type: t_type,
                                task_data: task.task_data.clone(),
                            };
                            println!("{}", serde_json::to_string(&out_msg).unwrap());
                            match rx.await {
                                Ok(res) => res,
                                Err(_) => Err("Client disconnected before responding".to_string()),
                            }
                        }
                    })
                    .await;

                let t_type_dlq = task_type.clone();
                queue
                    .register_max_retry_handler(&task_type, move |task: RetryableTask| {
                        let t_type = t_type_dlq.clone();
                        async move {
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
                    println!(
                        "{}",
                        serde_json::to_string(&OutgoingMessage::Ack {
                            task_id: Some(task_id.clone()), message: "Enqueued successfully".to_string()
                        })
                        .unwrap()
                    );
                }
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
