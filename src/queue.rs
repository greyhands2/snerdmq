use chrono::Utc;
use std::collections::{HashMap, HashSet, BinaryHeap};
use tokio::sync::Semaphore;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::RwLock;

use crate::file_store::FileStore;
use crate::task::{RetryableTask, PriorityTask};
use crate::rate_limiter::RateLimiter;
use serde_json::json;

use std::future::Future;
use std::pin::Pin;

pub type TaskHandler = Arc<
    dyn Fn(RetryableTask) -> Pin<Box<dyn Future<Output = Result<(), String>> + Send>> + Send + Sync,
>;
pub type MaxRetryHandler = Arc<
    dyn Fn(RetryableTask) -> Pin<Box<dyn Future<Output = Result<(), String>> + Send>> + Send + Sync,
>;

struct ExecutingGuard {
    executing_tasks: Arc<Mutex<HashSet<String>>>,
    task_id: String,
}

impl Drop for ExecutingGuard {
    fn drop(&mut self) {
        if let Ok(mut executing) = self.executing_tasks.lock() {
            executing.remove(&self.task_id);
        }
    }
}

#[derive(Clone)]
pub struct SnerdQueue {
    pub name: String,
    pub file_store: FileStore,
    pub rate_limiter: RateLimiter,
    task_handlers: Arc<RwLock<HashMap<String, TaskHandler>>>,
    max_retry_handlers: Arc<RwLock<HashMap<String, MaxRetryHandler>>>,
    active_hashes: Arc<Mutex<HashSet<String>>>,
    executing_tasks: Arc<Mutex<HashSet<String>>>,
    worker_semaphore: Arc<Semaphore>,
}

impl SnerdQueue {
    pub fn new(name: &str, file_store: FileStore, rate_limiter: RateLimiter) -> Self {
        let mut initial_hashes = HashSet::new();
        if let Ok(tasks) = file_store.read_tasks() {
            for task in tasks {
                if task.deleted_at.is_none() {
                    if let Some(hash) = task.payload_hash {
                        initial_hashes.insert(hash);
                    }
                }
            }
        }
        
        Self {
            name: name.to_string(),
            file_store,
            rate_limiter,
            task_handlers: Arc::new(RwLock::new(HashMap::new())),
            max_retry_handlers: Arc::new(RwLock::new(HashMap::new())),
            active_hashes: Arc::new(Mutex::new(initial_hashes)),
            executing_tasks: Arc::new(Mutex::new(HashSet::new())),
            worker_semaphore: Arc::new(Semaphore::new(100)), // Limit to 100 concurrent tasks
        }
    }

    pub async fn register_task_handler<F, Fut>(&self, task_type: &str, handler: F)
    where
        F: Fn(RetryableTask) -> Fut + Send + Sync + 'static,
        Fut: std::future::Future<Output = Result<(), String>> + Send + 'static,
    {
        self.task_handlers.write().await.insert(
            task_type.to_string(),
            Arc::new(move |task| Box::pin(handler(task))),
        );
    }

    pub async fn register_max_retry_handler<F, Fut>(&self, task_type: &str, handler: F)
    where
        F: Fn(RetryableTask) -> Fut + Send + Sync + 'static,
        Fut: std::future::Future<Output = Result<(), String>> + Send + 'static,
    {
        self.max_retry_handlers.write().await.insert(
            task_type.to_string(),
            Arc::new(move |task| Box::pin(handler(task))),
        );
    }

    pub fn enqueue(&self, mut task: RetryableTask) -> std::io::Result<()> {
        if let Some(ref hash) = task.payload_hash {
            if let Ok(mut hashes) = self.active_hashes.lock() {
                if hashes.contains(hash) {
                    // Duplicate found, drop silently
                    return Ok(());
                }
                hashes.insert(hash.clone());
            }
        }
        task.deleted_at = None;
        self.file_store.save_task(&task)?;

        if task.execute_at <= Utc::now() && task.retry_after_time <= Utc::now() {
            if let Some(ref group) = task.rate_limit_group {
                if let Some(limit) = task.max_per_minute {
                    match self.rate_limiter.check_and_increment(group, limit) {
                        Ok(true) => {}
                        Ok(false) | Err(_) => {
                            task.retry_after_time = Utc::now() + chrono::Duration::seconds(60);
                            let _ = self.file_store.save_task(&task);
                            return Ok(());
                        }
                    }
                }
            }
            
            // Try to acquire a permit for immediate execution
            if let Ok(permit) = self.worker_semaphore.clone().try_acquire_owned() {
                // Lock check before executing
                if let Ok(mut executing) = self.executing_tasks.lock() {
                    if executing.contains(&task.task_id) {
                        return Ok(());
                    }
                    executing.insert(task.task_id.clone());
                }
                
                let q = self.clone();
                tokio::spawn(async move {
                    let _p = permit; // Hold permit until execution finishes
                    q.execute_task(task).await;
                });
            }
        }
        Ok(())
    }

    pub async fn start_processor(&self, interval: Duration) {
        let q = self.clone();
        tokio::spawn(async move {
            let mut interval_timer = tokio::time::interval(interval);
            loop {
                interval_timer.tick().await;
                q.process_due_tasks().await;
            }
        });
    }

    pub async fn process_due_tasks(&self) {
        let tasks = match self.file_store.read_tasks() {
            Ok(t) => t,
            Err(_) => return,
        };

        let now = Utc::now();
        let mut heap = BinaryHeap::new();
        
        for task in tasks {
            if task.execute_at <= now && task.retry_after_time <= now && task.deleted_at.is_none() {
                heap.push(PriorityTask(task));
            }
        }

        let available = self.worker_semaphore.available_permits();
        for _ in 0..available {
            if let Some(PriorityTask(mut task)) = heap.pop() {
                if let Some(ref group) = task.rate_limit_group {
                    if let Some(limit) = task.max_per_minute {
                        match self.rate_limiter.check_and_increment(group, limit) {
                            Ok(true) => {}
                            Ok(false) | Err(_) => {
                                task.retry_after_time = now + chrono::Duration::seconds(60);
                                let _ = self.file_store.save_task(&task);
                                continue;
                            }
                        }
                    }
                }

                // Lock check before executing
                if let Ok(mut executing) = self.executing_tasks.lock() {
                    if executing.contains(&task.task_id) {
                        continue;
                    }
                    executing.insert(task.task_id.clone());
                }

                if let Ok(permit) = self.worker_semaphore.clone().try_acquire_owned() {
                    let q = self.clone();
                    tokio::spawn(async move {
                        let _p = permit;
                        q.execute_task(task).await;
                    });
                }
            } else {
                break;
            }
        }
    }

    async fn execute_task(&self, mut task: RetryableTask) {
        // Drop guard guarantees removal from executing_tasks
        let _guard = ExecutingGuard {
            executing_tasks: Arc::clone(&self.executing_tasks),
            task_id: task.task_id.clone(),
        };

        // If webhook_url is set, dispatch via HTTP instead of local handler
        let result: Result<(), String> = if let Some(ref url) = task.webhook_url.clone() {
            let payload = json!({
                "taskId": task.task_id,
                "taskType": task.task_type,
                "data": task.task_data,
            });
            let url = url.clone();
            match reqwest::Client::new()
                .post(&url)
                .header("Content-Type", "application/json")
                .header("X-SnerdMQ-Event", "Execute")
                .json(&payload)
                .send()
                .await
            {
                Ok(resp) if resp.status().is_success() => Ok(()),
                Ok(resp) => Err(format!("Webhook returned non-2xx status: {}", resp.status())),
                Err(e) => Err(format!("Webhook request failed: {}", e)),
            }
        } else {
            let handler = {
                let handlers = self.task_handlers.read().await;
                handlers.get(&task.task_type).cloned()
            };
            if let Some(h) = handler {
                h(task.clone()).await
            } else {
                return;
            }
        };

        match result {
            Ok(_) => {
                let mut rescheduled = false;
                if let Some(ref cron_expr) = task.cron_expression {
                    use cron::Schedule;
                    use std::str::FromStr;
                    if let Ok(schedule) = Schedule::from_str(cron_expr) {
                        if let Some(next) = schedule.upcoming(Utc).next() {
                            task.execute_at = next;
                            task.retry_count = 0;
                            task.last_error_obj = None;
                            task.last_job_error = None;
                            let _ = self.file_store.save_task(&task);
                            rescheduled = true;
                        }
                    }
                }

                if !rescheduled {
                    let _ = self.file_store.delete_task(&task.task_id);
                    if let Some(ref hash) = task.payload_hash {
                        if let Ok(mut hashes) = self.active_hashes.lock() {
                            hashes.remove(hash);
                        }
                    }
                }
            }
            Err(e) => {
                if task.retry_count < task.max_retries {
                    task.update_retry_config(Some(e));
                    let _ = self.file_store.save_task(&task);
                } else {
                    // Max retries reached — fire DLQ webhook or local max retry handler
                    if let Some(ref url) = task.webhook_url.clone() {
                        let payload = json!({
                            "taskId": task.task_id,
                            "taskType": task.task_type,
                            "data": task.task_data,
                        });
                        let url = url.clone();
                        tokio::spawn(async move {
                            let _ = reqwest::Client::new()
                                .post(&url)
                                .header("Content-Type", "application/json")
                                .header("X-SnerdMQ-Event", "MaxRetriesReached")
                                .json(&payload)
                                .send()
                                .await;
                        });
                    } else {
                        let max_handler = {
                            let max_handlers = self.max_retry_handlers.read().await;
                            max_handlers.get(&task.task_type).cloned()
                        };
                        if let Some(mh) = max_handler {
                            let _ = mh(task.clone()).await;
                        }
                    }

                    let _ = self.file_store.delete_task(&task.task_id);
                    if let Some(ref hash) = task.payload_hash {
                        if let Ok(mut hashes) = self.active_hashes.lock() {
                            hashes.remove(hash);
                        }
                    }
                }
            }
        }
    }
}
