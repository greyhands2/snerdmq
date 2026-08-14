use chrono::Utc;
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::RwLock;

use crate::file_store::FileStore;
use crate::task::RetryableTask;
use crate::rate_limiter::RateLimiter;

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

        if task.retry_after_time <= Utc::now() {
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
            
            // Lock check before executing
            if let Ok(mut executing) = self.executing_tasks.lock() {
                if executing.contains(&task.task_id) {
                    return Ok(());
                }
                executing.insert(task.task_id.clone());
            }
            
            let q = self.clone();
            tokio::spawn(async move {
                q.execute_task(task).await;
            });
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
        for mut task in tasks {
            if task.retry_after_time <= now && task.deleted_at.is_none() {
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

                let q = self.clone();
                tokio::spawn(async move {
                    q.execute_task(task).await;
                });
            }
        }
    }

    async fn execute_task(&self, mut task: RetryableTask) {
        // Drop guard guarantees removal from executing_tasks
        let _guard = ExecutingGuard {
            executing_tasks: Arc::clone(&self.executing_tasks),
            task_id: task.task_id.clone(),
        };

        let handler = {
            let handlers = self.task_handlers.read().await;
            handlers.get(&task.task_type).cloned()
        };

        if let Some(h) = handler {
            let result = h(task.clone()).await;

            match result {
                Ok(_) => {
                    let _ = self.file_store.delete_task(&task.task_id);
                    if let Some(ref hash) = task.payload_hash {
                        if let Ok(mut hashes) = self.active_hashes.lock() {
                            hashes.remove(hash);
                        }
                    }
                }
                Err(e) => {
                    if task.retry_count < task.max_retries {
                        task.update_retry_config(Some(e));
                        let _ = self.file_store.save_task(&task);
                    } else {
                        // Max retries reached
                        let max_handler = {
                            let max_handlers = self.max_retry_handlers.read().await;
                            max_handlers.get(&task.task_type).cloned()
                        };

                        if let Some(mh) = max_handler {
                            let _ = mh(task.clone()).await;
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
}
