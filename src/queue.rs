use chrono::Utc;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::RwLock;

use crate::file_store::FileStore;
use crate::task::RetryableTask;

use std::future::Future;
use std::pin::Pin;

pub type TaskHandler = Arc<
    dyn Fn(RetryableTask) -> Pin<Box<dyn Future<Output = Result<(), String>> + Send>> + Send + Sync,
>;
pub type MaxRetryHandler = Arc<
    dyn Fn(RetryableTask) -> Pin<Box<dyn Future<Output = Result<(), String>> + Send>> + Send + Sync,
>;

#[derive(Clone)]
pub struct SnerdQueue {
    pub name: String,
    pub file_store: FileStore,
    task_handlers: Arc<RwLock<HashMap<String, TaskHandler>>>,
    max_retry_handlers: Arc<RwLock<HashMap<String, MaxRetryHandler>>>,
}

impl SnerdQueue {
    pub fn new(name: &str, file_store: FileStore) -> Self {
        Self {
            name: name.to_string(),
            file_store,
            task_handlers: Arc::new(RwLock::new(HashMap::new())),
            max_retry_handlers: Arc::new(RwLock::new(HashMap::new())),
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
        task.deleted_at = None;
        self.file_store.save_task(&task)?;

        if task.retry_after_time <= Utc::now() {
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
        for task in tasks {
            if task.retry_after_time <= now && task.deleted_at.is_none() {
                let q = self.clone();
                tokio::spawn(async move {
                    q.execute_task(task).await;
                });
            }
        }
    }

    async fn execute_task(&self, mut task: RetryableTask) {
        let handler = {
            let handlers = self.task_handlers.read().await;
            handlers.get(&task.task_type).cloned()
        };

        if let Some(h) = handler {
            let result = h(task.clone()).await;

            match result {
                Ok(_) => {
                    let _ = self.file_store.delete_task(&task.task_id);
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
                    }
                }
            }
        }
    }
}
