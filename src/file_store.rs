use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io::{BufRead, BufReader, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, RwLock};

use crate::task::RetryableTask;

#[derive(Clone)]
pub struct FileStore {
    file_path: Arc<PathBuf>,
    total_tasks: Arc<Mutex<usize>>,
    deleted_tasks: Arc<Mutex<usize>>,
    append_count: Arc<Mutex<usize>>,
    compacting: Arc<AtomicBool>,
    file_lock: Arc<RwLock<()>>,
    tasks_cache: Arc<RwLock<HashMap<String, RetryableTask>>>,
}

impl FileStore {
    pub fn new<P: AsRef<Path>>(path: P) -> std::io::Result<Self> {
        let fs = FileStore {
            file_path: Arc::new(path.as_ref().to_path_buf()),
            total_tasks: Arc::new(Mutex::new(0)),
            deleted_tasks: Arc::new(Mutex::new(0)),
            append_count: Arc::new(Mutex::new(0)),
            compacting: Arc::new(AtomicBool::new(false)),
            file_lock: Arc::new(RwLock::new(())),
            tasks_cache: Arc::new(RwLock::new(HashMap::new())),
        };
        fs.rebuild_metadata()?;
        Ok(fs)
    }

    fn rebuild_metadata(&self) -> std::io::Result<()> {
        let _lock = self.file_lock.write().unwrap();
        let mut cache = self.tasks_cache.write().unwrap();
        cache.clear();

        if !self.file_path.exists() {
            return Ok(());
        }

        let file = File::open(self.file_path.as_ref())?;

        let mut total = 0;
        let mut deleted = 0;
        let mut appended = 0;

        let reader = BufReader::new(&file);
        for line_str in reader.lines().map_while(Result::ok) {
            if line_str.trim().is_empty() {
                continue;
            }
            if let Ok(task) = serde_json::from_str::<RetryableTask>(&line_str) {
                appended += 1;
                if task.deleted_at.is_some() {
                    deleted += 1;
                    cache.remove(&task.task_id);
                } else {
                    total += 1;
                    cache.insert(task.task_id.clone(), task);
                }
            }
        }

        *self.total_tasks.lock().unwrap() = total;
        *self.deleted_tasks.lock().unwrap() = deleted;
        *self.append_count.lock().unwrap() = appended;

        Ok(())
    }

    pub fn save_task(&self, task: &RetryableTask) -> std::io::Result<()> {
        let _lock = self.file_lock.write().unwrap();

        if let Some(parent) = self.file_path.parent() {
            std::fs::create_dir_all(parent)?;
        }

        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(self.file_path.as_ref())?;

        let json_str = serde_json::to_string(task)?;
        writeln!(file, "{}", json_str)?;
        file.sync_all()?;

        let is_deleted = task.deleted_at.is_some();
        
        {
            let mut cache = self.tasks_cache.write().unwrap();
            if is_deleted {
                cache.remove(&task.task_id);
            } else {
                cache.insert(task.task_id.clone(), task.clone());
            }
        }

        {
            *self.append_count.lock().unwrap() += 1;
            if is_deleted {
                *self.deleted_tasks.lock().unwrap() += 1;
            } else {
                *self.total_tasks.lock().unwrap() += 1;
            }
        }

        if self.should_compact() {
            let fs_clone = self.clone();
            tokio::spawn(async move {
                let _ = fs_clone.compact_log();
            });
        }

        Ok(())
    }

    pub fn read_tasks(&self) -> std::io::Result<Vec<RetryableTask>> {
        let cache = self.tasks_cache.read().unwrap();
        Ok(cache.values().cloned().collect())
    }

    pub fn get_latest_task(&self, task_id: &str) -> std::io::Result<Option<RetryableTask>> {
        let cache = self.tasks_cache.read().unwrap();
        Ok(cache.get(task_id).cloned())
    }

    pub fn delete_task(&self, task_id: &str) -> std::io::Result<()> {
        let mut task_opt = None;
        {
            let cache = self.tasks_cache.read().unwrap();
            if let Some(task) = cache.get(task_id) {
                if task.deleted_at.is_none() {
                    task_opt = Some(task.clone());
                }
            }
        }
        
        if let Some(mut task) = task_opt {
            task.mark_deleted();
            self.save_task(&task)?;
        }
        Ok(())
    }

    fn should_compact(&self) -> bool {
        if let Ok(metadata) = std::fs::metadata(self.file_path.as_ref()) {
            if metadata.len() > 20 * 1024 * 1024 {
                return true;
            }
        }

        let total = *self.total_tasks.lock().unwrap();
        let deleted = *self.deleted_tasks.lock().unwrap();
        if total > 0 && (deleted as f64 / total as f64) > 0.5 {
            return true;
        }

        if *self.append_count.lock().unwrap() >= 10000 {
            return true;
        }

        false
    }

    pub fn compact_log(&self) -> std::io::Result<()> {
        if self
            .compacting
            .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
            .is_err()
        {
            return Ok(()); // Already compacting
        }

        let temp_path = self.file_path.with_extension("tmp");

        let result = (|| -> std::io::Result<()> {
            let _lock = self.file_lock.write().unwrap();
            
            let input_file = File::open(self.file_path.as_ref())?;

            let mut temp_file = OpenOptions::new()
                .create(true)
                .write(true)
                .truncate(true)
                .open(&temp_path)?;

            let mut task_map = HashMap::new();
            let mut _total_tasks = 0;
            let mut _deleted_count = 0;

            let reader = BufReader::new(&input_file);
            for line_str in reader.lines().map_while(Result::ok) {
                if line_str.trim().is_empty() {
                    continue;
                }
                _total_tasks += 1;
                if let Ok(task) = serde_json::from_str::<RetryableTask>(&line_str) {
                    if task.deleted_at.is_some() {
                        _deleted_count += 1;
                        task_map.remove(&task.task_id);
                    } else if let Some(existing) = task_map.get(&task.task_id) {
                        let existing_task: &RetryableTask = existing;
                        if task.updated_at > existing_task.updated_at {
                            task_map.insert(task.task_id.clone(), task);
                        }
                    } else {
                        task_map.insert(task.task_id.clone(), task);
                    }
                }
            }

            for task in task_map.values() {
                let json_str = serde_json::to_string(task)?;
                writeln!(temp_file, "{}", json_str)?;
            }

            temp_file.sync_all()?;
            std::fs::rename(&temp_path, self.file_path.as_ref())?;

            *self.total_tasks.lock().unwrap() = task_map.len();
            *self.deleted_tasks.lock().unwrap() = 0;
            *self.append_count.lock().unwrap() = 0;

            // Update memory cache
            let mut cache = self.tasks_cache.write().unwrap();
            cache.clear();
            for (id, t) in task_map.into_iter() {
                cache.insert(id, t);
            }

            Ok(())
        })();

        self.compacting.store(false, Ordering::SeqCst);
        result
    }
}
