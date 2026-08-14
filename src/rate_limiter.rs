use chrono::{DateTime, Utc};
use fs3::FileExt;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs::OpenOptions;
use std::io::{BufRead, BufReader, Seek, SeekFrom, Write};
use std::path::PathBuf;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct RateWindow {
    pub count: i32,
    pub window_start: DateTime<Utc>,
}

#[derive(Clone)]
pub struct RateLimiter {
    file_path: PathBuf,
}

impl RateLimiter {
    pub fn new(tasks_path: &PathBuf) -> Self {
        let mut path = tasks_path.clone();
        path.set_file_name("rate_limits.json");
        Self { file_path: path }
    }

    pub fn check_and_increment(&self, group: &str, limit: i32) -> std::io::Result<bool> {
        if let Some(parent) = self.file_path.parent() {
            std::fs::create_dir_all(parent)?;
        }

        let mut file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .open(&self.file_path)?;

        file.lock_exclusive()?;

        let mut limits: HashMap<String, RateWindow> = HashMap::new();
        {
            let mut reader = BufReader::new(&file);
            let mut line = String::new();
            if let Ok(bytes_read) = reader.read_line(&mut line) {
                if bytes_read > 0 {
                    if let Ok(parsed) = serde_json::from_str(&line) {
                        limits = parsed;
                    }
                }
            }
        }

        let now = Utc::now();
        let mut allow = false;

        let window = limits.entry(group.to_string()).or_insert(RateWindow {
            count: 0,
            window_start: now,
        });

        if (now - window.window_start).num_seconds() >= 60 {
            window.count = 0;
            window.window_start = now;
        }

        if window.count < limit {
            window.count += 1;
            allow = true;
        }

        file.set_len(0)?;
        file.seek(SeekFrom::Start(0))?;
        let out = serde_json::to_string(&limits)?;
        writeln!(file, "{}", out)?;
        file.sync_all()?;

        file.unlock()?;
        Ok(allow)
    }
}
