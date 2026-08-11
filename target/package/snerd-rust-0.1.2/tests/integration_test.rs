use snerd_rust::file_store::FileStore;
use snerd_rust::queue::SnerdQueue;
use snerd_rust::task::RetryableTask;
use std::io::Write;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

#[tokio::test]
async fn test_file_store_lifecycle() {
    let temp_dir = tempfile::tempdir().unwrap();
    let file_path = temp_dir.path().join("tasks.log");

    let store = FileStore::new(&file_path).unwrap();

    let task = RetryableTask::new(
        "task-1".to_string(),
        "email".to_string(),
        r#"{"to": "test@example.com"}"#.to_string(),
        3,
        0.0,
    );

    // Test Save
    store.save_task(&task).unwrap();
    let tasks = store.read_tasks().unwrap();
    assert_eq!(tasks.len(), 1);
    assert_eq!(tasks[0].task_id, "task-1");

    // Test Update / Retry
    let mut task_from_store = store.get_latest_task("task-1").unwrap().unwrap();
    task_from_store.update_retry_config(Some("network error".to_string()));
    store.save_task(&task_from_store).unwrap();

    let updated_task = store.get_latest_task("task-1").unwrap().unwrap();
    assert_eq!(updated_task.retry_count, 1);
    assert!(updated_task.last_job_error.is_some());

    // Test Delete
    store.delete_task("task-1").unwrap();
    let tasks_after_delete = store.read_tasks().unwrap();
    assert_eq!(tasks_after_delete.len(), 0);

    // Test Compaction
    store.compact_log().unwrap();
    let file_content = std::fs::read_to_string(&file_path).unwrap();
    assert_eq!(file_content.trim(), ""); // Should be empty after compaction
}

#[tokio::test]
async fn test_queue_execution() {
    let temp_dir = tempfile::tempdir().unwrap();
    let file_path = temp_dir.path().join("queue_tasks.log");

    let store = FileStore::new(&file_path).unwrap();
    let queue = SnerdQueue::new("test-queue", store);

    let exec_counter = Arc::new(AtomicUsize::new(0));
    let exec_counter_clone = exec_counter.clone();

    queue
        .register_task_handler("math-task", move |_data| {
            exec_counter_clone.fetch_add(1, Ordering::SeqCst);
            Ok(())
        })
        .await;

    let task = RetryableTask::new(
        "task-2".to_string(),
        "math-task".to_string(),
        r#"{"val": 1}"#.to_string(),
        3,
        0.0, // Retry after 0 hours (immediate)
    );

    queue.enqueue(task).unwrap();

    // Give tokio time to process the immediately spawned task
    tokio::time::sleep(Duration::from_millis(200)).await;

    assert_eq!(exec_counter.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn test_queue_retry_and_max() {
    let temp_dir = tempfile::tempdir().unwrap();
    let file_path = temp_dir.path().join("retry_tasks.log");

    let store = FileStore::new(&file_path).unwrap();
    let queue = SnerdQueue::new("retry-queue", store.clone());

    let exec_counter = Arc::new(AtomicUsize::new(0));
    let exec_counter_clone = exec_counter.clone();

    let max_counter = Arc::new(AtomicUsize::new(0));
    let max_counter_clone = max_counter.clone();

    queue
        .register_task_handler("fail-task", move |_data| {
            exec_counter_clone.fetch_add(1, Ordering::SeqCst);
            Err("intentional failure".to_string())
        })
        .await;

    queue
        .register_max_retry_handler("fail-task", move |_data| {
            max_counter_clone.fetch_add(1, Ordering::SeqCst);
            Ok(())
        })
        .await;

    let task = RetryableTask::new(
        "task-3".to_string(),
        "fail-task".to_string(),
        r#"{}"#.to_string(),
        2,   // Max retries = 2
        0.0, // Retry after 0 hours
    );

    queue.enqueue(task).unwrap();

    // Process loop:
    // Initial execution fails, schedules retry 1
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Trigger Retry 1
    queue.process_due_tasks().await;
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Trigger Retry 2 (max retries reached)
    queue.process_due_tasks().await;
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Check results
    // Initial + 2 retries = 3 executions total
    assert_eq!(exec_counter.load(Ordering::SeqCst), 3);

    // Should have triggered max handler exactly once
    assert_eq!(max_counter.load(Ordering::SeqCst), 1);

    // Ensure the task was deleted after max retries
    let tasks = store.read_tasks().unwrap();
    assert_eq!(tasks.len(), 0);
}

// ---- NEW ADVANCED EDGE CASE TESTS ----

#[tokio::test]
async fn test_concurrent_writes() {
    // Tests that OS-level file locking prevents corruption when 100 async tasks write simultaneously
    let temp_dir = tempfile::tempdir().unwrap();
    let file_path = temp_dir.path().join("concurrent_tasks.log");
    let store = Arc::new(FileStore::new(&file_path).unwrap());

    let mut handles = vec![];
    for i in 0..100 {
        let store_clone = store.clone();
        handles.push(tokio::spawn(async move {
            let task = RetryableTask::new(
                format!("task-{}", i),
                "concurrent-test".to_string(),
                "{}".to_string(),
                1,
                0.0,
            );
            store_clone.save_task(&task).unwrap();
        }));
    }

    // Wait for all 100 writes to finish
    for handle in handles {
        handle.await.unwrap();
    }

    // Read back and ensure exactly 100 distinct tasks exist and JSON is uncorrupted
    let tasks = store.read_tasks().unwrap();
    assert_eq!(tasks.len(), 100);
}

#[tokio::test]
async fn test_corrupted_file_recovery() {
    // Tests that if the log file has corrupted JSON injected, it gracefully skips it
    let temp_dir = tempfile::tempdir().unwrap();
    let file_path = temp_dir.path().join("corrupted.log");

    // Write corrupted data manually
    {
        let mut file = std::fs::File::create(&file_path).unwrap();
        writeln!(file, "{{ invalid json string that got cut off").unwrap();
    }

    // Open the store
    let store = FileStore::new(&file_path).unwrap();

    // Write a valid task AFTER the corruption
    let task = RetryableTask::new(
        "valid-task".to_string(),
        "test".to_string(),
        "{}".to_string(),
        1,
        0.0,
    );
    store.save_task(&task).unwrap();

    // It should skip the corrupted line and successfully read the valid task
    let tasks = store.read_tasks().unwrap();
    assert_eq!(tasks.len(), 1);
    assert_eq!(tasks[0].task_id, "valid-task");
}

#[tokio::test]
async fn test_delayed_execution() {
    // Tests that a task with retryAfterHours > 0 is NOT executed immediately
    let temp_dir = tempfile::tempdir().unwrap();
    let file_path = temp_dir.path().join("delayed.log");
    let store = FileStore::new(&file_path).unwrap();
    let queue = SnerdQueue::new("delayed-queue", store);

    let exec_counter = Arc::new(AtomicUsize::new(0));
    let exec_counter_clone = exec_counter.clone();

    queue
        .register_task_handler("delayed-task", move |_data| {
            exec_counter_clone.fetch_add(1, Ordering::SeqCst);
            Ok(())
        })
        .await;

    let mut task = RetryableTask::new(
        "task-future".to_string(),
        "delayed-task".to_string(),
        "{}".to_string(),
        3,
        1.0, // Delay by 1 hour
    );

    // Artificially set the retry time to the future, as new tasks execute immediately by default
    task.retry_after_time = chrono::Utc::now() + chrono::Duration::hours(1);

    // Enqueue the future task
    queue.enqueue(task).unwrap();

    // Wait for the tokio loop
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Execute Process due tasks (which shouldn't pick it up because it's an hour in the future)
    queue.process_due_tasks().await;
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Ensure it was NEVER executed
    assert_eq!(exec_counter.load(Ordering::SeqCst), 0);
}
