use std::{
    sync::Arc,
    thread::{self, sleep},
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use epoch_db::DB;
use tempfile::tempdir;

#[test]
fn test_set() {
    let temp_dir = tempdir().unwrap();

    let db = DB::new(temp_dir.path()).unwrap();

    db.set("user:1", "Alice", None).unwrap();

    assert_eq!("Alice", db.get("user:1").unwrap().unwrap());
}

#[test]
fn test_rm() {
    let temp_dir = tempdir().unwrap();

    let db = DB::new(temp_dir.path()).unwrap();

    db.set("user:1", "Alice", None).unwrap();

    assert_eq!("Alice", db.get("user:1").unwrap().unwrap());

    db.remove("user:1").unwrap();

    let _ = db.get("user:1").unwrap().is_none();
}

#[test]
fn test_get_metadata() {
    let temp_dir = tempdir().unwrap();

    let db = DB::new(temp_dir.path()).unwrap();

    db.set("user:1", "Alice", None).unwrap();

    assert_eq!("Alice", db.get("user:1").unwrap().unwrap());

    db.increment_frequency("user:1").unwrap();

    let meta = db.get_metadata("user:1").unwrap().unwrap();

    assert_eq!(meta.freq, 1);

    sleep(Duration::new(1, 100));

    assert!(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs()
            > meta.created_at
    )
}

#[test]
fn test_concurrent_increment() {
    let temp_dir = tempdir().unwrap();
    let db = Arc::new(DB::new(temp_dir.path()).unwrap());

    let key = "concurrent_key";
    let value = "test_value";

    db.set(key, value, None).unwrap();

    let mut handles = vec![];
    let num_threads = 10;
    let increments_per_thread = 100;

    for _ in 0..num_threads {
        let db_clone = Arc::clone(&db);
        let handle = thread::spawn(move || {
            for _ in 0..increments_per_thread {
                db_clone.increment_frequency(key).unwrap();
            }
        });
        handles.push(handle);
    }

    for handle in handles {
        handle.join().unwrap();
    }

    let expected_freq = num_threads * increments_per_thread;

    let final_meta = db.get_metadata(key).unwrap().unwrap();
    assert_eq!(
        final_meta.freq, expected_freq,
        "Frequency count should be accurate after concurrent increments."
    );
}

#[test]
fn test_data_integrity_on_update() {
    let temp_dir = tempdir().unwrap();
    let db = DB::new(temp_dir.path()).unwrap();

    let key = "user:integrity";

    db.set(key, "Version 1", None).unwrap();
    let initial_meta = db.get_metadata(key).unwrap().unwrap();
    assert_eq!(initial_meta.freq, 0);

    db.increment_frequency(key).unwrap();
    let incremented_meta = db.get_metadata(key).unwrap().unwrap();
    assert_eq!(incremented_meta.freq, 1);
    assert_eq!(
        initial_meta.created_at, incremented_meta.created_at,
        "created_at timestamp should not change on increment."
    );

    db.set(key, "Version 2", None).unwrap();

    let final_meta = db.get_metadata(key).unwrap().unwrap();
    let final_value = db.get(key).unwrap().unwrap();

    assert_eq!(final_value, "Version 2", "Value should be updated.");
    assert_eq!(
        final_meta.freq, 1,
        "Frequency should not be reset on a value update."
    );
    assert_eq!(
        final_meta.created_at, initial_meta.created_at,
        "created_at timestamp should not change on a value update."
    );
}
