use std::{thread::sleep, time::Duration};

use epoch_db::DB;
use tempfile::tempdir;

#[test]
fn test_ttl() {
    let temp_dir = tempdir().unwrap();

    let db = DB::new(temp_dir.path()).unwrap();

    db.set("user:1", "Alice", Some(Duration::new(5, 0)))
        .unwrap();

    assert_eq!("Alice", db.get("user:1").unwrap().unwrap());

    sleep(Duration::new(6, 0));

    assert_eq!(None, db.get("user:1").unwrap());
}

#[test]
fn test_ttl_update() {
    let temp_dir = tempdir().unwrap();
    let db = DB::new(temp_dir.path()).unwrap();

    db.set("user:update", "Alice", Some(Duration::from_secs(2)))
        .unwrap();

    db.set("user:update", "Alice V2", Some(Duration::from_secs(10)))
        .unwrap();

    sleep(Duration::from_secs(3));

    assert_eq!(
        "Alice V2",
        db.get("user:update").unwrap().unwrap(),
        "Key should not be deleted as its TTL was extended."
    );
}

#[test]
fn test_ttl_removal_to_permanent() {
    let temp_dir = tempdir().unwrap();
    let db = DB::new(temp_dir.path()).unwrap();

    db.set("user:permanent", "Bob", Some(Duration::from_secs(2)))
        .unwrap();

    db.set("user:permanent", "Bob The Permanent", None).unwrap();

    sleep(Duration::from_secs(3));

    assert_eq!(
        "Bob The Permanent",
        db.get("user:permanent").unwrap().unwrap(),
        "Key should persist after its original TTL was removed."
    );
}

#[test]
fn test_no_ttl_is_permanent() {
    let temp_dir = tempdir().unwrap();
    let db = DB::new(temp_dir.path()).unwrap();

    db.set("user:no_ttl", "Charlie", None).unwrap();

    sleep(Duration::from_secs(3));

    assert!(
        db.get("user:no_ttl").unwrap().is_some(),
        "Key set without a TTL should not be deleted."
    );
}

#[test]
fn test_manual_removal_of_ttl_key() {
    let temp_dir = tempdir().unwrap();
    let db = DB::new(temp_dir.path()).unwrap();

    db.set(
        "user:manual_delete",
        "David",
        Some(Duration::from_secs(120)),
    )
    .unwrap();

    assert!(db.get("user:manual_delete").unwrap().is_some());
    assert!(db.get_metadata("user:manual_delete").unwrap().is_some());

    db.remove("user:manual_delete").unwrap();

    assert!(
        db.get("user:manual_delete").unwrap().is_none(),
        "Key should be gone after manual remove."
    );
    assert!(
        db.get_metadata("user:manual_delete").unwrap().is_none(),
        "Metadata should be gone after manual remove."
    );
}
