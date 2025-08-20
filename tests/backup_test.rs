use std::thread::sleep;
use std::time::{
    Duration,
    SystemTime,
    UNIX_EPOCH
};

use epoch_db::DB;
use tempfile::tempdir;

#[test]
fn test_backup_get_key() {
    let temp_dir = tempdir().unwrap();
    let backup = tempdir().unwrap();
    let backup_path = backup.path();

    let db = DB::new(temp_dir.path()).unwrap();

    db.set("user:1", "Alice", None).unwrap();

    assert_eq!("Alice", db.get("user:1").unwrap().unwrap());

    db.backup_to(backup_path).unwrap();

    drop(db);

    temp_dir.close().unwrap();

    let temp_dir = tempdir().unwrap();

    let file = backup_path.read_dir().unwrap().next().unwrap().unwrap();

    let db = DB::load_from(&file.path(), temp_dir.path()).unwrap();

    assert_eq!("Alice", db.get("user:1").unwrap().unwrap());
}

#[test]
fn test_backup_get_metadata() {
    let temp_dir = tempdir().unwrap();
    let backup = tempdir().unwrap();
    let backup_path = backup.path();

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
    );

    db.backup_to(backup_path).unwrap();

    drop(db);

    temp_dir.close().unwrap();

    let temp_dir = tempdir().unwrap();

    let file = backup_path.read_dir().unwrap().next().unwrap().unwrap();

    let db = DB::load_from(&file.path(), temp_dir.path()).unwrap();

    assert_eq!("Alice", db.get("user:1").unwrap().unwrap());

    assert_eq!(meta.freq, 1);

    assert!(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs()
            > meta.created_at
    );
}

#[test]
fn test_backup_ttl() {
    let temp_dir = tempdir().unwrap();
    let backup = tempdir().unwrap();
    let backup_path = backup.path();

    let db = DB::new(temp_dir.path()).unwrap();

    db.set("user:1", "Alice", Some(Duration::new(5, 0)))
        .unwrap();

    assert_eq!("Alice", db.get("user:1").unwrap().unwrap());

    db.backup_to(backup_path).unwrap();

    drop(db);

    sleep(Duration::new(6, 0));
    temp_dir.close().unwrap();

    let temp_dir = tempdir().unwrap();

    let file = backup_path.read_dir().unwrap().next().unwrap().unwrap();

    let db = DB::load_from(&file.path(), temp_dir.path()).unwrap();
    sleep(Duration::new(1, 0));

    assert_eq!(None, db.get("user:1").unwrap());
}
