use epoch_db::DB;
use std::sync::Arc;
use std::thread;
use tempfile::tempdir;
/// Tests the "happy path" for a transaction.
/// It verifies that if the closure succeeds, all the changes within it
/// are correctly committed to the database.
#[test]
fn test_transaction_commit_on_success() {
    let temp_dir = tempdir().unwrap();
    let db = DB::new(temp_dir.path()).unwrap();

    // Setup initial state
    db.set("user:alice", "active", None).unwrap();
    db.set("user:bob", "inactive", None).unwrap();

    // Run the transaction
    let result = db.transaction(|tx| {
        // Perform multiple operations
        tx.remove("user:alice")?;
        tx.set("user:bob", "active_transferred", None)?;
        Ok(())
    });

    // The transaction should succeed
    assert!(result.is_ok());

    // Verify the final state
    assert!(
        db.get("user:alice").unwrap().is_none(),
        "Alice should be removed."
    );
    assert_eq!(
        db.get("user:bob").unwrap().unwrap(),
        "active_transferred",
        "Bob should have the new value."
    );
}

/// Tests the "all-or-nothing" promise (Atomicity).
/// It verifies that if the closure returns an error, ALL changes made
/// inside it are completely rolled back, as if they never happened.
#[test]
fn test_transaction_rollback_on_failure() {
    let temp_dir = tempdir().unwrap();
    let db = DB::new(temp_dir.path()).unwrap();

    // Setup initial state
    db.set("user:charlie", "initial_state", None).unwrap();

    // Run a transaction that will fail
    let result = db.transaction(|tx| {
        // Make a change
        tx.set("user:charlie", "new_state_that_should_be_rolled_back", None)?;

        // Force the transaction to fail
        Err(Box::from("Something went wrong!"))
    });

    // The transaction itself should report the failure
    assert!(result.is_err());

    // CRITICAL: Verify that the database is still in its original state.
    // The change made inside the failed transaction should have been undone.
    assert_eq!(
        db.get("user:charlie").unwrap().unwrap(),
        "initial_state",
        "Charlie's data should be rolled back to its original state."
    );
}

/// Tests the "Isolation" promise under concurrent load.
/// It spawns multiple threads that all try to modify the same counter
/// inside separate transactions. If isolation works, the final count
/// should be correct, with no lost updates.
#[test]
fn test_transaction_isolation() {
    let temp_dir = tempdir().unwrap();
    // Use Arc to share the DB instance across threads
    let db = Arc::new(DB::new(temp_dir.path()).unwrap());

    // Set an initial counter value
    db.set("counter", "0", None).unwrap();

    let mut handles = vec![];
    let num_threads = 10;
    let increments_per_thread = 50;

    for _ in 0..num_threads {
        let db_clone = Arc::clone(&db);
        let handle = thread::spawn(move || {
            for _ in 0..increments_per_thread {
                // Each thread runs its own transaction
                db_clone
                    .transaction(|tx| {
                        let current_val_str = tx.get("counter")?.unwrap();
                        let current_val: u64 = current_val_str.parse()?;
                        let new_val = current_val + 1;
                        tx.set("counter", &new_val.to_string(), None)?;
                        Ok(())
                    })
                    .unwrap();
            }
        });
        handles.push(handle);
    }

    for handle in handles {
        handle.join().unwrap();
    }

    // After all threads are done, the counter should have been incremented
    // exactly num_threads * increments_per_thread times.
    let final_value_str = db.get("counter").unwrap().unwrap();
    let final_value: u64 = final_value_str.parse().unwrap();
    let expected_value = (num_threads * increments_per_thread) as u64;

    assert_eq!(
        final_value, expected_value,
        "The final counter should be correct, proving no lost updates occurred."
    );
}
