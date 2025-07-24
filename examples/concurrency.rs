use epoch_db::DB;
use std::sync::Arc;
use std::thread;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let db = Arc::new(DB::new("./my_database".as_ref())?);
    db.set("concurrent_key", "initial_value", None)?;

    let mut handles = vec![];
    for _ in 0..10 {
        let db_clone = Arc::clone(&db);
        let handle = thread::spawn(move || {
            for _ in 0..100 {
                db_clone.increment_frequency("concurrent_key").unwrap();
            }
        });
        handles.push(handle);
    }

    for handle in handles {
        handle.join().unwrap();
    }

    let final_meta = db.get_metadata("concurrent_key")?.unwrap();
    println!("Final frequency: {}", final_meta.freq); // "Final frequency: 1000"

    Ok(())
}
