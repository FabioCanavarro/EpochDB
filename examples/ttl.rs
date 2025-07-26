use epoch_db::DB;
use std::path::Path;
use std::thread::sleep;
use std::time::Duration;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let db = DB::new(Path::new("./my_database"))?;

    // Set a key with a 2-second TTL
    db.set("session:123", "user_token", Some(Duration::from_secs(2)))?;
    println!("'session:123' is set.");

    // The key exists initially
    assert!(db.get("session:123")?.is_some());

    // Wait for the TTL to expire
    sleep(Duration::from_secs(3));

    // The key should now be gone
    assert!(db.get("session:123")?.is_none());
    println!("'session:123' has expired.");

    // You can also update a key to make it permanent
    db.set(
        "user:permanent",
        "This will last forever",
        Some(Duration::from_secs(1)),
    )?;
    db.set("user:permanent", "This will last forever", None)?; // Remove the TTL

    sleep(Duration::from_secs(2));
    assert!(db.get("user:permanent")?.is_some());
    println!("'user:permanent' is still here.");

    Ok(())
}
