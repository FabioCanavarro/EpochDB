use std::path::Path;
use std::time::Duration;

use epoch_db::DB;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // 1. Open the database. It will be created if it doesn't exist.
    let db = DB::new(Path::new("./my_database"))?;

    // 2. Set a value with a 10-second TTL.
    db.set("user:1", "Alice", Some(Duration::from_secs(10)))?;

    // 3. Get the value back.
    if let Some(value) = db.get("user:1")? {
        println!("Found value: {}", value); // "Found value: Alice"
    }

    // 4. Remove the data.
    db.remove("user:1")?;

    Ok(())
}
