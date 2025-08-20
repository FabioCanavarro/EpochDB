use std::path::Path;

use epoch_db::DB;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let db = DB::new(Path::new("./my_database"))?;

    db.set("user:2", "Bob", None)?;

    // Increment the frequency counter
    db.increment_frequency("user:2")?;

    // Get the metadata for the key
    if let Some(meta) = db.get_metadata("user:2")? {
        println!("'user:2' has been accessed {} time(s)", meta.freq);
        // "'user:2' has been accessed 1 time(s)"
    }

    Ok(())
}
