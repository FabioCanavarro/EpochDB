use epoch_db::DB;
use std::path::Path;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let db = DB::new(Path::new("./databasetest"))?;
    // What if I drop everything then re open it?
    
    db.set("H", "haha", None)?;
    db.set("HAHAHHAH", "Skib", None)?;
    db.set("HI", "h", None)?;
    db.set("Chronos", "Temporal", None)?;

    db.backup_to(Path::new("./backup/")).unwrap();

    Ok(())
}
