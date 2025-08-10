use epoch_db::DB;
use std::{path::Path, time::Duration};


fn main() -> Result<(), Box<dyn std::error::Error>> {
    let db = DB::new(Path::new("./databasetest")).unwrap();

    db.set("H", "haha", None).unwrap();
    db.set("HAHAHHAH", "Skib", None).unwrap();
    db.set("HI", "h", None).unwrap();
    db.set("Chronos", "Temporal", None).unwrap();
    db.set("pop", "HAHAHAHH", Some(Duration::new(0, 100000))).unwrap();
    for i in 0..1000 {
        db.get("HI").unwrap();
        db.set(&format!("{i}"), "h", None).unwrap();
    }
    db.backup_to(Path::new("./backup/")).unwrap();

    Ok(())
}
