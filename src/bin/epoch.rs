use epoch_db::DB;
use std::{path::Path, thread::sleep, time::Duration};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let db = DB::new(Path::new("./databasetest")).unwrap();
    // What if I drop everything then re open it?

    db.set("H", "haha", None).unwrap();
    db.set("HAHAHHAH", "Skib", None).unwrap();
    db.set("HI", "h", None).unwrap();
    db.set("Chronos", "Temporal", None).unwrap();
    db.set("pop", "HAHAHAHH", Some(Duration::new(0, 100000))).unwrap();
    db.get("pop").unwrap();
    db.get("pop").unwrap();
    db.get("pop").unwrap();
    db.get("pop").unwrap();

    sleep(Duration::new(1, 0));

    db.backup_to(Path::new("./backup/")).unwrap();

    Ok(())
}
