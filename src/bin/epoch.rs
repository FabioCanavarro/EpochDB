use epoch_db::DB;
use std::path::Path;

fn main() -> Result<(), Box<dyn std::error::Error>> {
DB::load_from(Path::new("./backup/backup-2025-07-26_22-14-37.zip"), Path::new("./da"));
    Ok(())
}
