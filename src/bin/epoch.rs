use std::fs::File;
use std::io::{self, Write};
use std::io::Read;
use std::path::{Path, PathBuf};
use epoch_db::db::errors::TransientError;
use epoch_db::DB;
use zip::write::SimpleFileOptions;
use zip::ZipWriter;


fn main() -> Result<(), Box<dyn std::error::Error>> {
    let db = DB::new(Path::new("./databasetest"))?;
    db.backup_to(Path::new("./backup")).unwrap();

    // What if I drop everything then re open it?
    

   Ok(())
}
