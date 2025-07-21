use std::fs::File;
use std::io::{self, Write};
use std::io::Read;
use std::path::{PathBuf};
use epoch_db::db::errors::TransientError;
use zip::write::SimpleFileOptions;
use zip::ZipWriter;


fn main() -> Result<(), Box<dyn std::error::Error>> {
    // let db = DB::new(Path::new("./databasetest"))?;
    // db.backup_to(Path::new("./backup")).unwrap();
    let path = PathBuf::from("./backup");
    if !path.is_dir() {
        Err(TransientError::FolderNotFound { path: path.to_path_buf() })?;
    }

    let options = SimpleFileOptions::default().compression_method(zip::CompressionMethod::Bzip2);
    println!("here");

    
    // WARN: Temporary
    let zip_file = File::create(path.join("backup.zip"))?;
    println!("here");

    let mut zipw = ZipWriter::new(zip_file);
    println!("here");
    let paths = PathBuf::from("/databasetest");

    for entry in paths.read_dir()? {
        let e = entry?.path();
        if e.is_file() {
            let file = File::open(&e)?;
            println!("this");
            let file_name = e.file_name()
                    .ok_or(TransientError::FileNameDoesntExist)?
                    .to_str().ok_or(TransientError::FileNameDoesntExist)?;

            println!("this??");
            zipw.start_file(
                file_name,
                options
                
            )?;
            println!("thisss");

            let mut buffer = Vec::new();
            println!("threre");

            io::copy(&mut file.take(u64::MAX), &mut buffer)?;
            println!("thus");

            zipw.write_all(&buffer)?;
            println!("Boom");
        }
    }
    

   Ok(())
}
