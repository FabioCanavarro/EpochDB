use std::{error::Error, fs::File, io::{self, Read, Write}, path::Path};

use zip::{write::SimpleFileOptions, ZipWriter};

use crate::{db::errors::TransientError, DB};

    pub fn backup_to(db: DB, path: &Path) -> Result<DB, Box<dyn Error>> {
        db.flush()?;

        let db_path = db.path.to_path_buf();

        drop(db);

        if !path.is_dir() {
            Err(TransientError::FolderNotFound { path: path.to_path_buf() })?;
        }

        let options = SimpleFileOptions::default().compression_method(zip::CompressionMethod::Bzip2);

        
        // WARN: Temporary
        let zip_file = File::create(path.join("backup.zip"))?;

        let mut zipw = ZipWriter::new(zip_file);

        for entry in db_path.read_dir()? {
            let e = entry?.path();
            if e.is_file() {
                let file = File::open(&e)?;
                let file_name = e.file_name()
                        .ok_or(TransientError::FileNameDoesntExist)?
                        .to_str().ok_or(TransientError::FileNameDoesntExist)?;

                zipw.start_file(
                    file_name,
                    options
                    
                )?;

                let mut buffer = Vec::new();

                io::copy(&mut file.take(u64::MAX), &mut buffer)?;

                zipw.write_all(&buffer)?;
            }
        }

        
        Ok(DB::new(&db_path)?)

    }
