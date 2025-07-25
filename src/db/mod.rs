//! The `db` module contains the core logic for the TransientDB database.
//! It includes the `DB` struct and its implementation, which provides the
//! primary API for interacting with the database.

pub mod backup;
pub mod errors;

use crate::{DB, Metadata};
use chrono::Local;
use errors::TransientError;
use sled::{
    transaction::{ConflictableTransactionError, TransactionError, Transactional}, Config, Iter, Tree
};
use std::io::Read;
use std::{
    env::current_dir,
    error::Error,
    fs::{File, create_dir},
    io::{self, Write},
    path::{Path, PathBuf},
    str::from_utf8,
    sync::{Arc, atomic::AtomicBool},
    thread::{self, JoinHandle},
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use zip::{
    ZipWriter,
    write::{FileOptions, SimpleFileOptions},
};

impl DB {
    /// Creates a new `DB` instance or opens an existing one at the specified path.
    ///
    /// This function initializes the underlying `sled` database, opens the required
    /// data trees (`data_tree`, `meta_tree`, `ttl_tree`), and spawns a background
    /// thread to handle TTL expirations.
    ///
    /// # Errors
    ///
    /// Returns a `sled::Error` if the database cannot be opened at the given path.
    pub fn new(path: &Path) -> Result<DB, sled::Error> {
        let db = Config::new()
            .path(path)
            .cache_capacity(512 * 1024 * 1024)
            .open()?;

        let data_tree = Arc::new(db.open_tree("data_tree")?);
        let meta_tree = Arc::new(db.open_tree("freq_tree")?);
        let ttl_tree = Arc::new(db.open_tree("ttl_tree")?);

        let ttl_tree_clone = Arc::clone(&ttl_tree);
        let meta_tree_clone = Arc::clone(&meta_tree);
        let data_tree_clone = Arc::clone(&data_tree);

        let shutdown: Arc<AtomicBool> = Arc::new(AtomicBool::new(false));
        let shutdown_clone = Arc::clone(&shutdown);

        // TODO: Later have a clean up thread that checks if the following thread is fine and spawn
        // it back and join the thread lol

        let thread: JoinHandle<Result<(), TransientError>> = thread::spawn(move || {
            loop {
                thread::sleep(Duration::new(0, 100000000));

                if shutdown_clone.load(std::sync::atomic::Ordering::SeqCst) {
                    break;
                }

                let keys = ttl_tree_clone.iter();

                for i in keys {
                    let full_key = i.map_err(|e| TransientError::SledError { error: e })?;

                    // NOTE: The reason time is 14 u8s long is because it is being stored like
                    // this ([time,key], key) not ((time,key), key)
                    let key = full_key.0;
                    let key_byte = full_key.1;

                    if key.len() < 8 {
                        Err(TransientError::ParsingToU64ByteFailed)?
                    }

                    let time_byte: [u8; 8] = (&key[..8])
                        .try_into()
                        .map_err(|_| TransientError::ParsingToByteError)?;

                    let time = u64::from_be_bytes(time_byte);
                    let curr_time = SystemTime::now()
                        .duration_since(UNIX_EPOCH)
                        .expect("Cant get SystemTime")
                        .as_secs();

                    if curr_time >= time {
                        let l: Result<(), TransactionError<()>> =
                            (&*data_tree_clone, &*meta_tree_clone, &*ttl_tree_clone).transaction(
                                |(data, freq, ttl_tree_clone)| {
                                    let byte = &key_byte;
                                    data.remove(byte)?;
                                    freq.remove(byte)?;

                                    let _ = ttl_tree_clone.remove([&time_byte, &byte[..]].concat());

                                    Ok(())
                                },
                            );
                        l.map_err(|_| TransientError::SledTransactionError)?;
                    } else {
                        continue;
                    }
                }
            }
            Ok(())
        });
        Ok(DB {
            data_tree,
            meta_tree,
            ttl_tree,
            ttl_thread: Some(thread),
            shutdown,
            path: path.to_path_buf(),
        })
    }

    /// Sets a key-value pair with an optional Time-To-Live (TTL).
    ///
    /// If the key already exists, its value and TTL will be updated.
    /// If `ttl` is `None`, the key will be persistent.
    ///
    /// # Errors
    ///
    /// This function can return an error if there's an issue with the underlying
    pub fn set(&self, key: &str, val: &str, ttl: Option<Duration>) -> Result<(), Box<dyn Error>> {
        let data_tree = &self.data_tree;
        let freq_tree = &self.meta_tree;
        let ttl_tree = &self.ttl_tree;
        let byte = key.as_bytes();
        let ttl_sec = match ttl {
            Some(t) => {
                let systime = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .expect("Cant get SystemTime");
                Some((t + systime).as_secs())
            }
            None => None,
        };

        let l: Result<(), TransactionError<()>> = (&**data_tree, &**freq_tree, &**ttl_tree)
            .transaction(|(data, freq, ttl_tree)| {
                match freq.get(byte)? {
                    Some(m) => {
                        let mut meta = Metadata::from_u8(&m.to_vec())
                            .map_err(|_| ConflictableTransactionError::Abort(()))?;
                        if let Some(t) = meta.ttl {
                            let _ = ttl_tree.remove([&t.to_be_bytes()[..], &byte[..]].concat());
                        }
                        meta.ttl = ttl_sec;
                        freq.insert(
                            byte,
                            meta.to_u8()
                                .map_err(|_| ConflictableTransactionError::Abort(()))?,
                        )?;
                    }
                    None => {
                        freq.insert(
                            byte,
                            Metadata::new(ttl_sec)
                                .to_u8()
                                .map_err(|_| ConflictableTransactionError::Abort(()))?,
                        )?;
                    }
                }

                data.insert(byte, val.as_bytes())?;

                match ttl_sec {
                    Some(d) => {
                        ttl_tree.insert([&d.to_be_bytes()[..], &byte[..]].concat(), byte)?;
                    }
                    None => (),
                };

                Ok(())
            });
        let _ = l.map_err(|_| TransientError::SledTransactionError)?;

        Ok(())
    }

    /// Retrieves the value for a given key.
    ///
    /// # Errors
    ///
    /// Returns an error if the value cannot be retrieved from the database or if
    /// the value is not valid UTF-8.
    pub fn get(&self, key: &str) -> Result<Option<String>, Box<dyn Error>> {
        let data_tree = &self.data_tree;
        let byte = key.as_bytes();
        let val = data_tree.get(byte)?;
        match val {
            Some(val) => Ok(Some(from_utf8(&val.to_vec())?.to_string())),
            None => Ok(None),
        }
    }

    /// Atomically increments the frequency counter for a given key.
    ///
    /// # Errors
    ///
    /// This function can return an error if the key does not exist or if there
    /// is an issue with the compare-and-swap operation.
    pub fn increment_frequency(&self, key: &str) -> Result<(), Box<dyn Error>> {
        let freq_tree = &self.meta_tree;
        let byte = &key.as_bytes();

        loop {
            let metadata = freq_tree
                .get(byte)?
                .ok_or(TransientError::IncretmentError)?;
            let meta = Metadata::from_u8(&metadata.to_vec())?;
            let s = freq_tree.compare_and_swap(
                byte,
                Some(metadata),
                Some(meta.freq_incretement().to_u8()?),
            );
            match s {
                Ok(ss) => match ss {
                    Ok(_) => break,
                    Err(_) => (),
                },
                Err(_) => (),
            }
        }

        Ok(())
    }

    /// Removes a key-value pair and its associated metadata from the database.
    ///
    /// # Errors
    ///
    /// Can return an error if the transaction to remove the data fails.
    pub fn remove(&self, key: &str) -> Result<(), Box<dyn Error>> {
        let data_tree = &self.data_tree;
        let freq_tree = &self.meta_tree;
        let ttl_tree = &self.ttl_tree;
        let byte = &key.as_bytes();
        let l: Result<(), TransactionError<()>> = (&**data_tree, &**freq_tree, &**ttl_tree)
            .transaction(|(data, freq, ttl_tree)| {
                data.remove(*byte)?;
                let meta = freq
                    .get(byte)?
                    .ok_or(ConflictableTransactionError::Abort(()))?;
                let time = Metadata::from_u8(&meta.to_vec())
                    .map_err(|_| ConflictableTransactionError::Abort(()))?
                    .ttl;
                freq.remove(*byte)?;

                match time {
                    Some(t) => {
                        let _ = ttl_tree.remove([&t.to_be_bytes()[..], &byte[..]].concat());
                    }
                    None => (),
                }

                Ok(())
            });
        l.map_err(|_| TransientError::SledTransactionError)?;
        Ok(())
    }

    /// Retrieves the metadata for a given key.
    ///
    /// # Errors
    ///
    /// Returns an error if the metadata cannot be retrieved or deserialized.
    pub fn get_metadata(&self, key: &str) -> Result<Option<Metadata>, Box<dyn Error>> {
        let freq_tree = &self.meta_tree;
        let byte = key.as_bytes();
        let meta = freq_tree.get(byte)?;
        match meta {
            Some(val) => Ok(Some(Metadata::from_u8(&val.to_vec())?)),
            None => Ok(None),
        }
    }

    pub fn flush(&self) -> Result<(), Box<dyn Error>> {
        self.data_tree.flush()?;
        self.meta_tree.flush()?;
        self.ttl_tree.flush()?;

        Ok(())
    }

    pub fn backup_to(self, path: &Path) -> Result<(), Box<dyn Error>> {
        self.flush()?;

        let db_path = self.path.to_path_buf();


        if !path.is_dir() {
            Err(TransientError::FolderNotFound {
                path: path.to_path_buf(),
            })?;
        }

        let files: [(Iter, &str);3] = [(self.data_tree.iter(),"data.bin"),(self.meta_tree.iter(),"meta.bin"),(self.ttl_tree.iter(),"ttl.bin")];

        let options = SimpleFileOptions::default().compression_method(zip::CompressionMethod::Bzip2);

        let zip_file = File::create(path.join("backup").join(Local::now().to_string()).join(".zip"))?;

        let mut zipw = ZipWriter::new(zip_file);

        for (iter, entry) in files {
            // Starts the Zipping process

            zipw.start_file(entry, options)?;

            let mut buffer: [u8; 8192] = [0u8; 8192];

            let mut index = 0;

            for i in iter {
                let iu = i?;

                let key = &iu.0.to_vec()[..];
                let value = &iu.1.to_vec()[..];
                let kl = key.len();
                let vl = value.len();

                buffer[index..(index + kl)].copy_from_slice(key);
                index += kl;

                buffer[index..(index + vl)].copy_from_slice(value);
                index += vl;


            }


            zipw.write_all(&buffer)?;
        }

        Ok(())
    }

    pub fn load_from(path: &Path) -> Result<DB, Box<dyn Error>> {
        todo!()
    }
}

impl Drop for DB {
    /// Gracefully shuts down the TTL background thread when the `DB` instance
    /// goes out of scope.
    fn drop(&mut self) {
        self.shutdown
            .store(true, std::sync::atomic::Ordering::SeqCst);

        let _ = self
            .ttl_thread
            .take()
            .expect("Fail to take ownership of ttl_thread")
            .join()
            .expect("Joining failed");
    }
}
