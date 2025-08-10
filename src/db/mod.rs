//! The `db` module contains the core logic for the TransientDB database.
//! It includes the `DB` struct and its implementation, which provides the
//! primary API for interacting with the database.

pub mod errors;

use crate::{DB, Metadata, metrics::Metrics};
use chrono::Local;
use errors::TransientError;
use sled::{
    Config,
    transaction::{ConflictableTransactionError, TransactionError, Transactional},
};
use std::{
    error::Error,
    fs::File,
    io::{ErrorKind, Read, Write},
    path::Path,
    str::from_utf8,
    sync::{Arc, atomic::AtomicBool},
    thread::{self, JoinHandle},
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use zip::{ZipArchive, ZipWriter, write::SimpleFileOptions};

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
    pub fn new(path: &Path) -> Result<DB, Box<dyn Error>> {
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
        let shutdown_clone_ttl_thread = Arc::clone(&shutdown);
        let shutdown_clone_size_thread = Arc::clone(&shutdown);

        // Convert to pathbuf to gain ownership
        let path_buf = path.to_path_buf();

        // TODO: Later have a clean up thread that checks if the following thread is fine and spawn
        // it back and join the thread lol

        let ttl_thread: JoinHandle<Result<(), TransientError>> = thread::spawn(move || {
            loop {
                thread::sleep(Duration::new(0, 100000000));

                if shutdown_clone_ttl_thread.load(std::sync::atomic::Ordering::SeqCst) {
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

                                    // Prometheus Metrics
                                    Metrics::dec_keys_total("data");
                                    Metrics::dec_keys_total("meta");
                                    Metrics::dec_keys_total("ttl");
                                    Metrics::increment_ttl_expired_keys();

                                    Ok(())
                                },
                            );
                        l.map_err(|_| TransientError::SledTransactionError)?;
                    } else {
                        break;
                    }
                }
            }
            Ok(())
        });

        let size_thread: JoinHandle<Result<(), TransientError>> = thread::spawn(move || {
            loop {
                thread::sleep(Duration::new(0, 100000000));

                if shutdown_clone_size_thread.load(std::sync::atomic::Ordering::SeqCst) {
                    break;
                }

                let metadata = path_buf
                    .metadata()
                    .map_err(|_| TransientError::DBMetadataNotFound)?;
                Metrics::set_disk_size((metadata.len() as f64) / 1024.0 / 1024.0);
            }
            Ok(())
        });

        Ok(DB {
            data_tree,
            meta_tree,
            ttl_tree,
            ttl_thread: Some(ttl_thread),
            size_thread: Some(size_thread),
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
                        let mut meta = Metadata::from_u8(&m)
                            .map_err(|_| ConflictableTransactionError::Abort(()))?;
                        if let Some(t) = meta.ttl {
                            let _ = ttl_tree.remove([&t.to_be_bytes()[..], byte].concat());
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

                if let Some(d) = ttl_sec {
                    ttl_tree.insert([&d.to_be_bytes()[..], byte].concat(), byte)?;
                    Metrics::inc_keys_total("ttl");
                };

                Ok(())
            });
        l.map_err(|_| TransientError::SledTransactionError)?;

        // Prometheus metrics
        Metrics::increment_operations("set");
        Metrics::inc_keys_total("data");
        Metrics::inc_keys_total("meta");

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

        Metrics::increment_operations("get");

        match val {
            Some(val) => Ok(Some(from_utf8(&val)?.to_string())),
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
            let meta = Metadata::from_u8(&metadata)?;
            let s = freq_tree.compare_and_swap(
                byte,
                Some(metadata),
                Some(meta.freq_incretement().to_u8()?),
            );
            if let Ok(ss) = s {
                if ss.is_ok() {
                    break;
                }
            }
        }
        Metrics::increment_operations("increment_frequency");

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
                let time = Metadata::from_u8(&meta)
                    .map_err(|_| ConflictableTransactionError::Abort(()))?
                    .ttl;
                freq.remove(*byte)?;

                Metrics::dec_keys_total("data");
                Metrics::dec_keys_total("meta");

                if let Some(t) = time {
                    Metrics::dec_keys_total("ttl");

                    let _ = ttl_tree.remove([&t.to_be_bytes()[..], &byte[..]].concat());
                }

                Ok(())
            });
        l.map_err(|_| TransientError::SledTransactionError)?;

        Metrics::increment_operations("rm");

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
            Some(val) => Ok(Some(Metadata::from_u8(&val)?)),
            None => Ok(None),
        }
    }

    pub fn flush(&self) -> Result<(), Box<dyn Error>> {
        self.data_tree.flush()?;
        self.meta_tree.flush()?;
        self.ttl_tree.flush()?;

        Ok(())
    }

    pub fn backup_to(&self, path: &Path) -> Result<(), Box<dyn Error>> {
        self.flush()?;

        if !path.is_dir() {
            Err(TransientError::FolderNotFound {
                path: path.to_path_buf(),
            })?;
        }

        let options =
            SimpleFileOptions::default().compression_method(zip::CompressionMethod::Bzip2);

        let backup_name = format!("backup-{}.zip", Local::now().format("%Y-%m-%d_%H-%M-%S"));

        let zip_file = File::create(path.join(&backup_name))?;

        let mut zipw = ZipWriter::new(zip_file);

        zipw.start_file("data.epoch", options)?;
        for i in self.data_tree.iter() {
            let iu = i?;

            let key = &iu.0;
            let value = &iu.1;
            let meta = self
                .meta_tree
                .get(key)?
                .ok_or(TransientError::MetadataNotFound)?;

            // NOTE: A usize is diffrent on diffrent machines
            // and a usize will never exceed a u64 in lenght lol
            let kl: u64 = key.len().try_into()?;
            let vl: u64 = value.len().try_into()?;
            let ml: u64 = meta.len().try_into()?;

            zipw.write_all(&kl.to_be_bytes())?;
            zipw.write_all(key)?;
            zipw.write_all(&vl.to_be_bytes())?;
            zipw.write_all(value)?;
            zipw.write_all(&ml.to_be_bytes())?;
            zipw.write_all(&meta)?;
        }

        zipw.finish()?;

        let zip_file = File::open(path.join(backup_name))?;
        let size = zip_file.metadata()?.len();
        Metrics::set_backup_size((size as f64) / 1024.0 / 1024.0);

        Ok(())
    }

    // WARN: Add a transactional batching algorithm to ensure safety incase of a power outage
    pub fn load_from(path: &Path, db_path: &Path) -> Result<DB, Box<dyn Error>> {
        if !path.is_file() {
            Err(TransientError::FolderNotFound {
                path: path.to_path_buf(),
            })?;
        }

        let db = DB::new(db_path)?;

        let file = File::open(path)?;

        let mut archive = ZipArchive::new(file)?;

        let mut data = archive.by_name("data.epoch")?;

        loop {
            let mut len: [u8; 8] = [0u8; 8];
            if let Err(e) = data.read_exact(&mut len) {
                if let ErrorKind::UnexpectedEof = e.kind() {
                    break;
                }
            }

            let mut key = vec![0; u64::from_be_bytes(len).try_into()?];
            data.read_exact(&mut key)?;

            data.read_exact(&mut len)?;
            let mut val = vec![0; u64::from_be_bytes(len).try_into()?];
            data.read_exact(&mut val)?;

            data.read_exact(&mut len)?;
            let mut meta_byte = vec![0; u64::from_be_bytes(len).try_into()?];
            data.read_exact(&mut meta_byte)?;

            let meta = Metadata::from_u8(&meta_byte)?;

            db.meta_tree.insert(&key, meta.to_u8()?)?;

            db.data_tree.insert(&key, val)?;

            if let Some(d) = meta.ttl {
                db.ttl_tree
                    .insert([&d.to_be_bytes()[..], &key].concat(), key)?;
            };
        }

        Ok(db)
    }

    pub fn iter(&mut self) -> DataIter {
        DataIter {
            data: (self.data_tree.iter(), self.meta_tree.clone()),
        }
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

        let _ = self
            .size_thread
            .take()
            .expect("Fail to take ownership of ttl_thread")
            .join()
            .expect("Joining failed");
    }
}

pub struct DataIter {
    pub data: (sled::Iter, Arc<sled::Tree>),
}

impl Iterator for DataIter {
    type Item = Result<(String, String, Metadata), Box<dyn Error>>;

    fn next(&mut self) -> Option<Self::Item> {
        let data_iter = &mut self.data.0;

        let data = match data_iter.next()? {
            Ok(a) => a,
            Err(e) => {
                return Some(Err(Box::new(e)));
            }
        };

        let (kb, vb) = data;

        let meta_tree = &mut self.data.1;

        let mb = match meta_tree.get(&kb) {
            Ok(a) => a,
            Err(e) => {
                return Some(Err(Box::new(e)));
            }
        }?;

        let key = match from_utf8(&kb) {
            Ok(a) => a,
            Err(e) => {
                return Some(Err(Box::new(e)));
            }
        }
        .to_string();

        let value = match from_utf8(&vb) {
            Ok(a) => a,
            Err(e) => {
                return Some(Err(Box::new(e)));
            }
        }
        .to_string();

        let meta = match Metadata::from_u8(&mb) {
            Ok(a) => a,
            Err(e) => {
                return Some(Err(Box::new(e)));
            }
        };

        Some(Ok((key, value, meta)))
    }
}
