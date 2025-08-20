use crate::{
    DB, Metadata,
    db::{errors::TransientError, transaction::metric_handler::GuardMetricChanged},
};
use sled::transaction::{
    ConflictableTransactionError, TransactionError, Transactional, TransactionalTree,
};
use std::{
    error::Error,
    str::from_utf8,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

pub mod metric_handler;

/// This struct is the Guard for the database Transaction method.
///
/// This struct holds all the trees of the main database and the changed_metric, It provides us
/// with the ability to make transactions for better data safety.
///
/// When the transaction method concludes this struct, will check all the changed_metric, and
/// will increment or decrement the corresponding metric in the real database, to ensure that the
/// correct metrics will be shown.
pub struct TransactionalGuard<'a> {
    data_tree: &'a TransactionalTree,
    meta_tree: &'a TransactionalTree,
    ttl_tree: &'a TransactionalTree,
    changed_metric: &'a mut GuardMetricChanged,
}

// NOTE: The reason why I didn't convert everything to Transient error is because of the
// UnabortableTransactionError enum, where is error, they will reset,
// If I fuck with this who knows what will be fucked up TT
impl<'a> TransactionalGuard<'a> {
    /// Sets a key-value pair with an optional Time-To-Live (TTL).
    ///
    /// If the key already exists, its value and TTL will be updated.
    /// If `ttl` is `None`, the key will be persistent.
    ///
    /// # Errors
    ///
    /// This function can return an error if there's an issue with the underlying
    pub fn set(
        &mut self,
        key: &str,
        val: &str,
        ttl: Option<Duration>,
    ) -> Result<(), Box<dyn Error>> {
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

        match freq_tree.get(byte)? {
            Some(m) => {
                let mut meta = Metadata::from_u8(&m)?;
                if let Some(t) = meta.ttl {
                    let _ = ttl_tree.remove([&t.to_be_bytes()[..], byte].concat());
                }
                meta.ttl = ttl_sec;
                freq_tree.insert(byte, meta.to_u8()?)?;
            }
            None => {
                freq_tree.insert(byte, Metadata::new(ttl_sec).to_u8()?)?;
            }
        }

        data_tree.insert(byte, val.as_bytes())?;

        if let Some(d) = ttl_sec {
            ttl_tree.insert([&d.to_be_bytes()[..], byte].concat(), byte)?;
            self.changed_metric.ttl_keys_total_changed += 1;
        };

        // Prometheus metrics
        self.changed_metric.keys_total_changed += 1;
        self.changed_metric.set_operation_total += 1;

        Ok(())
    }

    /// Retrieves the value for a given key.
    ///
    /// # Errors
    ///
    /// Returns an error if the value cannot be retrieved from the database or if
    /// the value is not valid UTF-8.
    pub fn get(&mut self, key: &str) -> Result<Option<String>, Box<dyn Error>> {
        let data_tree = &self.data_tree;
        let byte = key.as_bytes();
        let val = data_tree.get(byte)?;

        self.changed_metric.get_operation_total += 1;

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
    pub fn increment_frequency(&mut self, key: &str) -> Result<(), Box<dyn Error>> {
        let freq_tree = &self.meta_tree;
        let byte = &key.as_bytes();

        let metadata = freq_tree
            .get(byte)?
            .ok_or(TransientError::IncretmentError)?;
        let meta = Metadata::from_u8(&metadata)?;

        freq_tree.remove(*byte)?;
        freq_tree.insert(*byte, meta.freq_incretement().to_u8()?)?;

        self.changed_metric.inc_freq_operation_total += 1;

        Ok(())
    }

    /// Removes a key-value pair and its associated metadata from the database.
    ///
    /// # Errors
    ///
    /// Can return an error if the transaction to remove the data fails.
    pub fn remove(&mut self, key: &str) -> Result<(), Box<dyn Error>> {
        let data_tree = &self.data_tree;
        let freq_tree = &self.meta_tree;
        let ttl_tree = &self.ttl_tree;
        let byte = &key.as_bytes();
        data_tree.remove(*byte)?;
        let meta = freq_tree
            .get(byte)?
            .ok_or(TransientError::MetadataNotFound)?;
        let time = Metadata::from_u8(&meta)?.ttl;
        freq_tree.remove(*byte)?;

        self.changed_metric.keys_total_changed -= 1;

        if let Some(t) = time {
            self.changed_metric.ttl_keys_total_changed -= 1;

            let _ = ttl_tree.remove([&t.to_be_bytes()[..], &byte[..]].concat());
        }

        self.changed_metric.rm_operation_total += 1;

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
}

impl DB {
    pub fn transaction<F>(&self, f: F) -> Result<(), TransientError>
    where
        F: Fn(&mut TransactionalGuard) -> Result<(), Box<dyn Error>>,
    {
        let l: Result<GuardMetricChanged, TransactionError<()>> =
            (&*self.data_tree, &*self.meta_tree, &*self.ttl_tree).transaction(
                |(data_tree, meta_tree, ttl_tree)| {
                    let mut guard_metrics = GuardMetricChanged {
                        keys_total_changed: 0,
                        ttl_keys_total_changed: 0,
                        set_operation_total: 0,
                        rm_operation_total: 0,
                        inc_freq_operation_total: 0,
                        get_operation_total: 0,
                    };
                    let mut transaction_guard = TransactionalGuard {
                        data_tree,
                        meta_tree,
                        ttl_tree,
                        changed_metric: &mut guard_metrics,
                    };
                    f(&mut transaction_guard)
                        .map_err(|_| ConflictableTransactionError::Abort(()))?;

                    Ok(guard_metrics)
                },
            );

        l.map_err(|_| TransientError::SledTransactionError)?
            .inc_all_metrics();
        Ok(())
    }
}
