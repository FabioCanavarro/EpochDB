// TODO: Refactor the whole file to use Prometheus crate instead for testing purposes

use metrics::{counter, gauge};

/// A stateless struct that provides a clean, organized API for updating
/// the application's global Prometheus metrics via the `metrics` facade.
#[derive(Debug)]
pub struct Metrics {}

impl Metrics {
    /// Sets the current number of keys for a given tree.
    pub fn set_keys_total(tree: &str, value: u64) {
        gauge!("epochdb_keys_total", "tree" => tree.to_string()).set(value as f64);
    }

    /// Sets the current number of keys for a given tree.
    pub fn inc_keys_total(tree: &str) {
        gauge!("epochdb_keys_total", "tree" => tree.to_string()).increment(1);
    }

    /// Sets the current number of keys for a given tree.
    pub fn dec_keys_total(tree: &str) {
        gauge!("epochdb_keys_total", "tree" => tree.to_string()).decrement(1);
    }

    /// Increments the counter for a specific database operation.
    pub fn increment_operations(op: &str) {
        counter!("epochdb_operations_total", "operation" => op.to_string()).increment(1);
    }

    /// Sets the current total disk size of the database.
    pub fn set_disk_size(bytes: f64) {
        gauge!("epochdb_disk_size_bytes").set(bytes);
    }

    /// Sets the size of the last successful backup.
    pub fn set_backup_size(bytes: f64) {
        gauge!("epochdb_backup_size_bytes").set(bytes);
    }

    /// Increments the counter for expired TTL keys.
    pub fn increment_ttl_expired_keys() {
        counter!("epochdb_ttl_expired_keys_total").increment(1);
    }

    /// Sets the current number of keys for a given tree.
    pub fn inc_amount_keys_total(tree: &str, value: u64) {
        gauge!("epochdb_keys_total", "tree" => tree.to_string()).set(value as f64);
    }

    /// Sets the current number of keys for a given tree.
    pub fn dec_amount_keys_total(tree: &str, amount: u64) {
        gauge!("epochdb_keys_total", "tree" => tree.to_string()).decrement(amount as f64);
    }

    /// Increments the counter for a specific database operation.
    pub fn increment_amount_operations(op: &str, amount: u64) {
        counter!("epochdb_operations_total", "operation" => op.to_string()).increment(amount);
    }
}
