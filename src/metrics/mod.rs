use std::error::Error;

use prometheus::{Gauge, IntCounter, IntCounterVec, IntGaugeVec, Opts};

#[derive(Debug)]
pub struct Metrics {
    pub keys_total: IntGaugeVec,
    pub operations_total: IntCounterVec,
    pub disk_size: Gauge,
    pub backup_size: Gauge,
    pub ttl_expired_keys_total: IntCounter,
}

impl Metrics {
    pub fn new() -> Result<Metrics, Box<dyn Error>> {
        let keys_total_opts = Opts::new("epochdb_keys_total", "Total number of keys in a tree");

        let operations_total_opts =
            Opts::new("epochdb_operations_total", "Total number of operations");

        let disk_size_opts = Opts::new("epochdb_disk_size", "Size of the directory");

        let backup_size_opts = Opts::new("epochdb_backup_size", "Size of the backup");

        let ttl_expired_keys_total_opts = Opts::new(
            "epochdb_ttl_expired_keys_total_opts",
            "Total amount of expired ttl keys",
        );

        let keys_total = IntGaugeVec::new(keys_total_opts, &["tree"])?;

        let operations_total = IntCounterVec::new(
            operations_total_opts,
            &["operations"],
        )?;

        let disk_size = Gauge::with_opts(disk_size_opts)?;

        let backup_size = Gauge::with_opts(backup_size_opts)?;

        let ttl_expired_keys_total = IntCounter::with_opts(ttl_expired_keys_total_opts)?;

        Ok(Metrics {
            keys_total,
            operations_total,
            disk_size,
            backup_size,
            ttl_expired_keys_total,
        })
    }
}
