use std::error::Error;

use prometheus::{Gauge, IntCounterVec, IntGauge, IntGaugeVec, Opts};

#[derive(Debug)]
pub struct Metrics {
    pub keys_total: IntGaugeVec,
    pub operations_total: IntCounterVec,
    pub disk_size: Gauge,
    pub backup_size: Gauge,
    pub ttl_expired_keys_total: IntGauge,
    pub cache_hits_total: IntGauge,
    pub cache_misses_total: IntGauge,
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

        let cache_hits_opts = Opts::new("epochdb_cache_hits_total", "Total amount of cache hits");

        let cache_misses_opts =
            Opts::new("epochdb_cache_misses_total", "Total amount of cache misses");

        let keys_total = IntGaugeVec::new(keys_total_opts, &["data", "meta", "ttl"])?;

        let operations_total = IntCounterVec::new(
            operations_total_opts,
            &["set", "get", "rm", "increment_frequency"],
        )?;

        let disk_size = Gauge::with_opts(disk_size_opts)?;

        let backup_size = Gauge::with_opts(backup_size_opts)?;

        let ttl_expired_keys_total = IntGauge::with_opts(ttl_expired_keys_total_opts)?;

        let cache_hits_total = IntGauge::with_opts(cache_hits_opts)?;

        let cache_misses_total = IntGauge::with_opts(cache_misses_opts)?;

        Ok(Metrics {
            keys_total,
            operations_total,
            disk_size,
            backup_size,
            ttl_expired_keys_total,
            cache_hits_total,
            cache_misses_total,
        })
    }
}
