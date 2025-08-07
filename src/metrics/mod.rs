use prometheus::{Gauge, IntGauge, IntGaugeVec, Opts};

pub struct Metrics {
    pub keys_total: IntGaugeVec,
    pub operations_total: IntGaugeVec,
    pub disk_size: Gauge,
    pub backup_size: Gauge,
    pub ttl_expired_keys_total: IntGauge,
    pub cache_hits_total: IntGauge,
    pub cache_misses_total: IntGauge
}

impl Metrics {
    fn new() -> Self {
    }
}
