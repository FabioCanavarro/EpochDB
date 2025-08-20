use crate::metrics::Metrics;

pub struct GuardMetricChanged {
    pub keys_total_changed: i64,
    pub ttl_keys_total_changed: i64,
    pub set_operation_total: u64,
    pub rm_operation_total: u64,
    pub inc_freq_operation_total: u64,
    pub get_operation_total: u64,
}

impl GuardMetricChanged {
    pub fn inc_all_metrics(&self) {
        let i = self.keys_total_changed;

        if i > 0 {
            Metrics::inc_amount_keys_total("data", i.unsigned_abs());
            Metrics::inc_amount_keys_total("meta", i.unsigned_abs());
        } else {
            Metrics::dec_amount_keys_total("data", i.unsigned_abs());
            Metrics::dec_amount_keys_total("meta", i.unsigned_abs());
        }

        let i = self.ttl_keys_total_changed;

        if i > 0 {
            Metrics::inc_amount_keys_total("ttl", i.unsigned_abs());
        } else {
            Metrics::dec_amount_keys_total("ttl", i.unsigned_abs());
        }

        Metrics::increment_amount_operations("set", self.set_operation_total);
        Metrics::increment_amount_operations("rm", self.rm_operation_total);
        Metrics::increment_amount_operations("increment_frequency", self.inc_freq_operation_total);
        Metrics::increment_amount_operations("get", self.get_operation_total);
    }
}
