use std::sync::Arc;

use daft_micropartition::MicroPartition;

pub trait PartitionRef: std::fmt::Debug + Clone + 'static {
    fn metadata(&self) -> PartitionMetadata;
    fn partition(&self) -> Arc<MicroPartition>;
}

#[derive(Debug, Clone)]
pub struct PartitionMetadata {
    num_rows: usize,
    size_bytes: usize,
    // part_col_stats: PartitionColumnStats,
    // execution_stats: ExecutionStats,
}

impl PartitionMetadata {
    pub fn new(
        num_rows: usize,
        size_bytes: usize,
        // part_col_stats: PartitionColumnStats,
        // execution_stats: ExecutionStats,
    ) -> Self {
        Self {
            num_rows,
            size_bytes,
            // part_col_stats,
            // execution_stats,
        }
    }
}

#[derive(Debug)]
pub struct PartitionColumnStats {}

#[derive(Debug)]
pub struct ExecutionStats {
    wall_time_s: f64,
    cpu_time_s: f64,
    max_rss_bytes: usize,
    node_id: String,
    partition_id: String,
}
