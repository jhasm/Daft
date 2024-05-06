use std::sync::Arc;

use daft_micropartition::MicroPartition;

use crate::compute::partition::partition_ref::{PartitionMetadata, PartitionRef};

#[derive(Debug, Clone)]
pub struct LocalPartitionRef {
    partition: Arc<MicroPartition>,
    metadata: PartitionMetadata,
}

impl LocalPartitionRef {
    pub fn new(partition: Arc<MicroPartition>) -> Self {
        // TODO(Clark): Error handling for size_bytes().
        let metadata =
            PartitionMetadata::new(partition.len(), partition.size_bytes().unwrap().unwrap());
        Self {
            partition,
            metadata,
        }
    }
}

impl PartitionRef for LocalPartitionRef {
    fn metadata(&self) -> PartitionMetadata {
        self.metadata.clone()
    }
    fn partition(&self) -> Arc<MicroPartition> {
        self.partition.clone()
    }
}
