use std::sync::Arc;

use common_error::DaftResult;
use daft_micropartition::MicroPartition;

use crate::compute::partition::partition_ref::{PartitionMetadata, PartitionRef};

#[derive(Debug, Clone)]
pub struct LocalPartitionRef {
    partition: Arc<MicroPartition>,
    metadata: PartitionMetadata,
}

impl LocalPartitionRef {
    pub fn try_new(partition: Arc<MicroPartition>) -> DaftResult<Self> {
        // TODO(Clark): Error handling for size_bytes().
        let metadata = PartitionMetadata::new(Some(partition.len()), partition.size_bytes()?);
        Ok(Self {
            partition,
            metadata,
        })
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
