use std::sync::Arc;

use common_error::DaftResult;
use daft_micropartition::MicroPartition;
use daft_plan::ResourceRequest;

use crate::compute::partition::partition_ref::PartitionMetadata;

pub trait PartitionTaskOp: std::fmt::Debug + Send + Sync {
    type Input;

    fn execute(&self, inputs: Vec<Arc<Self::Input>>) -> DaftResult<Vec<Arc<MicroPartition>>>;
    fn num_outputs(&self) -> usize {
        1
    }
    fn num_inputs(&self) -> usize {
        1
    }
    fn resource_request(&self) -> &ResourceRequest;
    fn resource_request_with_input_metadata(
        &self,
        input_meta: &[PartitionMetadata],
    ) -> ResourceRequest {
        self.resource_request()
            .or_memory_bytes(input_meta.iter().map(|m| m.size_bytes).sum())
    }
    fn partial_metadata_from_input_metadata(
        &self,
        input_meta: &[PartitionMetadata],
    ) -> PartitionMetadata;
    fn name(&self) -> &str;
}
