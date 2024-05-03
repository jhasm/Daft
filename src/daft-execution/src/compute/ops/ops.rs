use std::sync::Arc;

use common_error::DaftResult;
use daft_micropartition::MicroPartition;
use daft_plan::ResourceRequest;

use crate::compute::partition::partition_ref::PartitionMetadata;

// pub trait TaskOp {
//     fn execute(&self) -> DaftResult<Vec<MicroPartition>>;
//     fn resource_request(&self) -> &ResourceRequest;
// }

pub trait PartitionTaskOp: std::fmt::Debug + Send + Sync {
    type Input;

    fn execute(&self, inputs: Vec<Arc<Self::Input>>) -> DaftResult<Vec<Arc<MicroPartition>>>;
    fn num_outputs(&self) -> usize;
    fn resource_request(&self) -> &ResourceRequest;
    fn resource_request_with_input_metadata(
        &self,
        input_meta: Vec<&PartitionMetadata>,
    ) -> ResourceRequest;
    fn partial_metadata_from_input_metadata(
        &self,
        input_meta: Vec<&PartitionMetadata>,
    ) -> PartitionMetadata;
}

// pub trait LeafTaskOp {
//     fn execute(&self, inputs: Vec<ScanTask>) -> DaftResult<Vec<MicroPartition>>;
//     fn resource_request(&self) -> &ResourceRequest;
// }
