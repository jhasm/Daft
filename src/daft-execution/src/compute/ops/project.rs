use std::sync::Arc;

use common_error::DaftResult;
use daft_dsl::Expr;
use daft_micropartition::MicroPartition;
use daft_plan::ResourceRequest;

use crate::compute::partition::partition_ref::PartitionMetadata;

use super::ops::PartitionTaskOp;

#[derive(Debug)]
pub struct ProjectOp {
    projection: Vec<Expr>,
    resource_request: ResourceRequest,
}

impl PartitionTaskOp for ProjectOp {
    type Input = MicroPartition;

    fn execute(&self, mut inputs: Vec<Arc<Self::Input>>) -> DaftResult<Vec<Arc<MicroPartition>>> {
        assert_eq!(inputs.len(), 1);
        let input = inputs.remove(0);
        input
            .eval_expression_list(self.projection.as_slice())
            .map(|out| vec![Arc::new(out)])
    }

    fn num_outputs(&self) -> usize {
        1
    }

    fn resource_request(&self) -> &ResourceRequest {
        &self.resource_request
    }

    fn resource_request_with_input_metadata(
        &self,
        input_meta: Vec<&PartitionMetadata>,
    ) -> ResourceRequest {
        todo!()
    }

    fn partial_metadata_from_input_metadata(
        &self,
        input_meta: Vec<&PartitionMetadata>,
    ) -> PartitionMetadata {
        todo!()
    }
}
