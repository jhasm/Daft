use std::sync::Arc;

use common_error::DaftResult;
use daft_dsl::{Expr, ExprRef};
use daft_micropartition::MicroPartition;
use daft_plan::ResourceRequest;

use crate::compute::partition::partition_ref::PartitionMetadata;

use super::ops::PartitionTaskOp;

#[derive(Debug)]
pub struct FilterOp {
    predicate: Vec<ExprRef>,
    resource_request: ResourceRequest,
}

impl FilterOp {
    pub fn new(predicate: Vec<ExprRef>) -> Self {
        Self {
            predicate,
            resource_request: Default::default(),
        }
    }
}

impl PartitionTaskOp for FilterOp {
    type Input = MicroPartition;

    fn execute(&self, inputs: Vec<Arc<Self::Input>>) -> DaftResult<Vec<Arc<MicroPartition>>> {
        assert_eq!(inputs.len(), 1);
        let input = inputs.into_iter().next().unwrap();
        let out = input.filter(self.predicate.as_slice())?;
        Ok(vec![Arc::new(out)])
    }

    fn num_outputs(&self) -> usize {
        1
    }

    fn resource_request(&self) -> &ResourceRequest {
        &self.resource_request
    }

    fn resource_request_with_input_metadata(
        &self,
        input_meta: &[PartitionMetadata],
    ) -> ResourceRequest {
        todo!()
    }

    fn partial_metadata_from_input_metadata(
        &self,
        input_meta: &[PartitionMetadata],
    ) -> PartitionMetadata {
        todo!()
    }
}
