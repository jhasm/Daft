use std::sync::Arc;

use common_error::DaftResult;
use daft_micropartition::MicroPartition;
use daft_plan::ResourceRequest;
use daft_scan::ScanTask;

use crate::compute::partition::partition_ref::PartitionMetadata;

use super::ops::PartitionTaskOp;

// pub struct FusedOpBuilder {
//     ops: Vec<Box<dyn PartitionTaskOp<Input = MicroPartition>>>,
//     resource_request: ResourceRequest,
// }

// impl FusedOpBuilder {
//     pub fn new() -> Self {
//         Self {
//             ops: vec![],
//             resource_request: Default::default(),
//         }
//     }

//     pub fn add_op(&mut self, op: Box<dyn PartitionTaskOp<Input = MicroPartition>>) {
//         self.ops.push(op);
//         self.resource_request = self.resource_request.max(op.resource_request());
//     }

//     pub fn can_add_op(&self, op: Box<dyn PartitionTaskOp<Input = MicroPartition>>) -> bool {
//         self.resource_request
//             .is_pipeline_compatible_with(op.resource_request())
//     }

//     pub fn build(self) -> Box<dyn PartitionTaskOp<Input = MicroPartition>> {
//         if self.ops.len() == 1 {
//             self.ops[0]
//         } else {
//             Box::new(FusedPartitionTaskOp::new(self.ops, self.resource_request))
//         }
//     }
// }

// pub enum OpBuilder {
//     ScanOpBuilder(FusedOpBuilder<ScanTask>),
//     PartitionOpBuilder(FusedOpBuilder<MicroPartition>),
// }

pub struct FusedOpBuilder<T> {
    source_op: Arc<dyn PartitionTaskOp<Input = T>>,
    fused_ops: Vec<Arc<dyn PartitionTaskOp<Input = MicroPartition>>>,
    resource_request: ResourceRequest,
}

impl<T: std::fmt::Debug + 'static> FusedOpBuilder<T> {
    pub fn new(source_op: Arc<dyn PartitionTaskOp<Input = T>>) -> Self {
        Self {
            source_op,
            fused_ops: vec![],
            resource_request: Default::default(),
        }
    }

    pub fn add_op(&mut self, op: Arc<dyn PartitionTaskOp<Input = MicroPartition>>) {
        self.resource_request = self.resource_request.max(op.resource_request());
        self.fused_ops.push(op);
    }

    pub fn can_add_op(&self, op: Arc<dyn PartitionTaskOp<Input = MicroPartition>>) -> bool {
        self.resource_request
            .is_pipeline_compatible_with(op.resource_request())
    }

    pub fn build(self) -> Arc<dyn PartitionTaskOp<Input = T>> {
        if self.fused_ops.len() == 0 {
            self.source_op
        } else {
            Arc::new(FusedPartitionTaskOp::<T>::new(
                self.source_op,
                self.fused_ops,
                self.resource_request,
            ))
        }
    }
}

#[derive(Debug)]
pub struct FusedPartitionTaskOp<T> {
    source_op: Arc<dyn PartitionTaskOp<Input = T>>,
    fused_ops: Vec<Arc<dyn PartitionTaskOp<Input = MicroPartition>>>,
    resource_request: ResourceRequest,
}

impl<T> FusedPartitionTaskOp<T> {
    pub fn new(
        source_op: Arc<dyn PartitionTaskOp<Input = T>>,
        fused_ops: Vec<Arc<dyn PartitionTaskOp<Input = MicroPartition>>>,
        resource_request: ResourceRequest,
    ) -> Self {
        Self {
            source_op,
            fused_ops,
            resource_request,
        }
    }
}

impl<T: std::fmt::Debug> PartitionTaskOp for FusedPartitionTaskOp<T> {
    type Input = T;

    fn execute(&self, inputs: Vec<Arc<Self::Input>>) -> DaftResult<Vec<Arc<MicroPartition>>> {
        let mut inputs = self.source_op.execute(inputs)?;
        for op in self.fused_ops.iter() {
            inputs = op.execute(inputs)?;
        }
        Ok(inputs)
    }

    fn num_outputs(&self) -> usize {
        self.fused_ops.last().unwrap().num_outputs()
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
