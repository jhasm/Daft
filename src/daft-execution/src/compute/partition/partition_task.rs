use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};

use common_error::DaftResult;
use daft_micropartition::MicroPartition;
use daft_plan::ResourceRequest;
use daft_scan::ScanTask;

use crate::compute::ops::ops::PartitionTaskOp;

use super::{
    partition_ref::{PartitionMetadata, PartitionRef},
    virtual_partition::VirtualPartition,
};

static TASK_ID_COUNTER: AtomicUsize = AtomicUsize::new(0);

// pub struct PartitionTaskSpec<T> {
//     task_op: Arc<dyn PartitionTaskOp<Input = T>>,
//     resource_request: ResourceRequest,
// }

// impl<T> PartitionTaskSpec<T> {
//     pub fn resource_request(&self) -> &ResourceRequest {
//         &self.resource_request
//     }

//     pub fn to_task(&self, inputs: Vec<VirtualPartition<T>>) -> Box<dyn Task<T>> {}
// }

// TODO(Clark): Break into enum for unary and binary tasks?
// TDOO(Clark): Static binding for input type?

// pub trait Task<T: PartitionRef> {
//     fn resource_request(&self) -> &ResourceRequest;
//     fn partial_metadata(&self) -> &PartitionMetadata;
//     fn task_id(&self) -> usize;
//     fn into_executable(
//         self,
//     ) -> (
//         Vec<VirtualPartition<T>>,
//         Arc<dyn PartitionTaskOp<Input = T>>,
//     );
// }

// pub trait Task<V: VirtualPartition> {
//     fn resource_request(&self) -> &ResourceRequest;
//     fn partial_metadata(&self) -> &PartitionMetadata;
//     fn task_id(&self) -> usize;
//     fn into_executable(self) -> (Vec<V>, Arc<dyn PartitionTaskOp<Input = V::TaskOpInput>>);
// }

#[derive(Debug)]
pub enum Task<T: PartitionRef> {
    ScanTask(PartitionTask<Arc<ScanTask>>),
    PartitionTask(PartitionTask<T>),
}

impl<T: PartitionRef> Task<T> {
    pub fn resource_request(&self) -> &ResourceRequest {
        match self {
            Self::ScanTask(pt) => pt.resource_request(),
            Self::PartitionTask(pt) => pt.resource_request(),
        }
    }

    pub fn task_id(&self) -> usize {
        match self {
            Self::ScanTask(pt) => pt.task_id(),
            Self::PartitionTask(pt) => pt.task_id(),
        }
    }
}

#[derive(Debug)]
pub struct PartitionTask<V: VirtualPartition> {
    inputs: Vec<V>,
    task_op: Arc<dyn PartitionTaskOp<Input = V::TaskOpInput>>,
    resource_request: ResourceRequest,
    partial_metadata: PartitionMetadata,
    task_id: usize,
}

impl<V: VirtualPartition> PartitionTask<V> {
    pub fn new(inputs: Vec<V>, task_op: Arc<dyn PartitionTaskOp<Input = V::TaskOpInput>>) -> Self {
        let task_id = TASK_ID_COUNTER.fetch_add(1, Ordering::SeqCst);
        let input_metadata = inputs.iter().map(|vp| vp.metadata()).collect::<Vec<_>>();
        let resource_request = task_op.resource_request_with_input_metadata(&input_metadata);
        let partial_metadata = task_op.partial_metadata_from_input_metadata(&input_metadata);
        Self {
            inputs,
            task_op,
            resource_request,
            partial_metadata,
            task_id,
        }
    }

    pub fn resource_request(&self) -> &ResourceRequest {
        &self.resource_request
    }

    pub fn partial_metadata(&self) -> &PartitionMetadata {
        &self.partial_metadata
    }

    pub fn task_id(&self) -> usize {
        self.task_id
    }

    pub fn into_executable(
        self,
    ) -> (
        Vec<V>,
        Arc<dyn PartitionTaskOp<Input = V::TaskOpInput>>,
        ResourceRequest,
    ) {
        (self.inputs, self.task_op, self.resource_request)
    }

    pub fn execute(self) -> DaftResult<Vec<Arc<MicroPartition>>> {
        let inputs = self
            .inputs
            .into_iter()
            .map(|input| input.partition())
            .collect::<Vec<_>>();
        self.task_op.execute(inputs)
    }
}

// pub struct PartitionTask<T: PartitionRef> {
//     inputs: Vec<VirtualPartition<T>>,
//     task_op: Arc<dyn PartitionTaskOp<Input = T>>,
//     resource_request: ResourceRequest,
//     partial_metadata: PartitionMetadata,
//     task_id: usize,
// }

// impl<T: PartitionRef> PartitionTask<T> {
//     pub fn new(
//         inputs: Vec<VirtualPartition<T>>,
//         task_op: Arc<dyn PartitionTaskOp<Input = T>>,
//     ) -> Self {
//         let task_id = TASK_ID_COUNTER.fetch_add(1, Ordering::SeqCst);
//         let input_metadata = inputs.iter().map(|vp| vp.metadata()).collect::<Vec<_>>();
//         let resource_request = task_op.resource_request_with_input_metadata(&input_metadata);
//         let partial_metadata = task_op.partial_metadata_from_input_metadata(&input_metadata);
//         Self {
//             inputs,
//             task_op,
//             resource_request,
//             partial_metadata,
//             task_id,
//         }
//     }
// }

// impl<T: PartitionRef> Task<T> for PartitionTask<T> {
//     fn resource_request(&self) -> &ResourceRequest {
//         &self.resource_request
//     }

//     fn partial_metadata(&self) -> &PartitionMetadata {
//         &self.partial_metadata
//     }

//     fn task_id(&self) -> usize {
//         self.task_id
//     }

//     fn into_executable(self) -> (Vec<VirtualPartition<T>>, Arc<dyn PartitionTaskOp<Input = T>>) {
//         (self.inputs, self.task_op)
//     }
// }
