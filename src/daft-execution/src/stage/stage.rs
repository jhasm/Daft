use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};

use daft_micropartition::MicroPartition;
use daft_scan::ScanTask;
use futures::Stream;

use crate::{
    compute::{
        ops::ops::PartitionTaskOp,
        partition::{
            partition_task_tree::PartitionTaskNode,
            virtual_partition::{VirtualPartition, VirtualPartitionSet},
            PartitionRef,
        },
    },
    executor::executor::Executor,
};

use super::{exchange::exchange::Exchange, sink::sink::Sink};

// pub struct ExecutionPlan {
//     pub root: ExecutionStage,
// }

// pub struct ExecutionStage<T: PartitionRef> {
//     pub stage_scheduler: StageScheduler<T>,
//     pub stage_id: usize,
// }

static STAGE_ID_COUNTER: AtomicUsize = AtomicUsize::new(0);

pub struct ExchangeStage<T: PartitionRef> {
    pub op: Box<dyn Exchange<T>>,
    pub inputs: Vec<VirtualPartitionSet<T>>,
    pub stage_id: usize,
}

impl<T: PartitionRef> ExchangeStage<T> {
    pub fn new(op: Box<dyn Exchange<T>>, inputs: Vec<VirtualPartitionSet<T>>) -> Self {
        let stage_id = STAGE_ID_COUNTER.fetch_add(1, Ordering::SeqCst);
        Self {
            op,
            inputs,
            stage_id,
        }
    }
}

pub struct SinkStage<T: PartitionRef> {
    pub op: Box<dyn Sink<T>>,
    pub inputs: Vec<VirtualPartitionSet<T>>,
    pub stage_id: usize,
}

impl<T: PartitionRef> SinkStage<T> {
    pub fn new(op: Box<dyn Sink<T>>, inputs: Vec<VirtualPartitionSet<T>>) -> Self {
        let stage_id = STAGE_ID_COUNTER.fetch_add(1, Ordering::SeqCst);
        Self {
            op,
            inputs,
            stage_id,
        }
    }
}

pub enum Stage<T: PartitionRef> {
    Exchange(ExchangeStage<T>),
    Sink(SinkStage<T>),
}

impl<T: PartitionRef> From<(Box<dyn Exchange<T>>, Vec<VirtualPartitionSet<T>>)> for Stage<T> {
    fn from(value: (Box<dyn Exchange<T>>, Vec<VirtualPartitionSet<T>>)) -> Self {
        let (op, inputs) = value;
        Self::Exchange(ExchangeStage::new(op, inputs))
    }
}

impl<T: PartitionRef> From<(Box<dyn Sink<T>>, Vec<VirtualPartitionSet<T>>)> for Stage<T> {
    fn from(value: (Box<dyn Sink<T>>, Vec<VirtualPartitionSet<T>>)) -> Self {
        let (op, inputs) = value;
        Self::Sink(SinkStage::new(op, inputs))
    }
}

// pub struct StageBuilder {
//     task_tree_buffer: Option<PartitionTaskNode>,
// }

// impl<T: PartitionRef> StageBuilder {
//     pub fn new() -> Self {
//         Self {
//             task_tree_buffer: None,
//         }
//     }

//     pub fn add_scan_task_to_stage(&self, task_op: Arc<dyn PartitionTaskOp<Input = ScanTask>>) {
//         todo!()
//     }

//     pub fn add_partition_task_to_stage(
//         &self,
//         task_op: Arc<dyn PartitionTaskOp<Input = MicroPartition>>,
//     ) {
//         todo!()
//     }

//     pub fn build_with_exchange(
//         self,
//         exchange_op: Box<dyn Exchange<T>>,
//         reduce_task_op: Option<Arc<dyn PartitionTaskOp<Input = MicroPartition>>>,
//     ) -> Stage<T> {
//         todo!()
//     }
// }

// impl<T: PartitionRef, E: Executor<T>> StageScheduler<T> {
//     pub fn execute(&self, executor: Arc<E>)
// }

pub struct ExecutionStage<T: PartitionRef> {
    stage: Stage<T>,
    stage_id: usize,
}

// impl<T: PartitionRef, E: Executor<T>> ExecutionStage<T> {
//     pub fn execute(
//         &self,
//         inputs: Vec<Vec<VirtualPartition<T>>>,
//         executor: E,
//     ) -> impl Stream<Item = T> {
//         self.exchange_op
//     }
// }

// pub struct ExecutionStageBuilder {
//     task_graph_buffer: Option<PartitionTaskNode>,
// }

// impl ExecutionStageBuilder {
//     pub fn new() -> Self {
//         Self {
//             task_graph_buffer: None,
//         }
//     }

//     pub fn add_to_stage(&self, partition_task_spec: PartitionTaskSpec) {
//         self.task_graph_buffer
//         todo!()
//     }

//     pub fn build_with_exchange(self, exchange_op: Box<dyn Exchange>) -> ExecutionStage {
//         todo!()
//     }
// }
