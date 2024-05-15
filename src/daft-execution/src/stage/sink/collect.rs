use std::{marker::PhantomData, sync::Arc};

use async_trait::async_trait;
use common_error::DaftResult;

use crate::{
    compute::{
        partition::{virtual_partition::VirtualPartitionSet, PartitionRef},
        tree::{
            partition_task_scheduler::StreamingPartitionTaskScheduler,
            partition_task_tree::PartitionTaskNode,
        },
    },
    executor::executor::Executor,
};

use super::sink::{Sink, SinkSpec};

#[derive(Debug)]
pub struct CollectSinkSpec<T: PartitionRef> {
    task_graph: PartitionTaskNode,
    _marker: PhantomData<T>,
}

impl<T: PartitionRef> CollectSinkSpec<T> {
    pub fn new(task_graph: PartitionTaskNode) -> Self {
        Self {
            task_graph,
            _marker: PhantomData,
        }
    }
}
impl<T: PartitionRef, E: Executor<T> + 'static> SinkSpec<T, E> for CollectSinkSpec<T> {
    fn to_runnable_sink(self: Box<Self>, executor: Arc<E>) -> Box<dyn Sink<T>> {
        Box::new(CollectSink {
            spec: self,
            executor,
        })
    }
}

pub struct CollectSink<T: PartitionRef, E: Executor<T>> {
    spec: Box<CollectSinkSpec<T>>,
    executor: Arc<E>,
}

#[async_trait(?Send)]
impl<T: PartitionRef, E: Executor<T> + 'static> Sink<T> for CollectSink<T, E> {
    async fn run(
        self: Box<Self>,
        inputs: Vec<VirtualPartitionSet<T>>,
        output_channel: std::sync::mpsc::Sender<DaftResult<Vec<T>>>,
    ) {
        let task_scheduler = StreamingPartitionTaskScheduler::new(
            self.spec.task_graph,
            inputs,
            output_channel,
            self.executor.clone(),
        );
        task_scheduler.execute().await;
    }
}
