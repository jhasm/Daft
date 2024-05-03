use std::{marker::PhantomData, sync::Arc};

use async_trait::async_trait;
use futures::Stream;

use crate::{
    compute::partition::{
        partition_task_tree::PartitionTaskNode, virtual_partition::VirtualPartitionSet,
        PartitionRef,
    },
    executor::executor::Executor,
};

use super::sink::Sink;

#[derive(Debug)]
pub struct CollectSink<T: PartitionRef, E: Executor<T>> {
    task_graph: PartitionTaskNode,
    executor: Arc<E>,
    _marker: PhantomData<T>,
}

#[async_trait]
impl<T: PartitionRef + Send + Sync, E: Executor<T> + Send + Sync> Sink<T> for CollectSink<T, E> {
    fn run(&self, inputs: Vec<VirtualPartitionSet<T>>) -> Vec<Box<dyn Stream<Item = T>>> {
        todo!()
    }
}
