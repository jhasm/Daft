use std::{marker::PhantomData, sync::Arc};

use async_trait::async_trait;
use common_error::DaftResult;
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

#[async_trait(?Send)]
impl<T: PartitionRef, E: Executor<T>> Sink<T> for CollectSink<T, E> {
    async fn run(
        self: Box<Self>,
        inputs: Vec<VirtualPartitionSet<T>>,
    ) -> DaftResult<Vec<Box<dyn Stream<Item = DaftResult<T>>>>> {
        todo!()
    }
}
