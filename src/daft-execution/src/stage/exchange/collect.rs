use std::{marker::PhantomData, sync::Arc};

use async_trait::async_trait;
use common_error::DaftResult;

use crate::{
    compute::partition::{
        partition_task_scheduler::BulkPartitionTaskScheduler,
        partition_task_tree::PartitionTaskNode, virtual_partition::VirtualPartitionSet,
        PartitionRef,
    },
    executor::executor::Executor,
};

use super::exchange::Exchange;

#[derive(Debug)]
pub struct CollectExchange<T: PartitionRef, E: Executor<T>> {
    task_graph: PartitionTaskNode,
    executor: Arc<E>,
    _marker: PhantomData<T>,
}

#[async_trait]
impl<T: PartitionRef + Send + Sync, E: Executor<T> + Send + Sync> Exchange<T>
    for CollectExchange<T, E>
{
    async fn run(
        self,
        inputs: Vec<VirtualPartitionSet<T>>,
    ) -> DaftResult<Vec<VirtualPartitionSet<T>>> {
        let task_scheduler =
            BulkPartitionTaskScheduler::new(self.task_graph, inputs, self.executor.clone());
        task_scheduler.execute().await
    }
}
