use std::{marker::PhantomData, sync::Arc};

use async_trait::async_trait;
use common_error::DaftResult;
use futures::Future;

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
pub struct ShuffleExchange<T: PartitionRef, E: Executor<T>> {
    map_task_graph: PartitionTaskNode,
    reduce_task_graph: PartitionTaskNode,
    executor: Arc<E>,
    _marker: PhantomData<T>,
}

impl<T: PartitionRef, E: Executor<T>> ShuffleExchange<T, E> {
    pub fn new(
        map_task_graph: PartitionTaskNode,
        reduce_task_graph: PartitionTaskNode,
        executor: Arc<E>,
    ) -> Self {
        Self {
            map_task_graph,
            reduce_task_graph,
            executor,
            _marker: PhantomData,
        }
    }
}

#[async_trait(?Send)]
impl<T: PartitionRef, E: Executor<T>> Exchange<T> for ShuffleExchange<T, E> {
    async fn run(self: Box<Self>, inputs: Vec<VirtualPartitionSet<T>>) -> DaftResult<Vec<Vec<T>>> {
        let map_task_scheduler =
            BulkPartitionTaskScheduler::new(self.map_task_graph, inputs, self.executor.clone());
        let map_outs = map_task_scheduler.execute().await?;
        let reduce_ins = transpose_map_outputs(map_outs);
        let reduce_ins = reduce_ins
            .into_iter()
            .map(|parts| VirtualPartitionSet::PartitionRef(parts))
            .collect::<Vec<_>>();
        let reduce_task_scheduler = BulkPartitionTaskScheduler::new(
            self.reduce_task_graph,
            reduce_ins,
            self.executor.clone(),
        );
        reduce_task_scheduler.execute().await
    }
}

fn transpose_map_outputs<T: PartitionRef>(map_outs: Vec<Vec<T>>) -> Vec<Vec<T>> {
    assert!(map_outs.len() > 0);
    let n = map_outs[0].len();
    let mut iters = map_outs
        .into_iter()
        .map(|out| out.into_iter())
        .collect::<Vec<_>>();
    (0..n)
        .map(|_| {
            iters
                .iter_mut()
                .map(|out| out.next().unwrap())
                .collect::<Vec<_>>()
        })
        .collect::<Vec<_>>()
}
