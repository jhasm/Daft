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
pub struct ShuffleExchange<T: PartitionRef + Send, E: Executor<T>> {
    map_task_graph: PartitionTaskNode,
    reduce_task_graph: PartitionTaskNode,
    executor: Arc<E>,
    _marker: PhantomData<T>,
}

#[async_trait]
impl<T: PartitionRef + Send + Sync, E: Executor<T> + Send + Sync> Exchange<T>
    for ShuffleExchange<T, E>
{
    async fn run(
        self,
        inputs: Vec<VirtualPartitionSet<T>>,
    ) -> DaftResult<Vec<VirtualPartitionSet<T>>> {
        let map_task_scheduler =
            BulkPartitionTaskScheduler::new(self.map_task_graph, inputs, self.executor.clone());
        let map_outs = map_task_scheduler.execute().await?;
        let map_outs = map_outs
            .into_iter()
            .map(|vps| match vps {
                VirtualPartitionSet::PartitionRef(prs) => prs,
                _ => unreachable!("Map outputs must be partition references"),
            })
            .collect::<Vec<_>>();
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
