use std::{marker::PhantomData, sync::Arc};

use async_trait::async_trait;
use common_error::DaftResult;
use futures::Future;

use crate::{
    compute::{
        ops::sort::BoundarySamplingOp,
        partition::{
            partition_task_scheduler::BulkPartitionTaskScheduler,
            partition_task_tree::{PartitionTaskLeafMemoryNode, PartitionTaskNode},
            virtual_partition::VirtualPartitionSet,
            PartitionRef,
        },
    },
    executor::executor::Executor,
};

use super::{exchange::Exchange, ShuffleExchange};

#[derive(Debug)]
pub struct SortExchange<T: PartitionRef, E: Executor<T>> {
    sampling_task_graph: PartitionTaskNode,
    reduce_to_quantiles_task_graph: PartitionTaskNode,
    shuffle_exchange: Box<ShuffleExchange<T, E>>,
    executor: Arc<E>,
    _marker: PhantomData<T>,
}

impl<T: PartitionRef, E: Executor<T>> SortExchange<T, E> {
    pub fn new(
        sampling_task_graph: PartitionTaskNode,
        reduce_to_quantiles_task_graph: PartitionTaskNode,
        map_task_graph: PartitionTaskNode,
        reduce_task_graph: PartitionTaskNode,
        executor: Arc<E>,
    ) -> Self {
        let shuffle_exchange = Box::new(ShuffleExchange::new(
            map_task_graph,
            reduce_task_graph,
            executor.clone(),
        ));
        Self {
            sampling_task_graph,
            reduce_to_quantiles_task_graph,
            shuffle_exchange,
            executor,
            _marker: PhantomData,
        }
    }
}

#[async_trait(?Send)]
impl<T: PartitionRef, E: Executor<T>> Exchange<T> for SortExchange<T, E> {
    async fn run(self: Box<Self>, inputs: Vec<VirtualPartitionSet<T>>) -> DaftResult<Vec<Vec<T>>> {
        assert!(inputs.len() == 1);
        let sample_task_scheduler = BulkPartitionTaskScheduler::new(
            self.sampling_task_graph,
            inputs.clone(),
            self.executor.clone(),
        );
        let sample_outs = sample_task_scheduler.execute().await?;
        let sample_outs = sample_outs
            .into_iter()
            .map(|out| VirtualPartitionSet::PartitionRef(out))
            .collect::<Vec<_>>();

        let quantiles_task_scheduler = BulkPartitionTaskScheduler::new(
            self.reduce_to_quantiles_task_graph,
            sample_outs,
            self.executor.clone(),
        );
        let boundaries = quantiles_task_scheduler.execute().await?;
        let boundaries = boundaries
            .into_iter()
            .map(|mut bounds| {
                assert!(bounds.len() == 1);
                bounds.remove(0)
            })
            .collect::<Vec<_>>();
        let inputs = vec![
            VirtualPartitionSet::PartitionRef(boundaries),
            inputs.into_iter().next().unwrap(),
        ];
        self.shuffle_exchange.run(inputs).await
    }
}
