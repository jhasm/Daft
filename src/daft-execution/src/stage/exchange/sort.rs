use std::{marker::PhantomData, sync::Arc};

use async_trait::async_trait;
use common_error::DaftResult;

use crate::{
    compute::{
        partition::{virtual_partition::VirtualPartitionSet, PartitionRef},
        tree::{
            partition_task_scheduler::BulkPartitionTaskScheduler,
            partition_task_tree::PartitionTaskNode,
        },
    },
    executor::executor::Executor,
};

use super::{exchange::Exchange, ShuffleExchange};

#[derive(Debug)]
pub struct SortExchange<T: PartitionRef, E: Executor<T>> {
    upstream_task_graph: PartitionTaskNode,
    sampling_task_graph: PartitionTaskNode,
    reduce_to_quantiles_task_graph: PartitionTaskNode,
    shuffle_exchange: Box<ShuffleExchange<T, E>>,
    executor: Arc<E>,
    _marker: PhantomData<T>,
}

impl<T: PartitionRef, E: Executor<T>> SortExchange<T, E> {
    pub fn new(
        upstream_task_graph: PartitionTaskNode,
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
            upstream_task_graph,
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
        let upstream_task_scheduler = BulkPartitionTaskScheduler::new(
            self.upstream_task_graph,
            inputs,
            self.executor.clone(),
        );
        let upstream_outs = upstream_task_scheduler.execute().await?;
        assert!(upstream_outs.len() == 1);
        let upstream_outs = upstream_outs
            .into_iter()
            .map(|out| VirtualPartitionSet::PartitionRef(out))
            .collect::<Vec<_>>();
        let sample_task_scheduler = BulkPartitionTaskScheduler::new(
            self.sampling_task_graph,
            upstream_outs.clone(),
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
        assert!(boundaries.len() == 1);
        let boundaries = boundaries.into_iter().next().unwrap();
        let inputs = vec![
            VirtualPartitionSet::PartitionRef(boundaries),
            upstream_outs.into_iter().next().unwrap(),
        ];
        self.shuffle_exchange.run(inputs).await
    }
}
