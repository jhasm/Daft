use futures::Stream;

use crate::{
    compute::partition::{
        virtual_partition::{VirtualPartition, VirtualPartitionSet},
        PartitionRef,
    },
    stage::stage::ExecutionStage,
};

pub trait StageRunner<T: PartitionRef> {
    type Output;

    fn run(stage: ExecutionStage<T>, inputs: Vec<VirtualPartitionSet<T>>) -> Self::Output;
}

pub struct SinkStageRunner {}

impl<T: PartitionRef> StageRunner<T> for SinkStageRunner {
    type Output = impl Stream<Item = T>;

    fn run(stage: ExecutionStage<T>, inputs: Vec<VirtualPartitionSet<T>>) -> Self::Output {
        futures::stream::iter([])
    }
}

pub struct ExchangeStageRunner {}

impl<T: PartitionRef> StageRunner<T> for ExchangeStageRunner {
    type Output = Vec<T>;

    fn run(stage: ExecutionStage<T>, inputs: Vec<VirtualPartitionSet<T>>) -> Self::Output {
        todo!()
    }
}
