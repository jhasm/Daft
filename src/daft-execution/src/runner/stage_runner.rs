use common_error::DaftResult;
use futures::Stream;

use crate::{
    compute::partition::{
        virtual_partition::{VirtualPartition, VirtualPartitionSet},
        PartitionRef,
    },
    stage::stage::{ExchangeStage, ExecutionStage, SinkStage},
};

// pub trait StageRunner<T: PartitionRef> {
//     type Output;

//     fn run(stage: ExecutionStage<T>, inputs: Vec<VirtualPartitionSet<T>>) -> Self::Output;
// }

// pub struct SinkStageRunner {}

// impl<T: PartitionRef> StageRunner<T> for SinkStageRunner {
//     type Output = impl Stream<Item = T>;

//     fn run(stage: ExecutionStage<T>, inputs: Vec<VirtualPartitionSet<T>>) -> Self::Output {
//         futures::stream::iter([])
//     }
// }

// pub struct ExchangeStageRunner {}

// impl<T: PartitionRef> StageRunner<T> for ExchangeStageRunner {
//     type Output = Vec<T>;

//     fn run(stage: ExecutionStage<T>, inputs: Vec<VirtualPartitionSet<T>>) -> Self::Output {
//         todo!()
//     }
// }
pub struct ExchangeStageRunner<T: PartitionRef> {
    stage: ExchangeStage<T>,
}

impl<T: PartitionRef> ExchangeStageRunner<T> {
    fn run(self, inputs: Vec<VirtualPartitionSet<T>>) -> DaftResult<Vec<Vec<T>>> {
        let local = tokio::task::LocalSet::new();
        let runtime = tokio::runtime::Runtime::new().unwrap();
        local.block_on(&runtime, async move {
            let stage = self.stage.op;
            tokio::task::spawn_local(async move { stage.run(inputs).await })
                .await
                .unwrap()
        })
    }
}

pub struct SinkStageRunner<T: PartitionRef> {
    stage: SinkStage<T>,
}

impl<T: PartitionRef> SinkStageRunner<T> {
    fn run(
        self,
        inputs: Vec<VirtualPartitionSet<T>>,
    ) -> DaftResult<Vec<Box<dyn Stream<Item = DaftResult<T>>>>> {
        let local = tokio::task::LocalSet::new();
        let runtime = tokio::runtime::Runtime::new().unwrap();
        local.block_on(&runtime, async move {
            let stage = self.stage.op;
            tokio::task::spawn_local(async move { stage.run(inputs).await })
                .await
                .unwrap()
        })
    }
}
