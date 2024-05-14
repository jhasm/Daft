use common_error::DaftResult;
use futures::Stream;

use crate::{
    compute::partition::PartitionRef,
    stage::stage::{ExchangeStage, SinkStage},
};

pub struct ExchangeStageRunner<T: PartitionRef> {
    stage: ExchangeStage<T>,
}

impl<T: PartitionRef> ExchangeStageRunner<T> {
    pub fn new(stage: ExchangeStage<T>) -> Self {
        Self { stage }
    }
}

impl<T: PartitionRef> ExchangeStageRunner<T> {
    pub fn run(self) -> DaftResult<Vec<Vec<T>>> {
        log::info!("Running exchange stage: {}", self.stage.stage_id);
        let local = tokio::task::LocalSet::new();
        let runtime = tokio::runtime::Runtime::new().unwrap();
        local.block_on(&runtime, async move {
            let stage = self.stage.op;
            let inputs = self.stage.inputs;
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
    pub fn new(stage: SinkStage<T>) -> Self {
        Self { stage }
    }
}

impl<T: PartitionRef> SinkStageRunner<T> {
    pub fn run(self) -> DaftResult<Vec<Box<dyn Stream<Item = DaftResult<T>> + Unpin>>> {
        log::info!("Running sink stage: {}", self.stage.stage_id);
        let local = tokio::task::LocalSet::new();
        let runtime = tokio::runtime::Runtime::new().unwrap();
        local.block_on(&runtime, async move {
            let stage = self.stage.op;
            let inputs = self.stage.inputs;
            tokio::task::spawn_local(async move { stage.run(inputs).await })
                .await
                .unwrap()
        })
    }
}
