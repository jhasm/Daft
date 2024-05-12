use std::{collections::HashMap, sync::Arc};

use common_error::DaftResult;
use daft_micropartition::MicroPartition;
use daft_plan::{PhysicalPlan, QueryStageOutput};
use futures::StreamExt;

use crate::{
    compute::partition::PartitionRef,
    executor::local::{local_executor::SerialExecutor, local_partition_ref::LocalPartitionRef},
    runner::{
        stage_planner::physical_plan_to_stage,
        stage_runner::{ExchangeStageRunner, SinkStageRunner},
    },
    stage::stage::Stage,
};

pub fn run_local_sync(
    query_stage: &QueryStageOutput,
    psets: HashMap<String, Vec<Arc<MicroPartition>>>,
) -> DaftResult<Box<dyn Iterator<Item = DaftResult<Arc<MicroPartition>>> + Send>> {
    let executor = Arc::new(SerialExecutor::new());
    let psets = psets
        .into_iter()
        .map(|(k, v)| {
            (
                k,
                v.into_iter()
                    .map(LocalPartitionRef::new)
                    .collect::<Vec<_>>(),
            )
        })
        .collect::<HashMap<_, _>>();
    let stage = physical_plan_to_stage(query_stage, &psets, executor.clone());
    match stage {
        Stage::Exchange(exchange_stage) => {
            let runner = ExchangeStageRunner::new(exchange_stage);
            let out = runner.run()?;
            assert!(out.len() == 1);
            let out = out.into_iter().next().unwrap();
            Ok(Box::new(out.into_iter().map(|part| Ok(part.partition()))))
        }
        Stage::Sink(sink_stage) => {
            let runner = SinkStageRunner::new(sink_stage);
            let out = runner.run()?;
            assert!(out.len() == 1);
            todo!()
        }
    }
}
