use std::{collections::HashMap, sync::Arc};

use common_error::DaftResult;
use daft_micropartition::MicroPartition;
use daft_plan::QueryStageOutput;
use itertools::Itertools;

use crate::{
    compute::partition::PartitionRef,
    executor::{
        executor::Executor,
        local::{
            local_executor::{LocalExecutor, SerialExecutor},
            local_partition_ref::LocalPartitionRef,
        },
        resource_manager::ExecutionResources,
    },
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
    // Create local synchronous (single-threaded) executor.
    let executor = Arc::new(SerialExecutor::new());
    run_local(query_stage, psets, executor)
}

pub fn run_local_async(
    query_stage: &QueryStageOutput,
    psets: HashMap<String, Vec<Arc<MicroPartition>>>,
) -> DaftResult<Box<dyn Iterator<Item = DaftResult<Arc<MicroPartition>>> + Send>> {
    // Configure resource capacity.
    let num_cpus: usize = std::thread::available_parallelism().unwrap().into();
    let mut system = sysinfo::System::new_with_specifics(
        sysinfo::RefreshKind::new().with_memory(sysinfo::MemoryRefreshKind::everything()),
    );
    system.refresh_memory();
    let memory_bytes = system.total_memory() as usize;
    let resources = ExecutionResources::new(num_cpus as f64, 0.0, memory_bytes);
    // Create local multithreaded executor.
    let executor = Arc::new(LocalExecutor::new(resources));
    run_local(query_stage, psets, executor)
}

fn run_local<E: Executor<LocalPartitionRef> + 'static>(
    query_stage: &QueryStageOutput,
    psets: HashMap<String, Vec<Arc<MicroPartition>>>,
    executor: Arc<E>,
) -> DaftResult<Box<dyn Iterator<Item = DaftResult<Arc<MicroPartition>>> + Send>> {
    let psets = psets
        .into_iter()
        .map(|(k, v)| {
            Ok((
                k,
                v.into_iter()
                    .map(LocalPartitionRef::try_new)
                    .collect::<DaftResult<Vec<_>>>()?,
            ))
        })
        .collect::<DaftResult<HashMap<_, _>>>()?;
    // Convert query stage to executable stage.
    let (physical_plan, is_final) = match query_stage {
        QueryStageOutput::Partial { physical_plan, .. } => (physical_plan.as_ref(), false),
        QueryStageOutput::Final { physical_plan, .. } => (physical_plan.as_ref(), true),
    };
    let stage = physical_plan_to_stage(physical_plan, is_final, &psets, executor.clone());
    match stage {
        Stage::Exchange(exchange_stage) => {
            let runner = ExchangeStageRunner::new(exchange_stage);
            let out = runner.run()?;
            assert!(out.len() == 1);
            let out = out.into_iter().next().unwrap();
            Ok(Box::new(out.into_iter().map(|part| Ok(part.partition()))))
        }
        Stage::Sink(sink_stage) => {
            let (tx, rx) = std::sync::mpsc::channel::<DaftResult<Vec<LocalPartitionRef>>>();
            let runner = SinkStageRunner::new(sink_stage);
            let executor = executor.clone();
            // TODO(Clark): Figure out why this locks everything up.
            // let handle = std::thread::spawn(move || {
            //     runner.run(tx, executor);
            // });
            // let out = rx.into_iter().map_ok(|v| {
            //     assert!(v.len() == 1);
            //     v.into_iter().next().unwrap().partition()
            // });
            // log::warn!("returning iterator");
            // struct ReceiverIterator<I: Iterator<Item = DaftResult<Arc<MicroPartition>>>> {
            //     rx_iter: I,
            //     join_handle: Option<JoinHandle<()>>,
            // }

            // impl<I: Iterator<Item = DaftResult<Arc<MicroPartition>>>> Iterator for ReceiverIterator<I> {
            //     type Item = DaftResult<Arc<MicroPartition>>;

            //     fn next(&mut self) -> Option<Self::Item> {
            //         let n = self.rx_iter.next();
            //         if n.is_none() && self.join_handle.is_some() {
            //             self.join_handle.take().unwrap().join().unwrap();
            //         }
            //         n
            //     }
            // }
            // Ok(Box::new(ReceiverIterator {
            //     rx_iter: out,
            //     join_handle: Some(handle),
            // }))
            std::thread::spawn(move || {
                runner.run(tx, executor);
            });
            let out = rx
                .into_iter()
                .map_ok(|v| {
                    assert!(v.len() == 1);
                    v.into_iter().next().unwrap().partition()
                })
                .collect::<Vec<_>>();
            Ok(Box::new(out.into_iter()))
        }
    }
}
