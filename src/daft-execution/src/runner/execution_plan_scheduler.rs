// use std::marker::PhantomData;

// use futures::Stream;

// use super::execution_stage::ExecutionStage;
// use crate::{compute::partition::PartitionRef, executor::executor::Executor};

// /// Scheduler for execution plans, choosing which exchange ops should run next.
// pub struct ExecutionPlanScheduler<T: PartitionRef, E: Executor<T>> {
//     execution_plan: ExecutionPlan,
//     executor: E,
//     _marker: PhantomData<T>,
// }

// impl<T: PartitionRef, E: Executor<T>> ExecutionPlanScheduler<T, E> {
//     pub fn new(execution_plan: ExecutionPlan, executor: E) -> Self {
//         Self {
//             execution_plan,
//             executor,
//             _marker: PhantomData,
//         }
//     }

//     fn run_stage(&self, stage: ExecutionStage) -> impl Stream<Item = T> {
//         // TODO(Clark): Execute independent (parallel) stages in a smarter order than naive DFS.
//         let inputs = stage
//             .inputs
//             .into_iter()
//             .map(|child| self.run_stage(child))
//             .collect::<Vec<_>>();
//         stage.execute(inputs, self.executor)
//     }

//     pub fn run(&self) -> impl Stream<Item = T> {
//         self.run_stage(self.execution_plan.root)
//     }
// }
