use std::{
    cell::RefCell,
    collections::HashMap,
    pin::Pin,
    rc::Rc,
    sync::{Arc, Mutex},
};

use common_error::DaftResult;
use futures::{stream::FuturesUnordered, Future, StreamExt};

use crate::{
    compute::partition::partition_task_tree::{task_tree_to_state_tree, topological_sort},
    executor::executor::Executor,
};

use super::{
    partition_task::Task,
    partition_task_tree::{PartitionTaskNode, PartitionTaskState},
    virtual_partition::VirtualPartitionSet,
    PartitionRef, PartitionTask,
};

// TODO(Clark): Add streaming partition task scheduler.

#[derive(Debug)]
pub struct BulkPartitionTaskScheduler<T: PartitionRef, E: Executor<T>> {
    state_root: Rc<PartitionTaskState<T>>,
    sorted_state: Vec<Rc<PartitionTaskState<T>>>,
    // running_task_futures: Arc<
    //     tokio::sync::Mutex<
    //         FuturesUnordered<Pin<Box<dyn Future<Output = DaftResult<(usize, Vec<T>)>> + 'a>>>,
    //     >,
    // >,
    inflight_tasks: HashMap<usize, RunningTask<T>>,
    inflight_op_task_count: Vec<usize>,
    // TODO(Clark): Add per-op resource utilization tracking.
    executor: Arc<E>,
}

impl<T: PartitionRef, E: Executor<T>> BulkPartitionTaskScheduler<T, E> {
    pub fn from_state(state_root: Rc<PartitionTaskState<T>>, executor: Arc<E>) -> Self {
        // TODO(Clark): Defer topological sorting of state tree creation until execution?
        let sorted_state = topological_sort(state_root.clone());
        let max_op_id = sorted_state.iter().map(|x| x.op_id()).max().unwrap();
        Self {
            state_root,
            sorted_state,
            // running_task_futures: Arc::new(tokio::sync::Mutex::new(FuturesUnordered::new())),
            inflight_tasks: HashMap::new(),
            inflight_op_task_count: vec![0; max_op_id + 1],
            executor,
        }
    }

    pub fn new(
        task_tree_root: PartitionTaskNode,
        mut leaf_inputs: Vec<VirtualPartitionSet<T>>,
        executor: Arc<E>,
    ) -> Self {
        // TODO(Clark): Defer state tree creation until execution?
        let state_root = task_tree_to_state_tree::<T>(task_tree_root, &mut leaf_inputs);
        assert!(leaf_inputs.len() == 0);
        Self::from_state(state_root, executor)
    }

    fn has_inputs(&self) -> bool {
        self.sorted_state
            .iter()
            .any(|node| node.num_queued_inputs() > 0)
    }

    fn submittable(&self) -> impl Iterator<Item = SubmittableTask<T>> + '_ {
        // TODO(Clark): This will create a lot of redundant Tasks on each scheduling tick, which will have some
        // annoying side-effects such as incrementing the task counter. We should break this into two structs: a
        // submittable, candidate task struct (without a task ID) and a submitted task struct (with task ID).
        self.sorted_state
            .iter()
            .enumerate()
            .flat_map(|(node_idx, t)| match t.as_ref() {
                PartitionTaskState::LeafScan(scan_state) => scan_state
                    .inputs
                    .borrow()
                    .iter()
                    .enumerate()
                    .map(|(input_idx, input)| {
                        let partition_task =
                            PartitionTask::new(vec![input.clone()], scan_state.task_op.clone());
                        SubmittableTask::new(
                            Task::<T>::ScanTask(partition_task),
                            scan_state.op_id,
                            node_idx,
                            input_idx,
                        )
                    })
                    .collect::<Vec<_>>(),
                PartitionTaskState::LeafMemory(memory_state) => memory_state
                    .inputs
                    .borrow()
                    .iter()
                    .enumerate()
                    .map(|(input_idx, input)| {
                        // Our task tree -> state tree translation guarantees that any LeafMemory nodes with None task ops
                        // (i.e. no-op in-memory scans) will have their inputs moved into their outputs before execution,
                        // so we can safely unwrap the Option<PartitionTaksOp>.
                        let task_op = memory_state.task_op.as_ref().unwrap().clone();
                        let partition_task = PartitionTask::new(vec![input.clone()], task_op);
                        SubmittableTask::new(
                            Task::<T>::PartitionTask(partition_task),
                            memory_state.op_id,
                            node_idx,
                            input_idx,
                        )
                    })
                    .collect::<Vec<_>>(),
                PartitionTaskState::Inner(inner_state) => match inner_state.inputs.len() {
                    1 => inner_state.inputs[0]
                        .borrow()
                        .iter()
                        .enumerate()
                        .map(|(input_idx, input)| {
                            let partition_task = PartitionTask::new(
                                vec![input.clone()],
                                inner_state.task_op.clone(),
                            );
                            SubmittableTask::new(
                                Task::<T>::PartitionTask(partition_task),
                                inner_state.op_id,
                                node_idx,
                                input_idx,
                            )
                        })
                        .collect::<Vec<_>>(),
                    2 => inner_state.inputs[0]
                        .borrow()
                        .iter()
                        .zip(inner_state.inputs[1].borrow().iter())
                        .enumerate()
                        .map(|(input_idx, (l, r))| {
                            let partition_task = PartitionTask::new(
                                vec![l.clone(), r.clone()],
                                inner_state.task_op.clone(),
                            );
                            SubmittableTask::new(
                                Task::<T>::PartitionTask(partition_task),
                                inner_state.op_id,
                                node_idx,
                                input_idx,
                            )
                        })
                        .collect::<Vec<_>>(),
                    _ => unreachable!("Only support 1 or 2 inputs"),
                },
            })
    }

    async fn schedule_next_admissible(&self) -> Option<SubmittableTask<T>> {
        let submittable = self.submittable();
        {
            let admissible =
                submittable.filter(|t| self.executor.can_admit(t.task.resource_request()));
            // TODO(Clark): Use a better metric than output queue size.
            // TODO(Clark): Prioritize metadata-only ops.
            admissible.min_by(|x, y| {
                self.sorted_state[x.node_idx]
                    .num_queued_outputs()
                    .cmp(&self.sorted_state[y.node_idx].num_queued_outputs())
            })
        }
    }

    fn schedule_next_submittable(&self) -> Option<SubmittableTask<T>> {
        let submittable = self.submittable();
        // TODO(Clark): Return the most admissable task (smallest resource capacity violation).
        submittable.min_by(|x, y| {
            self.sorted_state[x.node_idx]
                .num_queued_outputs()
                .cmp(&self.sorted_state[y.node_idx].num_queued_outputs())
        })
    }

    fn submit_task(
        &mut self,
        task: SubmittableTask<T>,
    ) -> impl Future<Output = DaftResult<(usize, Vec<T>)>> {
        let task_id = task.task.task_id();
        self.inflight_op_task_count[task.op_id] += 1;
        let node = self.sorted_state[task.node_idx].clone();
        node.pop_input(task.input_idx);
        let running_task = RunningTask::new(node, task_id, task.op_id);
        self.inflight_tasks.insert(task_id, running_task);
        let t = task.task;
        let e = self.executor.clone();
        // self.executor.submit_task(t).await
        async move { e.submit_task(t).await }
        // let fut = Box::pin(self.executor.submit_task(task.task));
        // self.running_task_futures.lock().await.push(fut);
    }

    pub async fn execute(mut self) -> DaftResult<Vec<Vec<T>>> {
        let mut running_task_futures = FuturesUnordered::new();
        while !running_task_futures.is_empty() || self.has_inputs() {
            let next_task = self.schedule_next_admissible().await;
            let next_task = next_task.or_else(|| {
                if running_task_futures.is_empty() {
                    Some(self.schedule_next_submittable().unwrap())
                } else {
                    None
                }
            });
            let fut = if let Some(task) = next_task {
                Some((&mut self).submit_task(task))
            } else {
                None
            };
            if let Some(fut) = fut {
                running_task_futures.push(fut);
            }
            while let Some(result) = running_task_futures.next().await {
                let (task_id, result) = result?;
                let running_task = self.inflight_tasks.remove(&task_id).unwrap();
                self.inflight_op_task_count[running_task.op_id] -= 1;
                running_task
                    .node
                    .outputs()
                    .iter()
                    .zip(result.into_iter())
                    .for_each(|(out, r)| out.borrow_mut().push_front(r));
            }
        }
        Ok(self
            .state_root
            .outputs()
            .into_iter()
            .map(|v| v.borrow_mut().make_contiguous().into())
            .collect::<Vec<_>>())
    }
}

#[derive(Debug)]
struct SubmittableTask<T: PartitionRef> {
    task: Task<T>,
    op_id: usize,
    node_idx: usize,
    input_idx: usize,
}

impl<T: PartitionRef> SubmittableTask<T> {
    fn new(task: Task<T>, op_id: usize, node_idx: usize, input_idx: usize) -> Self {
        Self {
            task,
            op_id,
            node_idx,
            input_idx,
        }
    }
}

#[derive(Debug)]
struct RunningTask<T: PartitionRef> {
    node: Rc<PartitionTaskState<T>>,
    task_id: usize,
    op_id: usize,
}

impl<T: PartitionRef> RunningTask<T> {
    fn new(node: Rc<PartitionTaskState<T>>, task_id: usize, op_id: usize) -> Self {
        Self {
            node,
            task_id,
            op_id,
        }
    }
}
