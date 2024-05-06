use std::{
    collections::HashMap,
    pin::Pin,
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
pub struct BulkPartitionTaskScheduler<'a, T: PartitionRef + Send + 'a, E: Executor<T> + 'a> {
    state_root: Arc<PartitionTaskState<T>>,
    sorted_state: Vec<Arc<PartitionTaskState<T>>>,
    running_task_futures: Arc<
        tokio::sync::Mutex<
            FuturesUnordered<Pin<Box<dyn Future<Output = DaftResult<(usize, Vec<T>)>> + 'a>>>,
        >,
    >,
    inflight_tasks: Arc<Mutex<HashMap<usize, RunningTask<T>>>>,
    inflight_op_task_count: Arc<Mutex<Vec<usize>>>,
    // TODO(Clark): Add per-op resource utilization tracking.
    executor: Arc<E>,
}

impl<'a, T: PartitionRef + Send, E: Executor<T>> BulkPartitionTaskScheduler<'a, T, E> {
    pub fn new(
        task_tree_root: PartitionTaskNode,
        mut leaf_inputs: Vec<VirtualPartitionSet<T>>,
        executor: Arc<E>,
    ) -> Self {
        let state_root = task_tree_to_state_tree::<T>(task_tree_root, &mut leaf_inputs);
        assert!(leaf_inputs.len() == 0);
        let sorted_state = topological_sort(state_root.clone());
        let max_op_id = sorted_state.iter().map(|x| x.op_id()).max().unwrap();
        Self {
            state_root,
            sorted_state,
            running_task_futures: Arc::new(tokio::sync::Mutex::new(FuturesUnordered::new())),
            inflight_tasks: Arc::new(Mutex::new(HashMap::new())),
            inflight_op_task_count: Arc::new(Mutex::new(vec![0; max_op_id + 1])),
            executor,
        }
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
                    .lock()
                    .unwrap()
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
                    .lock()
                    .unwrap()
                    .iter()
                    .enumerate()
                    .map(|(input_idx, input)| {
                        let partition_task =
                            PartitionTask::new(vec![input.clone()], memory_state.task_op.clone());
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
                        .lock()
                        .unwrap()
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
                        .lock()
                        .unwrap()
                        .iter()
                        .zip(inner_state.inputs[1].lock().unwrap().iter())
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

    fn schedule_next_admissible(&self) -> Option<SubmittableTask<T>> {
        let submittable = self.submittable();
        let admissible = submittable.filter(|t| self.executor.can_admit(t.task.resource_request()));
        // TODO(Clark): Use a better metric than output queue size.
        admissible.min_by(|x, y| {
            self.sorted_state[x.node_idx]
                .num_queued_outputs()
                .cmp(&self.sorted_state[y.node_idx].num_queued_outputs())
        })
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

    async fn submit_task(&'a self, task: SubmittableTask<T>) {
        let task_id = task.task.task_id();
        self.inflight_op_task_count.lock().unwrap()[task.op_id] += 1;
        let node = self.sorted_state[task.node_idx].clone();
        node.pop_input(task.input_idx);
        let running_task = RunningTask::new(node, task_id, task.op_id);
        self.inflight_tasks
            .lock()
            .unwrap()
            .insert(task_id, running_task);
        let fut = Box::pin(self.executor.submit_task(task.task));
        self.running_task_futures.lock().await.push(fut);
    }

    pub async fn execute(self) -> DaftResult<Vec<VirtualPartitionSet<T>>> {
        // TODO(Clark):
        // - Need to support partition task tree building during planning.
        while !self.running_task_futures.lock().await.is_empty() || self.has_inputs() {
            let next_task = self.schedule_next_admissible();
            if let Some(task) = next_task {
                self.submit_task(task).await;
            } else if self.inflight_tasks.lock().unwrap().len() == 0 {
                // Ensure liveness by always running at least one task.
                let task = self.schedule_next_submittable().unwrap();
                self.submit_task(task).await;
            }
            while let Some(result) = self.running_task_futures.lock().await.next().await {
                let (task_id, result) = result?;
                let running_task = self
                    .inflight_tasks
                    .lock()
                    .unwrap()
                    .remove(&task_id)
                    .unwrap();
                self.inflight_op_task_count.lock().unwrap()[running_task.op_id] -= 1;
                running_task
                    .node
                    .outputs()
                    .iter_mut()
                    .zip(result.into_iter())
                    .map(|(out, r)| out.lock().unwrap().push_front(r));
            }
        }
        Ok(self
            .state_root
            .outputs()
            .into_iter()
            .map(|v| VirtualPartitionSet::PartitionRef(v.lock().unwrap().make_contiguous().into()))
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
    node: Arc<PartitionTaskState<T>>,
    task_id: usize,
    op_id: usize,
}

impl<T: PartitionRef> RunningTask<T> {
    fn new(node: Arc<PartitionTaskState<T>>, task_id: usize, op_id: usize) -> Self {
        Self {
            node,
            task_id,
            op_id,
        }
    }
}
