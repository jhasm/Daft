use std::{collections::HashMap, rc::Rc, sync::Arc, time::Duration};

use common_error::DaftResult;
use futures::{stream::FuturesUnordered, Future};
use tokio_stream::StreamExt;

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
        log::debug!(
            "Building task tree state on {} leaf inputs: ",
            leaf_inputs.len()
        );
        let state_root = task_tree_to_state_tree::<T>(task_tree_root, &mut leaf_inputs);
        assert!(
            leaf_inputs.len() == 0,
            "leaf inputs should be empty, but has {} left",
            leaf_inputs.len()
        );
        Self::from_state(state_root, executor)
    }

    fn num_queued_inputs(&self) -> usize {
        self.sorted_state
            .iter()
            .map(|node| node.num_queued_inputs())
            .sum()
    }

    fn has_inputs(&self) -> bool {
        self.num_queued_inputs() > 0
    }

    #[allow(unused)]
    fn submittable_unordered(&self) -> impl Iterator<Item = SubmittableTask<T>> + '_ {
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
                        )
                    })
                    .collect::<Vec<_>>(),
                PartitionTaskState::LeafMemory(memory_state) => {
                    let zipped_inputs = MultiZip(
                        memory_state
                            .inputs
                            .iter()
                            .map(|inner| {
                                inner
                                    .borrow()
                                    .iter()
                                    .map(|input| input.clone())
                                    .collect::<Vec<_>>()
                                    .into_iter()
                            })
                            .collect::<Vec<_>>(),
                    );
                    zipped_inputs
                        .into_iter()
                        .enumerate()
                        .map(|(input_idx, inputs)| {
                            // Our task tree -> state tree translation guarantees that any LeafMemory nodes with None task ops
                            // (i.e. no-op in-memory scans) will have their inputs moved into their outputs before execution,
                            // so we can safely unwrap the Option<PartitionTaksOp>.
                            let task_op = memory_state.task_op.as_ref().unwrap().clone();
                            let partition_task = PartitionTask::new(inputs, task_op);
                            SubmittableTask::new(
                                Task::<T>::PartitionTask(partition_task),
                                memory_state.op_id,
                                node_idx,
                            )
                        })
                        .collect::<Vec<_>>()
                }
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
                            )
                        })
                        .collect::<Vec<_>>(),
                    _ => unreachable!("Only support 1 or 2 inputs"),
                },
            })
    }

    fn submittable_ordered(&self) -> impl Iterator<Item = SubmittableTask<T>> + '_ {
        // TODO(Clark): This will create a lot of redundant Tasks on each scheduling tick, which will have some
        // annoying side-effects such as incrementing the task counter. We should break this into two structs: a
        // submittable, candidate task struct (without a task ID) and a submitted task struct (with task ID).
        self.sorted_state
            .iter()
            .enumerate()
            .filter_map(|(node_idx, t)| match t.as_ref() {
                PartitionTaskState::LeafScan(scan_state) => {
                    let input = scan_state.inputs.borrow().back().cloned();
                    input.map(|input| {
                        let partition_task =
                            PartitionTask::new(vec![input.clone()], scan_state.task_op.clone());
                        SubmittableTask::new(
                            Task::<T>::ScanTask(partition_task),
                            scan_state.op_id,
                            node_idx,
                        )
                    })
                }
                PartitionTaskState::LeafMemory(memory_state) => {
                    let inputs = memory_state
                        .inputs
                        .iter()
                        .map(|input| input.borrow().back().cloned())
                        .collect::<Option<Vec<_>>>();
                    inputs.map(|inputs| {
                        // Our task tree -> state tree translation guarantees that any LeafMemory nodes with None task ops
                        // (i.e. no-op in-memory scans) will have their inputs moved into their outputs before execution,
                        // so we can safely unwrap the Option<PartitionTaksOp>.
                        let task_op = memory_state.task_op.as_ref().unwrap().clone();
                        let partition_task = PartitionTask::new(inputs, task_op);
                        SubmittableTask::new(
                            Task::<T>::PartitionTask(partition_task),
                            memory_state.op_id,
                            node_idx,
                        )
                    })
                }
                PartitionTaskState::Inner(inner_state) => {
                    let inputs = inner_state
                        .inputs
                        .iter()
                        .map(|input| input.borrow().back().cloned())
                        .collect::<Option<Vec<_>>>();
                    inputs.map(|inputs| {
                        let task_op = inner_state.task_op.clone();
                        let partition_task = PartitionTask::new(inputs, task_op);
                        SubmittableTask::new(
                            Task::<T>::PartitionTask(partition_task),
                            inner_state.op_id,
                            node_idx,
                        )
                    })
                }
            })
    }

    fn schedule_next_admissible(&self) -> Option<SubmittableTask<T>> {
        // TODO(Clark): Use submittable_unordered to mitigate head-of-line blocking.
        let submittable = self.submittable_ordered().collect::<Vec<_>>();
        log::debug!(
            "Trying to schedule next admissible: capacity = {:?}, utilization = {:?}, submittable = {:?}",
            self.executor.current_capacity(),
            self.executor.current_utilization(),
            submittable.iter().map(|t| (t.task.task_op_name(), t.task.resource_request())).collect::<Vec<_>>()
        );
        let admissible = submittable
            .into_iter()
            .filter(|t| self.executor.can_admit(t.task.resource_request()));
        // TODO(Clark): Use a better metric than output queue size.
        // TODO(Clark): Prioritize metadata-only ops.
        admissible.min_by(|x, y| {
            self.sorted_state[x.node_idx]
                .num_queued_outputs()
                .cmp(&self.sorted_state[y.node_idx].num_queued_outputs())
        })
    }

    fn schedule_next_submittable(&self) -> Option<SubmittableTask<T>> {
        // TODO(Clark): Use submittable_unordered to mitigate head-of-line blocking.
        let submittable = self.submittable_ordered();
        // TODO(Clark): Return the most admissible task (smallest resource capacity violation).
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
        node.pop_input_back();
        // node.pop_input_at_index(task.input_idx);
        let running_task = RunningTask::new(node, task_id);
        log::debug!(
            "Submitting task for op {} with ID = {}.",
            running_task.node.op_name(),
            task_id
        );
        self.inflight_tasks.insert(task_id, running_task);
        let t = task.task;
        let e = self.executor.clone();
        async move { e.submit_task(t).await }
    }

    pub async fn execute(mut self) -> DaftResult<Vec<Vec<T>>> {
        let mut running_task_futures = FuturesUnordered::new();
        let mut num_scheduling_loop_runs = 0;
        let mut num_tasks_run = 0;
        log::warn!(
            "Root num inputs = {}, num_outputs = {}",
            self.state_root.num_queued_inputs(),
            self.state_root.num_queued_outputs()
        );
        while !running_task_futures.is_empty() || self.has_inputs() {
            num_scheduling_loop_runs += 1;
            log::debug!(
                "Running scheduling loop - inflight futures: {}, num queued inputs: {}",
                running_task_futures.len(),
                self.num_queued_inputs(),
            );
            let next_task = self.schedule_next_admissible().or_else(|| {
                if running_task_futures.is_empty() {
                    log::info!("No admissible tasks available and no inflight tasks - scheduling an unadmissible task.");
                    Some(self.schedule_next_submittable().unwrap())
                } else {
                    None
                }
            });
            if let Some(task) = next_task {
                let fut = (&mut self).submit_task(task);
                running_task_futures.push(fut);
                num_tasks_run += 1;
                log::debug!(
                    "Submitted new task - inflight futures: {}, num queued inputs: {}",
                    running_task_futures.len(),
                    self.num_queued_inputs(),
                );
            }
            // TODO(Clark): Tweak this timeout.
            while let Ok(Some(result)) =
                tokio::time::timeout(Duration::from_millis(10), running_task_futures.next()).await
            {
                let (task_id, result) = result?;
                let running_task = self.inflight_tasks.remove(&task_id).unwrap();
                log::debug!(
                    "Task done for op {} with ID = {}.",
                    running_task.node.op_name(),
                    task_id
                );
                self.inflight_op_task_count[running_task.op_id()] -= 1;
                let op_name = running_task.node.op_name();
                running_task
                    .node
                    .outputs()
                    .iter()
                    .zip(result.into_iter())
                    .for_each(|(out, r)| {
                        out.borrow_mut().push_front(r);
                        log::warn!(
                            "Added item to output for op {} and task {}, output of size = {}",
                            op_name,
                            task_id,
                            out.borrow().len(),
                        );
                    });
            }
        }
        log::info!(
            "Scheduling loop ran {} times, running {} tasks",
            num_scheduling_loop_runs,
            num_tasks_run
        );
        log::warn!(
            "Root num outputs after = {}",
            self.state_root.num_queued_outputs()
        );
        Ok(self
            .state_root
            .outputs()
            .into_iter()
            .map(|v| v.borrow_mut().make_contiguous().into())
            .collect::<Vec<_>>())
    }
}

struct MultiZip<T>(Vec<T>);

impl<T> Iterator for MultiZip<T>
where
    T: Iterator,
{
    type Item = Vec<T::Item>;

    fn next(&mut self) -> Option<Self::Item> {
        self.0.iter_mut().map(Iterator::next).collect()
    }
}

#[derive(Debug)]
struct SubmittableTask<T: PartitionRef> {
    task: Task<T>,
    op_id: usize,
    node_idx: usize,
}

impl<T: PartitionRef> SubmittableTask<T> {
    fn new(task: Task<T>, op_id: usize, node_idx: usize) -> Self {
        Self {
            task,
            op_id,
            node_idx,
        }
    }
}

#[derive(Debug)]
struct RunningTask<T: PartitionRef> {
    pub node: Rc<PartitionTaskState<T>>,
    pub task_id: usize,
}

impl<T: PartitionRef> RunningTask<T> {
    fn new(node: Rc<PartitionTaskState<T>>, task_id: usize) -> Self {
        Self { node, task_id }
    }

    fn op_id(&self) -> usize {
        self.node.op_id()
    }
}
