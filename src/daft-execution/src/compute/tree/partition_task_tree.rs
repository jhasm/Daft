use std::{
    cell::RefCell,
    collections::VecDeque,
    rc::Rc,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
};

use daft_micropartition::MicroPartition;
use daft_scan::ScanTask;

use crate::compute::{
    ops::{op_builder::FusedOpBuilder, ops::PartitionTaskOp},
    partition::{virtual_partition::VirtualPartitionSet, PartitionRef},
};

#[derive(Debug, Clone)]
pub struct PartitionTaskLeafScanNode {
    task_op: Arc<dyn PartitionTaskOp<Input = ScanTask>>,
}

impl PartitionTaskLeafScanNode {
    pub fn new(task_op: Arc<dyn PartitionTaskOp<Input = ScanTask>>) -> Self {
        Self { task_op }
    }
}

impl<T> From<T> for PartitionTaskLeafScanNode
where
    T: PartitionTaskOp<Input = ScanTask> + 'static,
{
    fn from(value: T) -> Self {
        Self::new(Arc::new(value))
    }
}

#[derive(Debug, Clone)]
pub struct PartitionTaskLeafMemoryNode {
    task_op: Option<Arc<dyn PartitionTaskOp<Input = MicroPartition>>>,
}

impl PartitionTaskLeafMemoryNode {
    pub fn new(task_op: Option<Arc<dyn PartitionTaskOp<Input = MicroPartition>>>) -> Self {
        Self { task_op }
    }
}

impl From<Option<Arc<dyn PartitionTaskOp<Input = MicroPartition>>>>
    for PartitionTaskLeafMemoryNode
{
    fn from(value: Option<Arc<dyn PartitionTaskOp<Input = MicroPartition>>>) -> Self {
        Self::new(value)
    }
}

#[derive(Debug, Clone)]
pub struct PartitionTaskInnerNode {
    inputs: Vec<PartitionTaskNode>,
    task_op: Arc<dyn PartitionTaskOp<Input = MicroPartition>>,
}

impl PartitionTaskInnerNode {
    pub fn new(
        task_op: Arc<dyn PartitionTaskOp<Input = MicroPartition>>,
        inputs: Vec<PartitionTaskNode>,
    ) -> Self {
        Self { inputs, task_op }
    }
}

impl<T> From<(T, Vec<PartitionTaskNode>)> for PartitionTaskInnerNode
where
    T: PartitionTaskOp<Input = MicroPartition> + 'static,
{
    fn from(value: (T, Vec<PartitionTaskNode>)) -> Self {
        let (task_op, inputs) = value;
        Self::new(Arc::new(task_op), inputs)
    }
}

#[derive(Debug, Clone)]
pub enum PartitionTaskNode {
    LeafScan(PartitionTaskLeafScanNode),
    LeafMemory(PartitionTaskLeafMemoryNode),
    Inner(PartitionTaskInnerNode),
}

impl PartitionTaskNode {
    pub fn num_outputs(&self) -> usize {
        match self {
            Self::LeafScan(PartitionTaskLeafScanNode { task_op }) => task_op.num_outputs(),
            Self::LeafMemory(PartitionTaskLeafMemoryNode { task_op }) => {
                task_op.as_ref().map(|op| op.num_outputs()).unwrap_or(1)
            }
            Self::Inner(PartitionTaskInnerNode { task_op, .. }) => task_op.num_outputs(),
        }
    }
}

#[derive(Debug, Clone)]
pub enum PartitionTaskNodeBuilder {
    LeafScan(FusedOpBuilder<ScanTask>),
    LeafMemory(Option<FusedOpBuilder<MicroPartition>>),
    Inner(Vec<PartitionTaskNode>, FusedOpBuilder<MicroPartition>),
}

impl PartitionTaskNodeBuilder {
    pub fn add_op(&mut self, op: Arc<dyn PartitionTaskOp<Input = MicroPartition>>) {
        match self {
            Self::LeafScan(builder) => builder.add_op(op),
            Self::LeafMemory(Some(builder)) | Self::Inner(_, builder) => builder.add_op(op),
            Self::LeafMemory(ref mut builder) => {
                let _ = builder.insert(FusedOpBuilder::new(op));
            }
        }
    }

    pub fn can_add_op(&self, op: &dyn PartitionTaskOp<Input = MicroPartition>) -> bool {
        match self {
            Self::LeafScan(builder) => builder.can_add_op(op),
            Self::LeafMemory(Some(builder)) | Self::Inner(_, builder) => builder.can_add_op(op),
            Self::LeafMemory(None) => true,
        }
    }

    pub fn build(self) -> PartitionTaskNode {
        match self {
            Self::LeafScan(builder) => {
                PartitionTaskNode::LeafScan(PartitionTaskLeafScanNode::new(builder.build()))
            }
            Self::LeafMemory(builder) => PartitionTaskNode::LeafMemory(
                PartitionTaskLeafMemoryNode::new(builder.map(|b| b.build())),
            ),
            Self::Inner(inputs, builder) => {
                PartitionTaskNode::Inner(PartitionTaskInnerNode::new(builder.build(), inputs))
            }
        }
    }

    pub fn fuse_or_link(mut self, op: Arc<dyn PartitionTaskOp<Input = MicroPartition>>) -> Self {
        if self.can_add_op(op.as_ref()) {
            self.add_op(op.clone());
            self
        } else {
            let op_builder = FusedOpBuilder::new(op);
            let child_node = self.build();
            PartitionTaskNodeBuilder::Inner(vec![child_node], op_builder)
        }
    }
}

static OP_ID_COUNTER: AtomicUsize = AtomicUsize::new(0);

#[derive(Debug)]
pub struct PartitionTaskLeafScanState<T: PartitionRef> {
    pub task_op: Arc<dyn PartitionTaskOp<Input = ScanTask>>,
    pub op_id: usize,
    pub inputs: Rc<RefCell<VecDeque<Arc<ScanTask>>>>,
    pub outputs: Vec<Rc<RefCell<VecDeque<T>>>>,
}

impl<T: PartitionRef> PartitionTaskLeafScanState<T> {
    pub fn new(
        task_op: Arc<dyn PartitionTaskOp<Input = ScanTask>>,
        inputs: Vec<Arc<ScanTask>>,
    ) -> Self {
        let inputs = Rc::new(RefCell::new(VecDeque::from(inputs)));
        let op_id = OP_ID_COUNTER.fetch_add(1, Ordering::SeqCst);
        let outputs = std::iter::repeat_with(|| Rc::new(RefCell::new(VecDeque::new())))
            .take(task_op.num_outputs())
            .collect::<Vec<_>>();
        Self {
            task_op,
            op_id,
            inputs,
            outputs,
        }
    }
}

impl<T: PartitionRef, P> From<(P, Vec<Arc<ScanTask>>)> for PartitionTaskLeafScanState<T>
where
    P: PartitionTaskOp<Input = ScanTask> + 'static,
{
    fn from(value: (P, Vec<Arc<ScanTask>>)) -> Self {
        let (task_op, inputs) = value;
        Self::new(Arc::new(task_op), inputs)
    }
}

#[derive(Debug)]
pub struct PartitionTaskLeafMemoryState<T: PartitionRef> {
    pub task_op: Option<Arc<dyn PartitionTaskOp<Input = MicroPartition>>>,
    pub op_id: usize,
    pub inputs: Vec<Rc<RefCell<VecDeque<T>>>>,
    pub outputs: Vec<Rc<RefCell<VecDeque<T>>>>,
}

impl<T: PartitionRef> PartitionTaskLeafMemoryState<T> {
    pub fn new(
        task_op: Option<Arc<dyn PartitionTaskOp<Input = MicroPartition>>>,
        inputs: Vec<Vec<T>>,
    ) -> Self {
        let inputs = inputs
            .into_iter()
            .map(|input| Rc::new(RefCell::new(VecDeque::from(input))))
            .collect::<Vec<_>>();
        let op_id = OP_ID_COUNTER.fetch_add(1, Ordering::SeqCst);
        let outputs = std::iter::repeat_with(|| Rc::new(RefCell::new(VecDeque::new())))
            .take(task_op.as_ref().map_or(1, |op| op.num_outputs()))
            .collect::<Vec<_>>();
        Self {
            task_op,
            op_id,
            inputs,
            outputs,
        }
    }
}

impl<T: PartitionRef>
    From<(
        Option<Arc<dyn PartitionTaskOp<Input = MicroPartition>>>,
        Vec<Vec<T>>,
    )> for PartitionTaskLeafMemoryState<T>
{
    fn from(
        value: (
            Option<Arc<dyn PartitionTaskOp<Input = MicroPartition>>>,
            Vec<Vec<T>>,
        ),
    ) -> Self {
        let (task_op, inputs) = value;
        Self::new(task_op, inputs)
    }
}

#[derive(Debug)]
pub struct PartitionTaskInnerState<T: PartitionRef> {
    pub task_op: Arc<dyn PartitionTaskOp<Input = MicroPartition>>,
    pub children: Vec<Rc<PartitionTaskState<T>>>,
    pub op_id: usize,
    pub inputs: Vec<Rc<RefCell<VecDeque<T>>>>,
    pub outputs: Vec<Rc<RefCell<VecDeque<T>>>>,
}

impl<T: PartitionRef> PartitionTaskInnerState<T> {
    pub fn new(
        task_op: Arc<dyn PartitionTaskOp<Input = MicroPartition>>,
        children: Vec<Rc<PartitionTaskState<T>>>,
    ) -> Self {
        let inputs = children
            .iter()
            .map(|child| {
                let child_outputs = child.outputs();
                assert!(child_outputs.len() == 1);
                child_outputs.into_iter().next().unwrap()
            })
            .collect::<Vec<_>>();
        let op_id = OP_ID_COUNTER.fetch_add(1, Ordering::SeqCst);
        let outputs = std::iter::repeat_with(|| Rc::new(RefCell::new(VecDeque::new())))
            .take(task_op.num_outputs())
            .collect::<Vec<_>>();
        Self {
            task_op,
            children,
            op_id,
            inputs,
            outputs,
        }
    }
}

impl<T: PartitionRef, P> From<(P, Vec<Rc<PartitionTaskState<T>>>)> for PartitionTaskInnerState<T>
where
    P: PartitionTaskOp<Input = MicroPartition> + 'static,
{
    fn from(value: (P, Vec<Rc<PartitionTaskState<T>>>)) -> Self {
        let (task_op, inputs) = value;
        Self::new(Arc::new(task_op), inputs)
    }
}

#[derive(Debug)]
pub enum PartitionTaskState<T: PartitionRef> {
    LeafScan(PartitionTaskLeafScanState<T>),
    LeafMemory(PartitionTaskLeafMemoryState<T>),
    Inner(PartitionTaskInnerState<T>),
}

impl<T: PartitionRef> PartitionTaskState<T> {
    pub fn pop_input_back(&self) {
        match self {
            Self::LeafScan(leaf) => {
                leaf.inputs.borrow_mut().pop_back();
            }
            Self::LeafMemory(leaf) => {
                leaf.inputs.iter().for_each(|inner_inputs| {
                    inner_inputs.borrow_mut().pop_back();
                });
            }
            Self::Inner(inner) => {
                inner.inputs.iter().for_each(|inner_inputs| {
                    inner_inputs.borrow_mut().pop_back();
                });
            }
        }
    }
    pub fn pop_input_at_index(&self, idx: usize) {
        // TODO(Clark): This is currently O(n), we should ideally make this O(1).
        match self {
            Self::LeafScan(leaf) => {
                leaf.inputs.borrow_mut().remove(idx);
            }
            Self::LeafMemory(leaf) => {
                leaf.inputs.iter().for_each(|inner_inputs| {
                    inner_inputs.borrow_mut().remove(idx);
                });
            }
            Self::Inner(inner) => {
                inner.inputs.iter().for_each(|inner_inputs| {
                    inner_inputs.borrow_mut().remove(idx);
                });
            }
        }
    }
    pub fn outputs(&self) -> Vec<Rc<RefCell<VecDeque<T>>>> {
        match self {
            Self::LeafScan(leaf) => leaf.outputs.clone(),
            Self::LeafMemory(leaf) => leaf.outputs.clone(),
            Self::Inner(inner) => inner.outputs.clone(),
        }
    }

    pub fn num_queued_inputs(&self) -> usize {
        match self {
            Self::LeafScan(leaf) => leaf.inputs.borrow().len(),
            Self::LeafMemory(leaf) => leaf.inputs[0].borrow().len(),
            Self::Inner(inner) => inner.inputs[0].borrow().len(),
        }
    }

    pub fn num_queued_outputs(&self) -> usize {
        match self {
            Self::LeafScan(leaf) => leaf
                .outputs
                .iter()
                .map(|output_lane| output_lane.borrow().len())
                .sum(),
            Self::LeafMemory(leaf) => leaf
                .outputs
                .iter()
                .map(|output_lane| output_lane.borrow().len())
                .sum(),
            Self::Inner(inner) => inner
                .outputs
                .iter()
                .map(|output_lane| output_lane.borrow().len())
                .sum(),
        }
    }

    pub fn op_id(&self) -> usize {
        match self {
            Self::LeafScan(leaf) => leaf.op_id,
            Self::LeafMemory(leaf) => leaf.op_id,
            Self::Inner(inner) => inner.op_id,
        }
    }

    pub fn op_name(&self) -> &str {
        match self {
            Self::LeafScan(leaf) => leaf.task_op.name(),
            Self::LeafMemory(leaf) => leaf
                .task_op
                .as_ref()
                .map(|op| op.name())
                .unwrap_or("InMemoryScan"),
            Self::Inner(inner) => inner.task_op.name(),
        }
    }
}

pub fn task_tree_to_state_tree<T: PartitionRef>(
    root: PartitionTaskNode,
    leaf_inputs: &mut Vec<VirtualPartitionSet<T>>,
) -> Rc<PartitionTaskState<T>> {
    match root {
        PartitionTaskNode::LeafScan(PartitionTaskLeafScanNode { task_op }) => {
            let partition_set = leaf_inputs.remove(0);
            if let VirtualPartitionSet::ScanTask(scan_tasks) = partition_set {
                PartitionTaskState::LeafScan(PartitionTaskLeafScanState::<T>::new(
                    task_op, scan_tasks,
                ))
                .into()
            } else {
                panic!(
                    "Leaf input for scan node must be scan tasks: {:?}",
                    partition_set
                )
            }
        }
        PartitionTaskNode::LeafMemory(PartitionTaskLeafMemoryNode { task_op }) => {
            let num_inputs = task_op.as_ref().map(|op| op.num_inputs()).unwrap_or(1);
            let partition_sets = leaf_inputs.drain(..num_inputs);
            let part_refs = partition_sets
                .map(|p| match p {
                    VirtualPartitionSet::PartitionRef(part_refs) => part_refs,
                    VirtualPartitionSet::ScanTask(_) => panic!(
                        "Leaf input for in-memory node must be in-memory references: {:?}",
                        p
                    ),
                })
                .collect::<Vec<_>>();
            let memory_state = PartitionTaskLeafMemoryState::<T>::new(task_op.clone(), part_refs);
            if task_op.is_none() {
                // If no task op for this in-memory scan, we can immediately push all inputs into the output queue.
                // TODO(Clark): We should probably lift this into the partition task scheduler, and have it be a generic procedure of
                // identifying no-op or metadata-only tasks and directly pushing inputs into outputs.
                assert!(memory_state.inputs.len() == 1);
                assert!(memory_state.outputs.len() == 1);
                memory_state.outputs[0]
                    .borrow_mut()
                    .extend(memory_state.inputs[0].borrow_mut().drain(..));
            }
            PartitionTaskState::LeafMemory(memory_state).into()
        }
        PartitionTaskNode::Inner(PartitionTaskInnerNode { inputs, task_op }) => {
            let children = inputs
                .into_iter()
                .map(|n| task_tree_to_state_tree(n, leaf_inputs))
                .collect::<Vec<_>>();
            PartitionTaskState::Inner(PartitionTaskInnerState::new(task_op, children).into()).into()
        }
    }
}

pub fn topological_sort<T: PartitionRef>(
    root: Rc<PartitionTaskState<T>>,
) -> Vec<Rc<PartitionTaskState<T>>> {
    let mut stack = VecDeque::new();
    in_order(root, &mut stack);
    let out = stack.make_contiguous();
    out.reverse();
    out.to_vec()
}

fn in_order<T: PartitionRef>(
    node: Rc<PartitionTaskState<T>>,
    stack: &mut VecDeque<Rc<PartitionTaskState<T>>>,
) {
    match node.as_ref() {
        PartitionTaskState::Inner(PartitionTaskInnerState { children, .. }) => {
            for child in children {
                in_order(child.clone(), stack);
            }
        }
        PartitionTaskState::LeafScan(_) | PartitionTaskState::LeafMemory(_) => {}
    }
    stack.push_back(node);
}
