use std::{
    cell::RefCell,
    collections::VecDeque,
    rc::Rc,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc, Mutex,
    },
};

use daft_micropartition::MicroPartition;
use daft_scan::ScanTask;

use crate::compute::ops::{op_builder::FusedOpBuilder, ops::PartitionTaskOp};

use super::{virtual_partition::VirtualPartitionSet, PartitionRef};

#[derive(Debug)]
pub struct PartitionTaskLeafScanNode {
    task_op: Arc<dyn PartitionTaskOp<Input = ScanTask>>,
}

impl PartitionTaskLeafScanNode {
    pub fn new(task_op: Arc<dyn PartitionTaskOp<Input = ScanTask>>) -> Self {
        Self { task_op }
    }
}

#[derive(Debug)]
pub struct PartitionTaskLeafMemoryNode {
    task_op: Arc<dyn PartitionTaskOp<Input = MicroPartition>>,
}

impl PartitionTaskLeafMemoryNode {
    pub fn new(task_op: Arc<dyn PartitionTaskOp<Input = MicroPartition>>) -> Self {
        Self { task_op }
    }
}

#[derive(Debug)]
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

#[derive(Debug)]
pub enum PartitionTaskNode {
    LeafScan(PartitionTaskLeafScanNode),
    LeafMemory(PartitionTaskLeafMemoryNode),
    Inner(PartitionTaskInnerNode),
}

pub struct PartitionTaskNodeBuilder<T: std::fmt::Debug> {
    task_op_builder: FusedOpBuilder<T>,
}

impl<T: std::fmt::Debug + 'static> PartitionTaskNodeBuilder<T> {
    pub fn can_fuse_op(&self, op: Arc<dyn PartitionTaskOp<Input = MicroPartition>>) -> bool {
        self.task_op_builder.can_add_op(op)
    }

    pub fn fuse_op(&mut self, op: Arc<dyn PartitionTaskOp<Input = MicroPartition>>) {
        self.task_op_builder.add_op(op)
    }

    pub fn build(self) -> PartitionTaskNode {
        let task_op = self.task_op_builder.build();
        todo!()
    }
}

// pub struct PartitionTaskTreeBuilder {
//     task_tree: Option<PartitionTaskNode>,
// }

// impl PartitionTaskTreeBuilder {
//     // pub fn from_scan(root: PartitionTaskSpec<ScanTask>) -> Self {
//     //     Self {
//     //         task_tree_buffer: PartitionTaskLeafNode::new(root),
//     //     }
//     // }

//     // pub fn from_partition_task(root: PartitionTaskSpec<ScanTask>) -> Self {
//     //     Self {
//     //         task_tree_buffer: PartitionTaskLeafNode::new(root),
//     //     }
//     // }

//     pub fn add_scan_task_to_stage(&mut self, partition_task_spec: PartitionTaskSpec<ScanTask>) {
//         let node = PartitionTaskLeafScanNode::new(partition_task_spec);
//         self.task_tree = match self.task_tree {
//             Some(PartitionTaskNode::Inner(mut inner)) => inner.inputs.pu
//         }
//         self.task_tree = if let Some(t) = self.task_tree {

//         }
//         todo!()
//     }

//     pub fn add_partition_task_to_stage(
//         &self,
//         partition_task_spec: PartitionTaskSpec<MicroPartition>,
//     ) {
//         todo!()
//     }
// }

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
        let outputs = vec![Rc::new(RefCell::new(VecDeque::new())); task_op.num_outputs()];
        Self {
            task_op,
            op_id,
            inputs,
            outputs,
        }
    }
}

#[derive(Debug)]
pub struct PartitionTaskLeafMemoryState<T: PartitionRef> {
    pub task_op: Arc<dyn PartitionTaskOp<Input = MicroPartition>>,
    pub op_id: usize,
    pub inputs: Rc<RefCell<VecDeque<T>>>,
    pub outputs: Vec<Rc<RefCell<VecDeque<T>>>>,
}

impl<T: PartitionRef> PartitionTaskLeafMemoryState<T> {
    pub fn new(task_op: Arc<dyn PartitionTaskOp<Input = MicroPartition>>, inputs: Vec<T>) -> Self {
        let inputs = Rc::new(RefCell::new(VecDeque::from(inputs)));
        let op_id = OP_ID_COUNTER.fetch_add(1, Ordering::SeqCst);
        let outputs = vec![Rc::new(RefCell::new(VecDeque::new())); task_op.num_outputs()];
        Self {
            task_op,
            op_id,
            inputs,
            outputs,
        }
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
                let mut child_outputs = child.outputs();
                assert!(child_outputs.len() == 1);
                child_outputs.remove(0)
            })
            .collect::<Vec<_>>();
        let op_id = OP_ID_COUNTER.fetch_add(1, Ordering::SeqCst);
        let outputs = vec![Rc::new(RefCell::new(VecDeque::new())); task_op.num_outputs()];
        Self {
            task_op,
            children,
            op_id,
            inputs,
            outputs,
        }
    }
}

#[derive(Debug)]
pub enum PartitionTaskState<T: PartitionRef> {
    LeafScan(PartitionTaskLeafScanState<T>),
    LeafMemory(PartitionTaskLeafMemoryState<T>),
    Inner(PartitionTaskInnerState<T>),
}

impl<T: PartitionRef> PartitionTaskState<T> {
    pub fn pop_input(&self, idx: usize) {
        // TODO(Clark): This is currently O(n), we should ideally make this O(1).
        match self {
            Self::LeafScan(leaf) => {
                leaf.inputs.borrow_mut().remove(idx);
            }
            Self::LeafMemory(leaf) => {
                leaf.inputs.borrow_mut().remove(idx);
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
            Self::LeafMemory(leaf) => leaf.inputs.borrow().len(),
            Self::Inner(inner) => inner
                .inputs
                .iter()
                .map(|input_lane| input_lane.borrow().len())
                .sum(),
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
            let partition_set = leaf_inputs.remove(0);
            if let VirtualPartitionSet::PartitionRef(part_refs) = partition_set {
                PartitionTaskState::LeafMemory(PartitionTaskLeafMemoryState::<T>::new(
                    task_op, part_refs,
                ))
                .into()
            } else {
                panic!(
                    "Leaf input for in-memory node must be in-memory references: {:?}",
                    partition_set
                )
            }
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
