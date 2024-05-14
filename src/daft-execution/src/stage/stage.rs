use std::sync::atomic::{AtomicUsize, Ordering};

use crate::compute::partition::{virtual_partition::VirtualPartitionSet, PartitionRef};

use super::{exchange::exchange::Exchange, sink::sink::Sink};

static STAGE_ID_COUNTER: AtomicUsize = AtomicUsize::new(0);

pub struct ExchangeStage<T: PartitionRef> {
    pub op: Box<dyn Exchange<T>>,
    pub inputs: Vec<VirtualPartitionSet<T>>,
    pub stage_id: usize,
}

impl<T: PartitionRef> ExchangeStage<T> {
    pub fn new(op: Box<dyn Exchange<T>>, inputs: Vec<VirtualPartitionSet<T>>) -> Self {
        let stage_id = STAGE_ID_COUNTER.fetch_add(1, Ordering::SeqCst);
        Self {
            op,
            inputs,
            stage_id,
        }
    }
}

pub struct SinkStage<T: PartitionRef> {
    pub op: Box<dyn Sink<T>>,
    pub inputs: Vec<VirtualPartitionSet<T>>,
    pub stage_id: usize,
}

impl<T: PartitionRef> SinkStage<T> {
    pub fn new(op: Box<dyn Sink<T>>, inputs: Vec<VirtualPartitionSet<T>>) -> Self {
        let stage_id = STAGE_ID_COUNTER.fetch_add(1, Ordering::SeqCst);
        Self {
            op,
            inputs,
            stage_id,
        }
    }
}

pub enum Stage<T: PartitionRef> {
    Exchange(ExchangeStage<T>),
    Sink(SinkStage<T>),
}

impl<T: PartitionRef> From<(Box<dyn Exchange<T>>, Vec<VirtualPartitionSet<T>>)> for Stage<T> {
    fn from(value: (Box<dyn Exchange<T>>, Vec<VirtualPartitionSet<T>>)) -> Self {
        let (op, inputs) = value;
        Self::Exchange(ExchangeStage::new(op, inputs))
    }
}

impl<T: PartitionRef> From<(Box<dyn Sink<T>>, Vec<VirtualPartitionSet<T>>)> for Stage<T> {
    fn from(value: (Box<dyn Sink<T>>, Vec<VirtualPartitionSet<T>>)) -> Self {
        let (op, inputs) = value;
        Self::Sink(SinkStage::new(op, inputs))
    }
}
