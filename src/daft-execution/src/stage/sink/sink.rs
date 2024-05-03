use futures::Stream;

use crate::compute::partition::{virtual_partition::VirtualPartitionSet, PartitionRef};

pub trait Sink<T: PartitionRef> {
    fn run(&self, inputs: Vec<VirtualPartitionSet<T>>) -> Vec<Box<dyn Stream<Item = T>>>;
}
