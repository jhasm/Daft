use std::sync::Arc;

use async_trait::async_trait;
use common_error::DaftResult;

use crate::{
    compute::partition::{virtual_partition::VirtualPartitionSet, PartitionRef},
    executor::executor::Executor,
};

pub trait SinkSpec<T: PartitionRef, E: Executor<T> + 'static> {
    fn to_runnable_sink(self: Box<Self>, executor: Arc<E>) -> Box<dyn Sink<T>>;
}

#[async_trait(?Send)]
pub trait Sink<T: PartitionRef> {
    async fn run(
        self: Box<Self>,
        inputs: Vec<VirtualPartitionSet<T>>,
        output_channel: std::sync::mpsc::Sender<DaftResult<Vec<T>>>,
    );
}
