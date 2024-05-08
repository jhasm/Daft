use async_trait::async_trait;
use common_error::{DaftError, DaftResult};
use futures::{Stream, TryStream};

use crate::compute::partition::{virtual_partition::VirtualPartitionSet, PartitionRef};

#[async_trait(?Send)]
pub trait Sink<T: PartitionRef> {
    async fn run(
        self: Box<Self>,
        inputs: Vec<VirtualPartitionSet<T>>,
    ) -> DaftResult<Vec<Box<dyn Stream<Item = DaftResult<T>>>>>;
}
