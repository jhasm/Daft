use async_trait::async_trait;
use common_error::DaftResult;

use crate::compute::partition::{virtual_partition::VirtualPartitionSet, PartitionRef};

#[async_trait(?Send)]
pub trait Exchange<T: PartitionRef> {
    async fn run(self: Box<Self>, inputs: Vec<VirtualPartitionSet<T>>) -> DaftResult<Vec<Vec<T>>>;
}
