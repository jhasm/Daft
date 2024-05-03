use async_trait::async_trait;
use common_error::DaftResult;

use crate::compute::partition::{virtual_partition::VirtualPartitionSet, PartitionRef};

#[async_trait]
pub trait Exchange<T: PartitionRef> {
    async fn run(
        self,
        inputs: Vec<VirtualPartitionSet<T>>,
    ) -> DaftResult<Vec<VirtualPartitionSet<T>>>;
}
