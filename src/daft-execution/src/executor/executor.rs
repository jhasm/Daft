use common_error::DaftResult;
use daft_plan::ResourceRequest;

use crate::compute::partition::{partition_task::Task, PartitionRef};

pub trait Executor<T: PartitionRef> {
    fn can_admit(&self, resource_request: &ResourceRequest) -> bool;

    async fn submit_task(&self, task: Task<T>) -> DaftResult<(usize, Vec<T>)>;
}
