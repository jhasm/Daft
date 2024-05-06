use common_error::DaftResult;
use daft_plan::ResourceRequest;

use crate::compute::partition::{partition_task::Task, PartitionRef};

pub trait Executor<T: PartitionRef + Send> {
    fn can_admit(&self, resource_request: &ResourceRequest) -> bool;

    fn submit_task(
        &self,
        task: Task<T>,
    ) -> impl std::future::Future<Output = DaftResult<(usize, Vec<T>)>> + std::marker::Send;
}
