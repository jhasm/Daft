use common_error::DaftResult;
use daft_plan::ResourceRequest;

use crate::{
    compute::partition::partition_task::Task,
    executor::{
        executor::Executor,
        resource_manager::{ExecutionResources, ResourceManager},
    },
};

use super::ray_partition_ref::RayPartitionRef;

#[derive(Debug)]
pub struct RayExecutor {
    resource_manager: ResourceManager,
}

impl RayExecutor {
    pub fn new(resource_capacity: ExecutionResources) -> Self {
        let resource_manager = ResourceManager::new(resource_capacity);
        Self { resource_manager }
    }
}

impl Executor<RayPartitionRef> for RayExecutor {
    fn can_admit(&self, resource_request: &ResourceRequest) -> bool {
        self.resource_manager.can_admit(resource_request)
    }

    async fn submit_task(
        &self,
        task: Task<RayPartitionRef>,
    ) -> DaftResult<(usize, Vec<RayPartitionRef>)> {
        self.resource_manager.admit(&task.resource_request());
        // TODO(Clark): Submit task to Ray cluster.
        todo!()
    }
}
