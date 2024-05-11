use std::sync::Arc;

use daft_micropartition::MicroPartition;
use daft_scan::ScanTask;

use super::{partition_ref::PartitionMetadata, PartitionRef};

pub trait VirtualPartition: Clone {
    type TaskOpInput;

    fn metadata(&self) -> PartitionMetadata;
    fn partition(&self) -> Arc<Self::TaskOpInput>;
}

impl<T: PartitionRef> VirtualPartition for T {
    type TaskOpInput = MicroPartition;

    fn metadata(&self) -> PartitionMetadata {
        self.metadata()
    }

    fn partition(&self) -> Arc<Self::TaskOpInput> {
        self.partition()
    }
}

impl VirtualPartition for Arc<ScanTask> {
    type TaskOpInput = ScanTask;

    fn metadata(&self) -> PartitionMetadata {
        // TODO(Clark): Add API to ScanTask that always returns a non-None estimate.
        let num_rows = self.num_rows().unwrap();
        // TODO(Clark): Add API to ScanTask that always returns a non-None estimate.
        let size_bytes = self.size_bytes().unwrap();
        PartitionMetadata::new(num_rows, size_bytes)
    }

    fn partition(&self) -> Arc<Self::TaskOpInput> {
        self.clone()
    }
}

// pub enum VirtualPartition<T: PartitionRef> {
//     PartitionRef(Arc<T>),
//     ScanTask(Arc<ScanTask>),
// }

// impl<T: PartitionRef> VirtualPartition<T> {
//     pub fn metadata(&self) -> PartitionMetadata {
//         match self {
//             Self::PartitionRef(part_ref) => part_ref.metadata(),
//             Self::ScanTask(scan_task) => {
//                 // TODO(Clark): Add API to ScanTask that always returns a non-None estimate.
//                 let num_rows = scan_task.num_rows().unwrap();
//                 // TODO(Clark): Add API to ScanTask that always returns a non-None estimate.
//                 let size_bytes = scan_task.size_bytes().unwrap();
//                 PartitionMetadata::new(num_rows, size_bytes)
//             }
//         }
//     }
// }

#[derive(Debug, Clone)]
pub enum VirtualPartitionSet<T: PartitionRef> {
    PartitionRef(Vec<T>),
    ScanTask(Vec<Arc<ScanTask>>),
}

impl<T: PartitionRef> VirtualPartitionSet<T> {
    pub fn num_partitions(&self) -> usize {
        match self {
            Self::PartitionRef(parts) => parts.len(),
            Self::ScanTask(parts) => parts.len(),
        }
    }
}

// pub trait VirtualPartition {}

// impl VirtualPartition for ScanTask {}

// impl<T: PartitionRef> VirtualPartition for T {}
