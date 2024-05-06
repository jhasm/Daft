use daft_plan::{
    physical_ops::{
        Aggregate, BroadcastJoin, Coalesce, Concat, EmptyScan, Explode, FanoutByHash, FanoutRandom,
        Filter, Flatten, HashJoin, IcebergWrite, InMemoryScan, Limit, MonotonicallyIncreasingId,
        Project, ReduceMerge, Sample, Sort, SortMergeJoin, Split, TabularScan, TabularWriteCsv,
        TabularWriteJson, TabularWriteParquet,
    },
    InMemoryInfo, OutputFileInfo, PhysicalPlan,
};

use crate::{
    compute::partition::{
        partition_task_tree::{PartitionTaskLeafScanNode, PartitionTaskNode},
        PartitionRef,
    },
    stage::stage::{ExchangeStage, SinkStage, Stage},
};

pub struct PartitionTaskTreeBuilder {
    root: PartitionTaskNode,
}

impl PartitionTaskTreeBuilder {
    pub fn from_physical_plan(physical_plan: &PhysicalPlan) -> Self {
        match physical_plan {
            #[cfg(feature = "python")]
            PhysicalPlan::InMemoryScan(InMemoryScan {
                in_memory_info: InMemoryInfo { cache_key, .. },
                ..
            }) => todo!(),
            PhysicalPlan::TabularScan(TabularScan { scan_tasks, .. }) => {
                todo!()
            }
            PhysicalPlan::EmptyScan(EmptyScan { schema, .. }) => todo!(),
            PhysicalPlan::Project(Project {
                input,
                projection,
                resource_request,
                ..
            }) => todo!(),
            PhysicalPlan::Filter(Filter { input, predicate }) => todo!(),
            PhysicalPlan::Limit(Limit {
                input,
                limit,
                eager,
                num_partitions,
            }) => todo!(),
            PhysicalPlan::Explode(Explode {
                input, to_explode, ..
            }) => todo!(),
            PhysicalPlan::Sample(Sample {
                input,
                fraction,
                with_replacement,
                seed,
            }) => todo!(),
            PhysicalPlan::MonotonicallyIncreasingId(MonotonicallyIncreasingId {
                input,
                column_name,
            }) => todo!(),
            PhysicalPlan::Sort(Sort {
                input,
                sort_by,
                descending,
                num_partitions,
            }) => todo!(),
            PhysicalPlan::Split(Split {
                input,
                input_num_partitions,
                output_num_partitions,
            }) => todo!(),
            PhysicalPlan::Flatten(Flatten { input }) => todo!(),
            PhysicalPlan::FanoutRandom(FanoutRandom {
                input,
                num_partitions,
            }) => todo!(),
            PhysicalPlan::FanoutByHash(FanoutByHash {
                input,
                num_partitions,
                partition_by,
            }) => todo!(),
            PhysicalPlan::FanoutByRange(_) => unimplemented!(
                "FanoutByRange not implemented, since only use case (sorting) doesn't need it yet."
            ),
            PhysicalPlan::ReduceMerge(ReduceMerge { input }) => todo!(),
            PhysicalPlan::Aggregate(Aggregate {
                aggregations,
                groupby,
                input,
                ..
            }) => todo!(),
            PhysicalPlan::Coalesce(Coalesce {
                input,
                num_from,
                num_to,
            }) => todo!(),
            PhysicalPlan::Concat(Concat { other, input }) => todo!(),
            PhysicalPlan::HashJoin(HashJoin {
                left,
                right,
                left_on,
                right_on,
                join_type,
                ..
            }) => todo!(),
            PhysicalPlan::SortMergeJoin(SortMergeJoin {
                left,
                right,
                left_on,
                right_on,
                join_type,
                num_partitions,
                left_is_larger,
                needs_presort,
            }) => todo!(),
            PhysicalPlan::BroadcastJoin(BroadcastJoin {
                broadcaster: left,
                receiver: right,
                left_on,
                right_on,
                join_type,
                is_swapped,
            }) => todo!(),
            PhysicalPlan::TabularWriteParquet(TabularWriteParquet {
                schema,
                file_info:
                    OutputFileInfo {
                        root_dir,
                        file_format,
                        partition_cols,
                        compression,
                        io_config,
                    },
                input,
            }) => todo!(),
            PhysicalPlan::TabularWriteCsv(TabularWriteCsv {
                schema,
                file_info:
                    OutputFileInfo {
                        root_dir,
                        file_format,
                        partition_cols,
                        compression,
                        io_config,
                    },
                input,
            }) => todo!(),
            PhysicalPlan::TabularWriteJson(TabularWriteJson {
                schema,
                file_info:
                    OutputFileInfo {
                        root_dir,
                        file_format,
                        partition_cols,
                        compression,
                        io_config,
                    },
                input,
            }) => todo!(),
            #[cfg(feature = "python")]
            PhysicalPlan::IcebergWrite(IcebergWrite {
                schema: _,
                iceberg_info,
                input,
            }) => todo!(),
            PhysicalPlan::Pivot(_) => todo!(),
        }
    }
}

pub struct StagePlanner {
    task_tree_buffer: Option<PartitionTaskNode>,
}

impl StagePlanner {
    pub fn create_stage<T: PartitionRef>(&mut self, physical_plan: &PhysicalPlan) -> Stage<T> {
        match physical_plan {
            #[cfg(feature = "python")]
            PhysicalPlan::InMemoryScan(InMemoryScan {
                in_memory_info: InMemoryInfo { cache_key, .. },
                ..
            }) => todo!(),
            PhysicalPlan::TabularScan(TabularScan { scan_tasks, .. }) => {
                todo!()
            }
            PhysicalPlan::EmptyScan(EmptyScan { schema, .. }) => todo!(),
            PhysicalPlan::Project(Project {
                input,
                projection,
                resource_request,
                ..
            }) => todo!(),
            PhysicalPlan::Filter(Filter { input, predicate }) => todo!(),
            PhysicalPlan::Limit(Limit {
                input,
                limit,
                eager,
                num_partitions,
            }) => todo!(),
            PhysicalPlan::Explode(Explode {
                input, to_explode, ..
            }) => todo!(),
            PhysicalPlan::Sample(Sample {
                input,
                fraction,
                with_replacement,
                seed,
            }) => todo!(),
            PhysicalPlan::MonotonicallyIncreasingId(MonotonicallyIncreasingId {
                input,
                column_name,
            }) => todo!(),
            PhysicalPlan::Sort(Sort {
                input,
                sort_by,
                descending,
                num_partitions,
            }) => todo!(),
            PhysicalPlan::Split(Split {
                input,
                input_num_partitions,
                output_num_partitions,
            }) => todo!(),
            PhysicalPlan::Flatten(Flatten { input }) => todo!(),
            PhysicalPlan::FanoutRandom(FanoutRandom {
                input,
                num_partitions,
            }) => todo!(),
            PhysicalPlan::FanoutByHash(FanoutByHash {
                input,
                num_partitions,
                partition_by,
            }) => todo!(),
            PhysicalPlan::FanoutByRange(_) => unimplemented!(
                "FanoutByRange not implemented, since only use case (sorting) doesn't need it yet."
            ),
            PhysicalPlan::ReduceMerge(ReduceMerge { input }) => todo!(),
            PhysicalPlan::Aggregate(Aggregate {
                aggregations,
                groupby,
                input,
                ..
            }) => todo!(),
            PhysicalPlan::Coalesce(Coalesce {
                input,
                num_from,
                num_to,
            }) => todo!(),
            PhysicalPlan::Concat(Concat { other, input }) => todo!(),
            PhysicalPlan::HashJoin(HashJoin {
                left,
                right,
                left_on,
                right_on,
                join_type,
                ..
            }) => todo!(),
            PhysicalPlan::SortMergeJoin(SortMergeJoin {
                left,
                right,
                left_on,
                right_on,
                join_type,
                num_partitions,
                left_is_larger,
                needs_presort,
            }) => todo!(),
            PhysicalPlan::BroadcastJoin(BroadcastJoin {
                broadcaster: left,
                receiver: right,
                left_on,
                right_on,
                join_type,
                is_swapped,
            }) => todo!(),
            PhysicalPlan::TabularWriteParquet(TabularWriteParquet {
                schema,
                file_info:
                    OutputFileInfo {
                        root_dir,
                        file_format,
                        partition_cols,
                        compression,
                        io_config,
                    },
                input,
            }) => todo!(),
            PhysicalPlan::TabularWriteCsv(TabularWriteCsv {
                schema,
                file_info:
                    OutputFileInfo {
                        root_dir,
                        file_format,
                        partition_cols,
                        compression,
                        io_config,
                    },
                input,
            }) => todo!(),
            PhysicalPlan::TabularWriteJson(TabularWriteJson {
                schema,
                file_info:
                    OutputFileInfo {
                        root_dir,
                        file_format,
                        partition_cols,
                        compression,
                        io_config,
                    },
                input,
            }) => todo!(),
            #[cfg(feature = "python")]
            PhysicalPlan::IcebergWrite(IcebergWrite {
                schema: _,
                iceberg_info,
                input,
            }) => todo!(),
            PhysicalPlan::Pivot(_) => todo!(),
        }
    }
}
