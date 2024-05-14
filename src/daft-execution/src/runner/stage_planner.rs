#[allow(unused_variables)]
use std::{collections::HashMap, sync::Arc};

use daft_micropartition::MicroPartition;
use daft_plan::{
    physical_ops::{
        Aggregate, BroadcastJoin, Coalesce, Concat, EmptyScan, Explode, FanoutByHash, FanoutRandom,
        Filter, Flatten, HashJoin, InMemoryScan, Limit, MonotonicallyIncreasingId, Project,
        ReduceMerge, Sample, Sort, SortMergeJoin, Split, TabularScan, TabularWriteCsv,
        TabularWriteJson, TabularWriteParquet,
    },
    InMemoryInfo, OutputFileInfo, PhysicalPlan,
};

use crate::{
    compute::{
        ops::{
            filter::FilterOp,
            op_builder::FusedOpBuilder,
            ops::PartitionTaskOp,
            project::ProjectOp,
            scan::ScanOp,
            shuffle::{FanoutHashOp, FanoutRandomOp, ReduceMergeOp},
            sort::{BoundarySamplingOp, FanoutRangeOp, SamplesToQuantilesOp, SortedMergeOp},
        },
        partition::{
            partition_task_tree::{PartitionTaskNode, PartitionTaskNodeBuilder},
            virtual_partition::VirtualPartitionSet,
            PartitionRef,
        },
    },
    executor::executor::Executor,
    stage::{
        exchange::{
            collect::CollectExchange, exchange::Exchange, sort::SortExchange, ShuffleExchange,
        },
        stage::Stage,
    },
};

#[cfg(feature = "python")]
use daft_plan::physical_ops::IcebergWrite;

fn physical_plan_to_partition_task_tree<T: PartitionRef>(
    physical_plan: &PhysicalPlan,
    leaf_inputs: &mut Vec<VirtualPartitionSet<T>>,
    psets: &HashMap<String, Vec<T>>,
) -> PartitionTaskNodeBuilder {
    match physical_plan {
        PhysicalPlan::InMemoryScan(InMemoryScan {
            in_memory_info: InMemoryInfo { cache_key, .. },
            ..
        }) => {
            leaf_inputs.push(VirtualPartitionSet::PartitionRef(psets[cache_key].clone()));
            PartitionTaskNodeBuilder::LeafMemory(None)
        }
        PhysicalPlan::TabularScan(TabularScan { scan_tasks, .. }) => {
            leaf_inputs.push(VirtualPartitionSet::ScanTask(scan_tasks.clone()));
            let scan_op = ScanOp::new();
            let op_builder = FusedOpBuilder::new(Arc::new(scan_op));
            // PartitionTaskStateBuilder::LeafScan(scan_tasks.clone(), op_builder)
            PartitionTaskNodeBuilder::LeafScan(op_builder)
        }
        PhysicalPlan::EmptyScan(EmptyScan { schema, .. }) => todo!(),
        PhysicalPlan::Project(Project {
            input,
            projection,
            resource_request,
            ..
        }) => {
            let builder =
                physical_plan_to_partition_task_tree::<T>(input.as_ref(), leaf_inputs, psets);
            let project_op = Arc::new(ProjectOp::new(projection.clone(), resource_request.clone()));
            builder.fuse_or_link(project_op)
        }
        PhysicalPlan::Filter(Filter { input, predicate }) => {
            let builder =
                physical_plan_to_partition_task_tree::<T>(input.as_ref(), leaf_inputs, psets);
            let filter_op = Arc::new(FilterOp::new(vec![predicate.clone()]));
            builder.fuse_or_link(filter_op)
        }
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
        }) => {
            let builder =
                physical_plan_to_partition_task_tree::<T>(input.as_ref(), leaf_inputs, psets);
            let fanout_random_op = Arc::new(FanoutRandomOp::new(*num_partitions));
            builder.fuse_or_link(fanout_random_op)
        }
        PhysicalPlan::FanoutByHash(FanoutByHash {
            input,
            num_partitions,
            partition_by,
        }) => {
            let builder =
                physical_plan_to_partition_task_tree::<T>(input.as_ref(), leaf_inputs, psets);
            let fanout_hash_op = Arc::new(FanoutHashOp::new(*num_partitions, partition_by.clone()));
            builder.fuse_or_link(fanout_hash_op)
        }
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
        PhysicalPlan::Unpivot(_) => todo!(),
    }
}

pub fn physical_plan_to_stage<T: PartitionRef, E: Executor<T> + 'static>(
    physical_plan: &PhysicalPlan,
    is_final: bool,
    psets: &HashMap<String, Vec<T>>,
    executor: Arc<E>,
) -> Stage<T> {
    match physical_plan {
        PhysicalPlan::TabularScan(_) | PhysicalPlan::Project(_) | PhysicalPlan::Filter(_) => {
            // TODO(Clark): Abstract out the following common pattern into a visitor:
            //   1. DFS post-order traversal to create a task graph for child while also gathering inputs in same order.
            //   2. Create exchange/sink on current node configuration, with child task graph as an input.
            //   3. Return exchange/sink + gathered inputs.
            let mut leaf_inputs = vec![];
            let task_graph =
                physical_plan_to_partition_task_tree::<T>(physical_plan, &mut leaf_inputs, psets)
                    .build();
            if is_final {
                // TODO(Clark): Create Sink stage, once implemented.
                // let sink: Box<dyn Sink<T>> = Box::new(CollectSink::new(task_graph, executor));
                // (sink, leaf_inputs).into()
                let exchange: Box<dyn Exchange<T>> =
                    Box::new(CollectExchange::new(task_graph, executor));
                (exchange, leaf_inputs).into()
            } else {
                let exchange: Box<dyn Exchange<T>> =
                    Box::new(CollectExchange::new(task_graph, executor));
                (exchange, leaf_inputs).into()
            }
        }
        PhysicalPlan::InMemoryScan(InMemoryScan {
            in_memory_info: InMemoryInfo { cache_key, .. },
            ..
        }) => todo!(),
        PhysicalPlan::EmptyScan(EmptyScan { schema, .. }) => todo!(),
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
        }) => {
            let mut leaf_inputs = vec![];
            let upstream_task_tree_builder =
                physical_plan_to_partition_task_tree::<T>(input.as_ref(), &mut leaf_inputs, psets);
            let upstream_task_graph = upstream_task_tree_builder.build();

            let sampling_task_op = BoundarySamplingOp::new(*num_partitions, sort_by.clone());
            let task_op: Arc<dyn PartitionTaskOp<Input = MicroPartition>> =
                Arc::new(sampling_task_op);
            let sampling_task_graph = PartitionTaskNode::LeafMemory(Some(task_op).into());

            let reduce_to_quantiles_op = SamplesToQuantilesOp::new(
                *num_partitions,
                sort_by.clone(),
                descending.clone(),
                sampling_task_graph.num_outputs(),
            );
            let task_op: Arc<dyn PartitionTaskOp<Input = MicroPartition>> =
                Arc::new(reduce_to_quantiles_op);
            let reduce_to_quantiles_task_graph =
                PartitionTaskNode::LeafMemory(Some(task_op).into());

            let fanout_range_op =
                FanoutRangeOp::new(*num_partitions, sort_by.clone(), descending.clone());
            let task_op: Arc<dyn PartitionTaskOp<Input = MicroPartition>> =
                Arc::new(fanout_range_op);
            let map_task_graph = PartitionTaskNode::LeafMemory(Some(task_op).into());

            let sorted_merge_op =
                SortedMergeOp::new(*num_partitions, sort_by.clone(), descending.clone());
            let task_op: Arc<dyn PartitionTaskOp<Input = MicroPartition>> =
                Arc::new(sorted_merge_op);
            let reduce_task_graph = PartitionTaskNode::LeafMemory(Some(task_op).into());

            let sort_exchange: Box<dyn Exchange<T>> = Box::new(SortExchange::new(
                upstream_task_graph,
                sampling_task_graph,
                reduce_to_quantiles_task_graph,
                map_task_graph,
                reduce_task_graph,
                executor,
            ));
            (sort_exchange, leaf_inputs).into()
        }
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
        }) => {
            todo!()
        }
        PhysicalPlan::FanoutByRange(_) => unimplemented!(
            "FanoutByRange not implemented, since only use case (sorting) doesn't need it yet."
        ),
        PhysicalPlan::ReduceMerge(ReduceMerge { input }) => {
            let mut leaf_inputs = vec![];
            let map_task_tree_builder =
                physical_plan_to_partition_task_tree::<T>(input.as_ref(), &mut leaf_inputs, psets);
            let map_task_graph = map_task_tree_builder.build();

            let reduce_merge_op = ReduceMergeOp::new(map_task_graph.num_outputs());
            let task_op: Arc<dyn PartitionTaskOp<Input = MicroPartition>> =
                Arc::new(reduce_merge_op);
            let reduce_task_graph = PartitionTaskNode::LeafMemory(Some(task_op).into());

            let shuffle_exchange: Box<dyn Exchange<T>> = Box::new(ShuffleExchange::new(
                map_task_graph,
                reduce_task_graph,
                executor,
            ));
            (shuffle_exchange, leaf_inputs).into()
        }
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
        PhysicalPlan::Unpivot(_) => todo!(),
    }
}
