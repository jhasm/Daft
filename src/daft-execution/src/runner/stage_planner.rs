use core::num;
use std::{collections::HashMap, sync::Arc};

use daft_micropartition::MicroPartition;
use daft_plan::{
    physical_ops::{
        Aggregate, BroadcastJoin, Coalesce, Concat, EmptyScan, Explode, FanoutByHash, FanoutRandom,
        Filter, Flatten, HashJoin, IcebergWrite, InMemoryScan, Limit, MonotonicallyIncreasingId,
        Project, ReduceMerge, Sample, Sort, SortMergeJoin, Split, TabularScan, TabularWriteCsv,
        TabularWriteJson, TabularWriteParquet,
    },
    InMemoryInfo, OutputFileInfo, PhysicalPlan, QueryStageOutput,
};

use crate::{
    compute::{
        ops::{
            filter::FilterOp,
            op_builder::FusedOpBuilder,
            ops::PartitionTaskOp,
            project::ProjectOp,
            scan::ScanOp,
            sort::{BoundarySamplingOp, FanoutRange, SamplesToQuantilesOp, SortedMerge},
        },
        partition::{
            partition_task_tree::{
                PartitionTaskLeafMemoryNode, PartitionTaskLeafScanNode, PartitionTaskNode,
                PartitionTaskNodeBuilder, PartitionTaskStateBuilder,
            },
            virtual_partition::VirtualPartitionSet,
            PartitionRef,
        },
    },
    executor::executor::Executor,
    stage::{
        exchange::{collect::CollectExchange, exchange::Exchange, sort::SortExchange},
        sink::{collect::CollectSink, sink::Sink},
        stage::{ExchangeStage, SinkStage, Stage},
    },
};

fn physical_plan_to_partition_task_tree<T: PartitionRef>(
    physical_plan: &PhysicalPlan,
    leaf_inputs: &mut Vec<VirtualPartitionSet<T>>,
    psets: &HashMap<String, Vec<T>>,
) -> PartitionTaskNodeBuilder {
    match physical_plan {
        #[cfg(feature = "python")]
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
        PhysicalPlan::Unpivot(_) => todo!(),
    }
}

pub fn physical_plan_to_stage<T: PartitionRef, E: Executor<T> + 'static>(
    query_stage: &QueryStageOutput,
    psets: &HashMap<String, Vec<T>>,
    executor: Arc<E>,
) -> Stage<T> {
    let (physical_plan, is_final) = match query_stage {
        QueryStageOutput::Partial { physical_plan, .. } => (physical_plan.as_ref(), false),
        QueryStageOutput::Final { physical_plan, .. } => (physical_plan.as_ref(), true),
    };
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
                let sink: Box<dyn Sink<T>> = Box::new(CollectSink::new(task_graph, executor));
                (sink, leaf_inputs).into()
            } else {
                let exchange: Box<dyn Exchange<T>> =
                    Box::new(CollectExchange::new(task_graph, executor));
                (exchange, leaf_inputs).into()
            }
        }
        #[cfg(feature = "python")]
        PhysicalPlan::InMemoryScan(InMemoryScan {
            in_memory_info: InMemoryInfo { cache_key, .. },
            ..
        }) => todo!(),
        // PhysicalPlan::TabularScan(TabularScan { scan_tasks, .. }) => {
        //     todo!()
        // }
        PhysicalPlan::EmptyScan(EmptyScan { schema, .. }) => todo!(),
        // PhysicalPlan::Project(Project {
        //     input,
        //     projection,
        //     resource_request,
        //     ..
        // }) => todo!(),
        // PhysicalPlan::Filter(Filter { input, predicate }) => todo!(),
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
            // Sort stage should be preceded by full materialization (in-memory scan).
            assert!(matches!(input.as_ref(), PhysicalPlan::InMemoryScan(_)));
            let mut leaf_inputs = vec![];
            let mut boundary_sampling_task_tree_builder =
                physical_plan_to_partition_task_tree::<T>(input.as_ref(), &mut leaf_inputs, psets);
            assert!(matches!(
                boundary_sampling_task_tree_builder,
                PartitionTaskNodeBuilder::LeafMemory(None)
            ));
            let mut fanout_range_task_tree_builder = boundary_sampling_task_tree_builder.clone();

            let sampling_task_op =
                Arc::new(BoundarySamplingOp::new(*num_partitions, sort_by.clone()));
            assert!(boundary_sampling_task_tree_builder.can_add_op(sampling_task_op.as_ref()));
            boundary_sampling_task_tree_builder.add_op(sampling_task_op);
            let sampling_task_graph = boundary_sampling_task_tree_builder.build();
            assert!(matches!(
                sampling_task_graph,
                PartitionTaskNode::LeafMemory(_)
            ));

            let reduce_to_quantiles_op =
                SamplesToQuantilesOp::new(*num_partitions, sort_by.clone(), descending.clone());
            let task_op: Arc<dyn PartitionTaskOp<Input = MicroPartition>> =
                Arc::new(reduce_to_quantiles_op);
            let reduce_to_quantiles_task_graph =
                PartitionTaskNode::LeafMemory(Some(task_op).into());

            let fanout_range_op = Arc::new(FanoutRange::new(
                *num_partitions,
                sort_by.clone(),
                descending.clone(),
            ));
            assert!(fanout_range_task_tree_builder.can_add_op(fanout_range_op.as_ref()));
            fanout_range_task_tree_builder.add_op(fanout_range_op);
            let map_task_graph = fanout_range_task_tree_builder.build();
            assert!(matches!(map_task_graph, PartitionTaskNode::LeafMemory(_)));

            let sorted_merge_op = SortedMerge::new(sort_by.clone(), descending.clone());
            let task_op: Arc<dyn PartitionTaskOp<Input = MicroPartition>> =
                Arc::new(sorted_merge_op);
            let reduce_task_graph = PartitionTaskNode::LeafMemory(Some(task_op).into());

            let sort_exchange: Box<dyn Exchange<T>> = Box::new(SortExchange::new(
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
        PhysicalPlan::Unpivot(_) => todo!(),
    }
}

// pub struct PartitionTaskTreeBuilder {
//     root: PartitionTaskNode,
// }

// impl PartitionTaskTreeBuilder {
//     pub fn from_physical_plan(physical_plan: &PhysicalPlan) -> Self {
//         match physical_plan {
//             #[cfg(feature = "python")]
//             PhysicalPlan::InMemoryScan(InMemoryScan {
//                 in_memory_info: InMemoryInfo { cache_key, .. },
//                 ..
//             }) => todo!(),
//             PhysicalPlan::TabularScan(TabularScan { scan_tasks, .. }) => {
//                 todo!()
//             }
//             PhysicalPlan::EmptyScan(EmptyScan { schema, .. }) => todo!(),
//             PhysicalPlan::Project(Project {
//                 input,
//                 projection,
//                 resource_request,
//                 ..
//             }) => todo!(),
//             PhysicalPlan::Filter(Filter { input, predicate }) => todo!(),
//             PhysicalPlan::Limit(Limit {
//                 input,
//                 limit,
//                 eager,
//                 num_partitions,
//             }) => todo!(),
//             PhysicalPlan::Explode(Explode {
//                 input, to_explode, ..
//             }) => todo!(),
//             PhysicalPlan::Sample(Sample {
//                 input,
//                 fraction,
//                 with_replacement,
//                 seed,
//             }) => todo!(),
//             PhysicalPlan::MonotonicallyIncreasingId(MonotonicallyIncreasingId {
//                 input,
//                 column_name,
//             }) => todo!(),
//             PhysicalPlan::Sort(Sort {
//                 input,
//                 sort_by,
//                 descending,
//                 num_partitions,
//             }) => todo!(),
//             PhysicalPlan::Split(Split {
//                 input,
//                 input_num_partitions,
//                 output_num_partitions,
//             }) => todo!(),
//             PhysicalPlan::Flatten(Flatten { input }) => todo!(),
//             PhysicalPlan::FanoutRandom(FanoutRandom {
//                 input,
//                 num_partitions,
//             }) => todo!(),
//             PhysicalPlan::FanoutByHash(FanoutByHash {
//                 input,
//                 num_partitions,
//                 partition_by,
//             }) => todo!(),
//             PhysicalPlan::FanoutByRange(_) => unimplemented!(
//                 "FanoutByRange not implemented, since only use case (sorting) doesn't need it yet."
//             ),
//             PhysicalPlan::ReduceMerge(ReduceMerge { input }) => todo!(),
//             PhysicalPlan::Aggregate(Aggregate {
//                 aggregations,
//                 groupby,
//                 input,
//                 ..
//             }) => todo!(),
//             PhysicalPlan::Coalesce(Coalesce {
//                 input,
//                 num_from,
//                 num_to,
//             }) => todo!(),
//             PhysicalPlan::Concat(Concat { other, input }) => todo!(),
//             PhysicalPlan::HashJoin(HashJoin {
//                 left,
//                 right,
//                 left_on,
//                 right_on,
//                 join_type,
//                 ..
//             }) => todo!(),
//             PhysicalPlan::SortMergeJoin(SortMergeJoin {
//                 left,
//                 right,
//                 left_on,
//                 right_on,
//                 join_type,
//                 num_partitions,
//                 left_is_larger,
//                 needs_presort,
//             }) => todo!(),
//             PhysicalPlan::BroadcastJoin(BroadcastJoin {
//                 broadcaster: left,
//                 receiver: right,
//                 left_on,
//                 right_on,
//                 join_type,
//                 is_swapped,
//             }) => todo!(),
//             PhysicalPlan::TabularWriteParquet(TabularWriteParquet {
//                 schema,
//                 file_info:
//                     OutputFileInfo {
//                         root_dir,
//                         file_format,
//                         partition_cols,
//                         compression,
//                         io_config,
//                     },
//                 input,
//             }) => todo!(),
//             PhysicalPlan::TabularWriteCsv(TabularWriteCsv {
//                 schema,
//                 file_info:
//                     OutputFileInfo {
//                         root_dir,
//                         file_format,
//                         partition_cols,
//                         compression,
//                         io_config,
//                     },
//                 input,
//             }) => todo!(),
//             PhysicalPlan::TabularWriteJson(TabularWriteJson {
//                 schema,
//                 file_info:
//                     OutputFileInfo {
//                         root_dir,
//                         file_format,
//                         partition_cols,
//                         compression,
//                         io_config,
//                     },
//                 input,
//             }) => todo!(),
//             #[cfg(feature = "python")]
//             PhysicalPlan::IcebergWrite(IcebergWrite {
//                 schema: _,
//                 iceberg_info,
//                 input,
//             }) => todo!(),
//             PhysicalPlan::Pivot(_) => todo!(),
//             PhysicalPlan::Unpivot(_) => todo!(),
//         }
//     }
// }

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
            PhysicalPlan::Unpivot(_) => todo!(),
        }
    }
}
