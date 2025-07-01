use std::sync::Arc;

use common_error::DaftResult;
use common_metrics::ops::{NodeCategory, NodeType};
use common_runtime::OrderedJoinSet;
use daft_local_plan::{LocalNodeContext, LocalPhysicalPlan};
use daft_logical_plan::{partitioning::UnknownClusteringConfig, stats::StatsState};
use daft_schema::schema::SchemaRef;
use futures::StreamExt;

use super::{PipelineNodeImpl, TaskBuilderStream};
use crate::{
    pipeline_node::{
        DistributedPipelineNode, MaterializedOutput, NodeID, PipelineNodeConfig,
        PipelineNodeContext,
    },
    plan::{PlanConfig, PlanExecutionContext, TaskIDCounter},
    scheduling::{
        scheduler::SchedulerHandle,
        task::{SwordfishTask, SwordfishTaskBuilder},
    },
    utils::channel::{Sender, create_channel},
};

#[derive(Clone)]
pub(crate) struct IntoPartitionsNode {
    config: PipelineNodeConfig,
    context: PipelineNodeContext,
    num_partitions: usize,
    child: DistributedPipelineNode,
}

impl IntoPartitionsNode {
    const NODE_NAME: &'static str = "IntoPartitions";

    pub fn new(
        node_id: NodeID,
        plan_config: &PlanConfig,
        num_partitions: usize,
        schema: SchemaRef,
        child: DistributedPipelineNode,
    ) -> Self {
        let context = PipelineNodeContext::new(
            plan_config.query_idx,
            plan_config.query_id.clone(),
            node_id,
            Arc::from(Self::NODE_NAME),
            NodeType::IntoPartitions,
            NodeCategory::BlockingSink,
        );
        let config = PipelineNodeConfig::new(
            schema,
            plan_config.config.clone(),
            Arc::new(UnknownClusteringConfig::new(num_partitions).into()),
        );

        Self {
            config,
            context,
            num_partitions,
            child,
        }
    }

    pub fn into_node(self) -> DistributedPipelineNode {
        DistributedPipelineNode::new(Arc::new(self))
    }

    async fn coalesce_tasks(
        self: Arc<Self>,
        builders: Vec<SwordfishTaskBuilder>,
        scheduler_handle: &SchedulerHandle<SwordfishTask>,
        task_id_counter: &TaskIDCounter,
        result_tx: Sender<SwordfishTaskBuilder>,
    ) -> DaftResult<()> {
        assert!(
            builders.len() >= self.num_partitions,
            "Cannot coalesce from {} to {} partitions.",
            builders.len(),
            self.num_partitions
        );

        // Coalesce partitions evenly with remainder handling
        // Example: 10 inputs, 3 partitions = 4, 3, 3

        // Base inputs per partition: 10 / 3 = 3 (all tasks get at least 3 inputs)
        let base_inputs_per_partition = builders.len() / self.num_partitions;
        // Remainder: 10 % 3 = 1 (one task gets an extra input)
        let num_partitions_with_extra_input = builders.len() % self.num_partitions;

        // 记录每个输出分区需要合并的 Task 数量，比如上游有 10 个 ScanTask，目标分区时 3，则会将这个 10 个 Task
        // 分组为 [4, 3, 3]，然后逐一等待这几个分组任务的执行，比如前 4 个任务执行完了则将其物化结果构造一个
        // InMemoryScan + IntoPartitions(1) 以实现分区合并
        // 所以这里看做是流式执行的，因为无需等待所有的分组任务执行完成再执行下游 Task，而是每当完成一个分组任务就可以提交一个下游 Task
        let mut tasks_per_partition = Vec::new();

        let mut builder_iter = builders.into_iter();
        for partition_idx in 0..self.num_partitions {
            let mut chunk_size = base_inputs_per_partition;
            // This partition needs an extra input, i.e. partition_idx == 0 and remainder == 1
            if partition_idx < num_partitions_with_extra_input {
                chunk_size += 1;
            }

            // Build and submit all the tasks for this partition
            let submitted_tasks = builder_iter
                .by_ref()
                .take(chunk_size)
                .map(|builder| {
                    let submittable_task = builder.build(self.context.query_idx, task_id_counter);
                    submittable_task.submit(scheduler_handle)
                })
                .collect::<DaftResult<Vec<_>>>()?;
            tasks_per_partition.push(submitted_tasks);
        }

        let mut output_futures = OrderedJoinSet::new();
        for tasks in tasks_per_partition {
            output_futures.spawn(async move {
                let materialized_output = futures::future::try_join_all(tasks)
                    .await?
                    .into_iter()
                    .flatten()
                    .collect::<Vec<_>>();
                DaftResult::Ok(materialized_output)
            });
        }

        // 逐一物化并处理各个分区任务的输出
        while let Some(result) = output_futures.join_next().await {
            // Collect all the outputs from this task and coalesce them into a single task.
            let materialized_outputs = result??;
            let (in_memory_scan, psets) = MaterializedOutput::into_in_memory_scan_with_psets(
                materialized_outputs,
                self.config.schema.clone(),
                self.node_id(),
            );
            let plan = LocalPhysicalPlan::into_partitions(
                in_memory_scan,
                1,
                StatsState::NotMaterialized,
                LocalNodeContext::new(Some(self.node_id() as usize)),
            );
            let builder =
                SwordfishTaskBuilder::new(plan, self.as_ref()).with_psets(self.node_id(), psets);
            if result_tx.send(builder).await.is_err() {
                break;
            }
        }

        Ok(())
    }

    async fn split_tasks(
        self: Arc<Self>,
        builders: Vec<SwordfishTaskBuilder>,
        scheduler_handle: &SchedulerHandle<SwordfishTask>,
        task_id_counter: &TaskIDCounter,
        result_tx: Sender<SwordfishTaskBuilder>,
    ) -> DaftResult<()> {
        assert!(
            builders.len() <= self.num_partitions,
            "Cannot split from {} to {} partitions.",
            builders.len(),
            self.num_partitions
        );
        // Split partitions evenly with remainder handling
        // Example: 3 inputs, 10 partitions = 4, 3, 3

        // Base outputs per partition: 10 / 3 = 3 (all partitions will split into at least 3 outputs)
        let base_splits_per_partition = self.num_partitions / builders.len();
        // Remainder: 10 % 3 = 1 (one partition will split into 4 outputs)
        let num_partitions_with_extra_output = self.num_partitions % builders.len();

        let mut submitted_tasks = Vec::new();

        for (input_partition_idx, builder) in builders.into_iter().enumerate() {
            let mut num_outputs = base_splits_per_partition;
            // This partition will split into one more output, i.e. input_partition_idx == 0 and remainder == 1
            if input_partition_idx < num_partitions_with_extra_output {
                num_outputs += 1;
            }
            // 构造一个 IntoPartitions(x) 任务，将各个 Task 的输出拆分成多个分区输出
            let into_partitions_builder = builder.map_plan(self.as_ref(), |plan| {
                LocalPhysicalPlan::into_partitions(
                    plan,
                    num_outputs,
                    StatsState::NotMaterialized,
                    LocalNodeContext::new(Some(self.node_id() as usize)),
                )
            });
            // Build and submit
            let submittable_task =
                into_partitions_builder.build(self.context.query_idx, task_id_counter);
            let submitted_task = submittable_task.submit(scheduler_handle)?;
            submitted_tasks.push(submitted_task);
        }

        let mut output_futures = OrderedJoinSet::new();
        for task in submitted_tasks {
            output_futures.spawn(task);
        }

        // Collect all the outputs and emit a new task for each output.
        while let Some(result) = output_futures.join_next().await {
            let materialized_outputs = result??;
            if let Some(output) = materialized_outputs {
                for output in output.split_into_materialized_outputs() {
                    let materialized_outputs = vec![output];
                    let (in_memory_scan, psets) =
                        MaterializedOutput::into_in_memory_scan_with_psets(
                            materialized_outputs,
                            self.config.schema.clone(),
                            self.node_id(),
                        );
                    let builder = SwordfishTaskBuilder::new(in_memory_scan, self.as_ref())
                        .with_psets(self.node_id(), psets);
                    if result_tx.send(builder).await.is_err() {
                        break;
                    }
                }
            }
        }

        Ok(())
    }

    async fn execute_into_partitions(
        self: Arc<Self>,
        input_stream: TaskBuilderStream,
        task_id_counter: TaskIDCounter,
        result_tx: Sender<SwordfishTaskBuilder>,
        scheduler_handle: SchedulerHandle<SwordfishTask>,
    ) -> DaftResult<()> {
        // Collect all input builders without materializing to count them
        let input_builders: Vec<SwordfishTaskBuilder> = input_stream.collect().await;
        let num_input_tasks = input_builders.len();

        println!(
            ">> input task num: {}, num_partitions: {}",
            num_input_tasks, self.num_partitions
        );
        match num_input_tasks.cmp(&self.num_partitions) {
            // 输入分区数 == 输出分区数，取决于是否需要进行 Task 合并
            std::cmp::Ordering::Equal => {
                if self
                    .config
                    .execution_config
                    .enable_scan_task_split_and_merge
                {
                    let node_id = self.node_id();
                    for builder in input_builders {
                        let builder = builder.map_plan(self.as_ref(), |plan| {
                            LocalPhysicalPlan::into_partitions(
                                plan,
                                1,
                                StatsState::NotMaterialized,
                                LocalNodeContext::new(Some(node_id as usize)),
                            )
                        });
                        let _ = result_tx.send(builder).await;
                    }
                } else {
                    for builder in input_builders {
                        let _ = result_tx.send(builder).await;
                    }
                }
            }
            // 输入分区数 > 输出分区数：合并
            std::cmp::Ordering::Greater => {
                // Too many tasks - coalesce
                self.coalesce_tasks(
                    input_builders,
                    &scheduler_handle,
                    &task_id_counter,
                    result_tx,
                )
                .await?;
            }
            // 输入分区数 < 输出分区数：拆分
            std::cmp::Ordering::Less => {
                // Too few tasks - split
                self.split_tasks(
                    input_builders,
                    &scheduler_handle,
                    &task_id_counter,
                    result_tx,
                )
                .await?;
            }
        }
        Ok(())
    }
}

impl PipelineNodeImpl for IntoPartitionsNode {
    fn context(&self) -> &PipelineNodeContext {
        &self.context
    }

    fn config(&self) -> &PipelineNodeConfig {
        &self.config
    }

    fn children(&self) -> Vec<DistributedPipelineNode> {
        vec![self.child.clone()]
    }

    fn multiline_display(&self, _verbose: bool) -> Vec<String> {
        vec![
            "IntoPartitions".to_string(),
            format!("Num partitions = {}", self.num_partitions),
        ]
    }

    fn produce_tasks(
        self: Arc<Self>,
        plan_context: &mut PlanExecutionContext,
    ) -> TaskBuilderStream {
        let input_stream = self.child.clone().produce_tasks(plan_context);
        let (result_tx, result_rx) = create_channel(1);

        plan_context.spawn(self.execute_into_partitions(
            input_stream,
            plan_context.task_id_counter(),
            result_tx,
            plan_context.scheduler_handle(),
        ));

        TaskBuilderStream::from(result_rx)
    }
}
