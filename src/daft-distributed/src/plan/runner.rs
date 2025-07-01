use std::{
    collections::HashMap,
    sync::{
        Arc,
        atomic::{AtomicU32, Ordering},
    },
};

use common_daft_config::DaftExecutionConfig;
use common_error::DaftResult;
use common_metrics::QueryID;
use common_partitioning::PartitionRef;
use common_treenode::{TreeNode, TreeNodeRecursion};
use futures::{Stream, StreamExt};

use super::{DistributedPhysicalPlan, PlanResult, QueryIdx};
use crate::{
    pipeline_node::{
        DistributedPipelineNode, MaterializedOutput, SubmittableTaskStream,
        logical_plan_to_pipeline_node, materialize::materialize_all_pipeline_outputs,
        viz_distributed_pipeline_ascii,
    },
    scheduling::{
        scheduler::{SchedulerHandle, spawn_scheduler_actor},
        task::{SwordfishTask, TaskID},
        worker::{Worker, WorkerManager},
    },
    statistics::{StatisticsManager, StatisticsSubscriber},
    utils::{
        channel::{Sender, create_channel},
        joinset::{JoinSet, create_join_set},
        runtime::get_or_init_runtime,
    },
};

#[derive(Clone)]
pub(crate) struct TaskIDCounter {
    counter: Arc<AtomicU32>,
}

impl TaskIDCounter {
    pub fn new() -> Self {
        Self {
            counter: Arc::new(AtomicU32::new(0)),
        }
    }

    pub fn next(&self) -> TaskID {
        self.counter.fetch_add(1, Ordering::Relaxed)
    }
}

pub(crate) struct PlanExecutionContext {
    scheduler_handle: SchedulerHandle<SwordfishTask>,
    joinset: JoinSet<DaftResult<()>>,
    task_id_counter: TaskIDCounter,
}

impl PlanExecutionContext {
    fn new(scheduler_handle: SchedulerHandle<SwordfishTask>) -> Self {
        let joinset = JoinSet::new();
        Self {
            scheduler_handle,
            joinset,
            task_id_counter: TaskIDCounter::new(),
        }
    }

    pub fn scheduler_handle(&self) -> SchedulerHandle<SwordfishTask> {
        self.scheduler_handle.clone()
    }

    pub fn spawn(&mut self, task: impl Future<Output = DaftResult<()>> + Send + 'static) {
        self.joinset.spawn(task);
    }

    pub fn task_id_counter(&self) -> TaskIDCounter {
        self.task_id_counter.clone()
    }
}

#[derive(Clone)]
pub(crate) struct PlanConfig {
    pub query_idx: QueryIdx,
    pub query_id: QueryID,
    pub config: Arc<DaftExecutionConfig>,
}

impl PlanConfig {
    pub fn new(query_idx: QueryIdx, query_id: QueryID, config: Arc<DaftExecutionConfig>) -> Self {
        Self {
            query_idx,
            query_id,
            config,
        }
    }
}

pub(crate) struct RunningPlan {
    task_stream: SubmittableTaskStream,
    plan_context: PlanExecutionContext,
}

impl RunningPlan {
    fn new(task_stream: SubmittableTaskStream, plan_context: PlanExecutionContext) -> Self {
        Self {
            task_stream,
            plan_context,
        }
    }

    pub fn materialize(
        self,
        scheduler_handle: SchedulerHandle<SwordfishTask>,
    ) -> impl Stream<Item = DaftResult<MaterializedOutput>> + Send + Unpin + 'static {
        let joinset = self.plan_context.joinset;
        materialize_all_pipeline_outputs(self.task_stream, scheduler_handle, Some(joinset))
    }
}

#[derive(Clone)]
pub(crate) struct PlanRunner<W: Worker<Task = SwordfishTask>> {
    worker_manager: Arc<dyn WorkerManager<Worker = W>>,
}

impl<W: Worker<Task = SwordfishTask>> PlanRunner<W> {
    pub fn new(worker_manager: Arc<dyn WorkerManager<Worker = W>>) -> Self {
        Self { worker_manager }
    }

    async fn execute_plan(
        &self,
        pipeline_node: DistributedPipelineNode, // 执行计划对应的 PipelineNode 树
        scheduler_handle: SchedulerHandle<SwordfishTask>, // Task 发送器
        sender: Sender<MaterializedOutput>,     // 结果发送器
    ) -> DaftResult<()> {
        // Task 发送器会封装到上下文中，这样后续在 produce_tasks 各个阶段都可以往该队列中提交任务
        let mut plan_context = PlanExecutionContext::new(scheduler_handle.clone());
        // 基于 PipelineNode 树构造一系列 SwordfishTask，从 Root 节点开始 DFS 遍历
        let running_node = pipeline_node.produce_tasks(&mut plan_context);
        println!(">> Finish producing tasks, and start materializing results");
        // 封装成 RunningPlan
        let running_stage = RunningPlan::new(running_node, plan_context);
        // 提交所有的 SwordfishTask 任务给 Scheduler 调度执行，并等待任务执行完成返回结果
        // 这里并不是完整执行计划对应的 SwordfishTask，而是最后一个片段的
        let mut materialized_result_stream = running_stage.materialize(scheduler_handle);
        while let Some(result) = materialized_result_stream.next().await {
            let mo = result?;
            println!(
                ">> Send materialized result, Worker ID: {}, num rows: {}",
                &mo.worker_id(),
                &mo.num_rows()
            );
            if sender.send(mo).await.is_err() {
                break;
            }
        }
        Ok(())
    }

    pub fn run_plan(
        self: &Arc<Self>,
        plan: &DistributedPhysicalPlan,
        psets: HashMap<String, Vec<PartitionRef>>,
        subscribers: Vec<Box<dyn StatisticsSubscriber>>,
    ) -> DaftResult<PlanResult> {
        let plan_id = plan.idx();
        let query_id = plan.query_id();
        let config = plan.execution_config().clone();
        let logical_plan = plan.logical_plan().clone();
        let plan_config = PlanConfig::new(plan_id, query_id, config);

        // 将 Optimized LogicalPlan 转换为 Pipeline Tree
        let pipeline_node =
            logical_plan_to_pipeline_node(plan_config, logical_plan, Arc::new(psets))?;

        println!(
            ">> {} Optimized LogicalPlan -> Pipeline Tree:\n{}",
            plan_id,
            viz_distributed_pipeline_ascii(&pipeline_node, false)
        );

        // Extract runtime stats from pipeline nodes to create the StatisticsManager
        let mut runtime_stats = HashMap::new();
        pipeline_node.apply(|node| {
            runtime_stats.insert(node.node_id(), node.runtime_stats());
            Ok(TreeNodeRecursion::Continue)
        })?;

        let statistics_manager = StatisticsManager::new(runtime_stats, subscribers);

        let runtime = get_or_init_runtime();
        // 创建一个 Channel 用于接收执行完成的 MaterializedOutput
        let (result_sender, result_receiver) = create_channel(1);
        let this = self.clone();
        let joinset = runtime.block_on_current_thread(async move {
            let mut joinset = create_join_set();
            // 创建并启动 Scheduler，用于调度执行 SwordfishTask，
            // 这里返回的 scheduler_handle 是一个 Task 接收器，用于接收待调度的 SwordfishTask
            let scheduler_handle = spawn_scheduler_actor(
                self.worker_manager.clone(),
                &mut joinset,
                statistics_manager.clone(),
            );

            // 基于 PipelineNode 树构造一系列 SwordfishTask，并通过 scheduler_handle 提交给 Scheduler 调度执行，
            // 并等待任务执行完成返回结果
            joinset.spawn(async move {
                this.execute_plan(pipeline_node, scheduler_handle, result_sender)
                    .await
            });
            joinset
        });

        Ok(PlanResult::new(joinset, result_receiver))
    }
}
