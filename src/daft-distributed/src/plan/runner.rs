use std::{collections::HashMap, sync::Arc};

use common_error::{DaftError, DaftResult};
use common_partitioning::PartitionRef;
use futures::StreamExt;

use super::{DistributedPhysicalPlan, PlanID, PlanResult};
use crate::{
    pipeline_node::MaterializedOutput,
    scheduling::{
        scheduler::{SchedulerHandle, spawn_default_scheduler_actor},
        task::SwordfishTask,
        worker::{Worker, WorkerManager},
    },
    stage::StagePlan,
    statistics::{StatisticsEvent, StatisticsManagerRef},
    utils::{
        channel::{Sender, create_channel},
        joinset::create_join_set,
        runtime::get_or_init_runtime,
    },
};

#[derive(Clone)]
pub(crate) struct PlanRunner<W: Worker<Task = SwordfishTask>> {
    worker_manager: Arc<dyn WorkerManager<Worker = W>>,
}

impl<W: Worker<Task = SwordfishTask>> PlanRunner<W> {
    pub fn new(worker_manager: Arc<dyn WorkerManager<Worker = W>>) -> Self {
        Self { worker_manager }
    }

    async fn execute_stages(
        &self,
        plan_id: PlanID,
        stage_plan: StagePlan,
        psets: HashMap<String, Vec<PartitionRef>>,
        // Task 发送器，用于向 Scheduler 发送待执行的 Task
        scheduler_handle: SchedulerHandle<SwordfishTask>,
        sender: Sender<MaterializedOutput>,
        statistics_manager: StatisticsManagerRef,
    ) -> DaftResult<()> {
        // 当前仅支持单 Stage 执行
        if stage_plan.num_stages() != 1 {
            return Err(DaftError::ValueError(format!(
                "Cannot run multiple stages on flotilla yet. Got {} stages",
                stage_plan.num_stages()
            )));
        }

        // 获取 Root State
        let stage = stage_plan.get_root_stage();
        // 基于 StagePlan 构造 Pipeline 节点树，并生成 SubmittableTask 任务流
        let running_stage = stage.run_stage(
            plan_id,
            psets,
            stage_plan.execution_config().clone(),
            scheduler_handle.clone(),
        )?;
        // 提交任务流中所有的 SubmittableTask 任务给 Scheduler，并等待返回结果流
        let mut materialized_result_stream = running_stage.materialize(scheduler_handle);
        // 消费结果流，并投递给结果通道
        while let Some(result) = materialized_result_stream.next().await {
            if sender.send(result?).await.is_err() {
                break;
            }
        }
        statistics_manager.handle_event(StatisticsEvent::PlanFinished { plan_id })?;
        Ok(())
    }

    pub fn run_plan(
        self: &Arc<Self>,
        plan: &DistributedPhysicalPlan,
        psets: HashMap<String, Vec<PartitionRef>>,
        statistics_manager: StatisticsManagerRef,
    ) -> DaftResult<PlanResult> {
        let plan_id = plan.id();
        let query_id = uuid::Uuid::new_v4().to_string();
        let stage_plan = plan.stage_plan().clone();

        let runtime = get_or_init_runtime();
        statistics_manager.handle_event(StatisticsEvent::PlanSubmitted {
            plan_id,
            query_id,
            logical_plan: plan.logical_plan().clone(),
        })?;

        // 创建结果通道
        let (result_sender, result_receiver) = create_channel(1);

        let this = self.clone();

        let joinset = runtime.block_on_current_thread(async move {
            let mut joinset = create_join_set();
            // 创建任务 Scheduler，默认采用 DefaultScheduler 实现，并返回一个 SwordfishTask 接收器，
            // 用于接收待执行的 SwordfishTask
            let scheduler_handle = spawn_default_scheduler_actor(
                self.worker_manager.clone(),
                &mut joinset,
                statistics_manager.clone(),
            );

            // 将 StagePlan 切分成多个 SwordfishTask，并通过 scheduler_handle 发送给 Scheduler 调度执行
            // 并等待任务执行结果，将结果投递给结果通道
            joinset.spawn(async move {
                this.execute_stages(
                    plan_id,
                    stage_plan,
                    psets,
                    scheduler_handle,
                    result_sender,
                    statistics_manager,
                )
                .await
            });
            joinset
        });
        Ok(PlanResult::new(joinset, result_receiver))
    }
}
