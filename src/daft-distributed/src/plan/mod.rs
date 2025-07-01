use std::sync::{
    atomic::{AtomicU16, Ordering},
    Arc,
};

use common_daft_config::DaftExecutionConfig;
use common_error::DaftResult;
use common_partitioning::PartitionRef;
use daft_logical_plan::{LogicalPlan, LogicalPlanBuilder};
use futures::{stream, Stream, StreamExt};
use serde::{Deserialize, Serialize};

use crate::{
    pipeline_node::MaterializedOutput,
    stage::StagePlan,
    utils::{
        channel::{Receiver, ReceiverStream},
        joinset::JoinSet,
        stream::JoinableForwardingStream,
    },
};

mod runner;
pub(crate) use runner::PlanRunner;

static PLAN_ID_COUNTER: AtomicU16 = AtomicU16::new(0);
pub(crate) type PlanID = u16;

#[derive(Serialize, Deserialize)]
pub(crate) struct DistributedPhysicalPlan {
    id: PlanID,
    stage_plan: StagePlan, // 基于 LogicalPlan 构造 StagePlan，当前也只是使用 MapPipeline 封装了一把 LogicalPlan
    logical_plan: Arc<LogicalPlan>, // 对于 LogicalPlan 的引用
}

impl DistributedPhysicalPlan {
    pub fn from_logical_plan_builder(
        builder: &LogicalPlanBuilder,
        config: Arc<DaftExecutionConfig>,
    ) -> DaftResult<Self> {
        // Optimized LogicalPlan
        let logical_plan = builder.build();
        let stage_plan = StagePlan::from_logical_plan(logical_plan.clone(), config)?;

        Ok(Self {
            id: PLAN_ID_COUNTER.fetch_add(1, Ordering::Relaxed),
            stage_plan,
            logical_plan,
        })
    }

    pub fn id(&self) -> PlanID {
        self.id
    }

    pub fn logical_plan(&self) -> &daft_logical_plan::LogicalPlanRef {
        &self.logical_plan
    }

    pub fn stage_plan(&self) -> &StagePlan {
        &self.stage_plan
    }
}

pub(crate) type PlanResultStream =
    JoinableForwardingStream<Box<dyn Stream<Item = PartitionRef> + Send + Unpin + 'static>>;

pub(crate) struct PlanResult {
    joinset: JoinSet<DaftResult<()>>,
    rx: Receiver<MaterializedOutput>,
}

impl PlanResult {
    fn new(joinset: JoinSet<DaftResult<()>>, rx: Receiver<MaterializedOutput>) -> Self {
        Self { joinset, rx }
    }

    pub fn into_stream(self) -> PlanResultStream {
        JoinableForwardingStream::new(
            Box::new(ReceiverStream::new(self.rx).flat_map(|mat| stream::iter(mat.into_inner().0))),
            self.joinset,
        )
    }
}
