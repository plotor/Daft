use common_error::{ensure, DaftResult};
use daft_dsl::ExprRef;
use itertools::Itertools;
use serde::{Deserialize, Serialize};

use crate::PhysicalPlanRef;

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct TopN {
    // Upstream node.
    pub input: PhysicalPlanRef,
    pub sort_by: Vec<ExprRef>,
    pub descending: Vec<bool>,
    pub nulls_first: Vec<bool>,
    pub offset: Option<u64>,
    pub limit: Option<u64>,
    pub num_partitions: usize,
}

impl TopN {
    pub(crate) fn try_new(
        input: PhysicalPlanRef,
        sort_by: Vec<ExprRef>,
        descending: Vec<bool>,
        nulls_first: Vec<bool>,
        offset: Option<u64>,
        limit: Option<u64>,
        num_partitions: usize,
    ) -> DaftResult<Self> {
        ensure!(
            offset.is_some() || limit.is_some(),
            "TopN node must have offset or limit"
        );
        Ok(Self {
            input,
            sort_by,
            descending,
            nulls_first,
            offset,
            limit,
            num_partitions,
        })
    }

    pub fn multiline_display(&self) -> Vec<String> {
        let mut res = vec![];
        // Must have at least one expression to sort by.
        assert!(!self.sort_by.is_empty());
        let pairs = self
            .sort_by
            .iter()
            .zip(self.descending.iter())
            .zip(self.nulls_first.iter())
            .map(|((sb, d), nf)| {
                format!(
                    "({}, {}, {})",
                    sb,
                    if *d { "descending" } else { "ascending" },
                    if *nf { "nulls first" } else { "nulls last" }
                )
            })
            .join(", ");

        res.push(match (self.offset, self.limit) {
            (Some(o), Some(l)) => {
                format!(
                    "TopN: Sort by = {}, Num Rows = {}, Offset = {}",
                    pairs,
                    l.saturating_sub(o),
                    o
                )
            }
            (Some(o), None) => format!("TopN: Sort by = {}, Num Rows = N/A, Offset = {}", pairs, o),
            (None, Some(l)) => format!("TopN: Sort by = {}, Num Rows = {}", pairs, l),
            (None, None) => unreachable!(),
        });

        res.push(format!("Num partitions = {}", self.num_partitions));
        res
    }
}

crate::impl_default_tree_display!(TopN);
