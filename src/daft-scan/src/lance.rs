use std::{collections::HashSet, sync::Arc};

use common_error::{DaftError, DaftResult};
use common_file_formats::FileFormatConfig;
use common_runtime::get_io_runtime;
use common_scan_info::{
    PartitionField, Pushdowns, ScanOperator, ScanTaskLike, ScanTaskLikeRef, SupportsPushdownFilters,
};
use daft_core::{count_mode::CountMode, prelude::SchemaRef};
use daft_dsl::{ExprRef, optimization::get_required_columns};
use lance::Dataset;

use crate::{DataSource, ScanTask, storage_config::StorageConfig};

#[derive(Debug)]
pub struct LanceScanOperator {
    uri: String,
    file_format_config: Arc<FileFormatConfig>,
    storage_config: Arc<StorageConfig>,

    ds: Arc<Dataset>,
}

impl LanceScanOperator {
    pub fn try_new(
        uri: String,
        file_format_config: Arc<FileFormatConfig>,
        storage_config: Arc<StorageConfig>,
    ) -> DaftResult<Self> {
        let cloned_uri = uri.clone();
        let rt = get_io_runtime(true);

        let lance_cfg = match file_format_config.as_ref() {
            FileFormatConfig::Lance(cfg) => cfg.clone(),
            _ => {
                return Err(DaftError::InternalError(format!(
                    "Expected Lance File Format Config, but got {}",
                    file_format_config.file_format()
                )));
            }
        };

        let ds = Arc::new(
            rt.block_within_async_context(async move {
                daft_lance::schema::try_open_dataset(
                    cloned_uri.as_str(),
                    lance_cfg.version,
                    lance_cfg.tag.clone(),
                    lance_cfg.block_size,
                    lance_cfg.index_cache_size,
                    lance_cfg.metadata_cache_size,
                    lance_cfg.storage_options,
                )
                .await
            })
            .map_err(|e| DaftError::External(Box::new(e)))??,
        );

        Ok(Self {
            uri,
            file_format_config,
            storage_config,
            ds,
        })
    }
}

impl ScanOperator for LanceScanOperator {
    fn name(&self) -> &'static str {
        "LanceScanOperator"
    }

    fn schema(&self) -> SchemaRef {
        SchemaRef::new(daft_lance::schema::to_daft_schema(self.ds.schema()))
    }

    fn partitioning_keys(&self) -> &[PartitionField] {
        &[]
    }

    fn file_path_column(&self) -> Option<&str> {
        None
    }

    fn generated_fields(&self) -> Option<SchemaRef> {
        None
    }

    fn can_absorb_filter(&self) -> bool {
        true
    }

    fn can_absorb_select(&self) -> bool {
        true
    }

    fn can_absorb_limit(&self) -> bool {
        true
    }

    fn can_absorb_shard(&self) -> bool {
        false
    }

    fn multiline_display(&self) -> Vec<String> {
        vec![
            format!("{}({})", self.name(), self.uri),
            format!("Schema = {}", self.schema()),
        ]
    }

    fn supports_count_pushdown(&self) -> bool {
        true
    }

    fn supported_count_modes(&self) -> Vec<CountMode> {
        vec![CountMode::All]
    }

    fn to_scan_tasks(&self, pushdowns: Pushdowns) -> DaftResult<Vec<ScanTaskLikeRef>> {
        let mut scan_tasks = vec![];

        // pushdown columns
        let required_columns = pushdowns.columns.as_ref().map(|columns| {
            let columns = &**columns;
            if let Some(filter_columns) = pushdowns.filters.as_ref().map(get_required_columns) {
                let mut merged_set: HashSet<String> = filter_columns.into_iter().collect();
                merged_set.extend(columns.iter().cloned());
                merged_set.into_iter().collect()
            } else {
                columns.clone()
            }
        });

        // pushdown filters
        // let filter_expr = match pushdowns.pushed_filters.as_ref() {
        //     None => None,
        //     Some(filters) => Some(filters.to_string()),
        // };

        let rt = get_io_runtime(true);
        let ds = self.ds.clone();
        let fragment_ids = rt
            .block_within_async_context(async move {
                // FIXME by zhenchao count_rows with filter
                daft_lance::schema::try_get_fragment_ids(ds.as_ref(), None).await
            })
            .map_err(|e| DaftError::External(Box::new(e)))??;

        for fid in fragment_ids {
            let scan_task = ScanTask::new(
                vec![DataSource::Fragment {
                    uri: self.uri.clone(),
                    fragment_ids: vec![fid],
                    columns: required_columns.clone(),
                    filter: None,
                    size_bytes: None,
                    metadata: None,
                    statistics: None,
                }],
                self.file_format_config.clone(),
                self.schema(),
                self.storage_config.clone(),
                pushdowns.clone(),
                self.generated_fields(),
            );

            scan_tasks.push(Arc::new(scan_task) as Arc<dyn ScanTaskLike>);
        }

        Ok(scan_tasks)
    }

    fn as_pushdown_filter(&self) -> Option<&dyn SupportsPushdownFilters> {
        Some(self)
    }
}

impl SupportsPushdownFilters for LanceScanOperator {
    fn push_filters(&self, filters: &[ExprRef]) -> (Vec<ExprRef>, Vec<ExprRef>) {
        let pushed_filters = vec![];
        let mut remain_filters = vec![];

        for expr in filters {
            // TODO add impl by zhenchao
            remain_filters.push(expr.clone());
        }

        (pushed_filters, remain_filters)
    }
}
