use std::sync::Arc;

use arrow2::datatypes::{Field as ArrowField, Schema as ArrowSchema};
use common_error::{DaftError, DaftResult};
use common_file_formats::FileFormatConfig;
use common_runtime::get_io_runtime;
use common_scan_info::{
    PartitionField, Pushdowns, ScanOperator, ScanTaskLikeRef, SupportsPushdownFilters,
};
use daft_core::{count_mode::CountMode, prelude::SchemaRef};
use daft_dsl::ExprRef;
use lance::Dataset;

#[derive(Debug)]
pub struct LanceScanOperator {
    uri: String,
    file_format_config: Arc<FileFormatConfig>,

    ds: Dataset,
    schema: SchemaRef,
}

impl LanceScanOperator {
    pub fn try_new(uri: String, file_format_config: Arc<FileFormatConfig>) -> DaftResult<Self> {
        let cloned_uri = uri.clone();
        let rt = get_io_runtime(true);
        let ds = rt
            .block_on_current_thread(async move {
                Ok::<Dataset, lance::Error>(Dataset::open(&cloned_uri).await?)
            })
            .map_err(|e| DaftError::External(Box::new(e)))?;

        let lance_schema = ds.schema();
        let arrow_schema = ArrowSchema {
            fields: lance_schema.fields.iter().map(ArrowField::from).collect(),
            metadata: lance_schema.metadata.clone(),
        };

        Ok(Self {
            uri,
            file_format_config,
            ds,
            schema: SchemaRef::new(arrow_schema.into()),
        })
    }
}

impl ScanOperator for LanceScanOperator {
    fn name(&self) -> &str {
        "LanceScanOperator"
    }

    fn schema(&self) -> SchemaRef {
        self.schema().clone()
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
            format!("Schema = {}", self.schema()), // FIXME short_string? by zhenchao
        ]
    }

    fn supports_count_pushdown(&self) -> bool {
        true
    }

    fn supported_count_modes(&self) -> Vec<CountMode> {
        vec![CountMode::All]
    }

    fn to_scan_tasks(&self, _pushdowns: Pushdowns) -> DaftResult<Vec<ScanTaskLikeRef>> {
        todo!("to_scan_tasks, by zhenchao")
    }

    fn as_pushdown_filter(&self) -> Option<&dyn SupportsPushdownFilters> {
        Some(self)
    }
}

impl SupportsPushdownFilters for LanceScanOperator {
    fn push_filters(&self, _filter: &[ExprRef]) -> (Vec<ExprRef>, Vec<ExprRef>) {
        todo!("push_filters, by zhenchao")
    }
}
