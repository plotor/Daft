use std::sync::Arc;

use common_error::DaftResult;
use common_file_formats::FileFormatConfig;
use common_scan_info::{
    PartitionField, Pushdowns, ScanOperator, ScanTaskLikeRef, SupportsPushdownFilters,
};
use daft_core::count_mode::CountMode;
use daft_schema::prelude::SchemaRef;

use crate::storage_config::StorageConfig;

#[derive(Debug)]
pub struct FragmentScanOperator {
    uri: String,
    file_format_config: Arc<FileFormatConfig>,
    storage_config: Arc<StorageConfig>,
}

impl FragmentScanOperator {
    pub fn new(
        uri: String,
        file_format_config: Arc<FileFormatConfig>,
        storage_config: Arc<StorageConfig>,
    ) -> Self {
        Self {
            uri,
            file_format_config,
            storage_config,
        }
    }
}

impl ScanOperator for FragmentScanOperator {
    fn name(&self) -> &str {
        todo!()
    }

    fn schema(&self) -> SchemaRef {
        todo!()
    }

    fn partitioning_keys(&self) -> &[PartitionField] {
        todo!()
    }

    fn file_path_column(&self) -> Option<&str> {
        todo!()
    }

    fn generated_fields(&self) -> Option<SchemaRef> {
        todo!()
    }

    fn can_absorb_filter(&self) -> bool {
        todo!()
    }

    fn can_absorb_select(&self) -> bool {
        todo!()
    }

    fn can_absorb_limit(&self) -> bool {
        todo!()
    }

    fn can_absorb_shard(&self) -> bool {
        todo!()
    }

    fn multiline_display(&self) -> Vec<String> {
        todo!()
    }

    fn supports_count_pushdown(&self) -> bool {
        todo!()
    }

    fn supported_count_modes(&self) -> Vec<CountMode> {
        todo!()
    }

    fn to_scan_tasks(&self, _pushdowns: Pushdowns) -> DaftResult<Vec<ScanTaskLikeRef>> {
        todo!()
    }

    fn as_pushdown_filter(&self) -> Option<&dyn SupportsPushdownFilters> {
        todo!()
    }
}
