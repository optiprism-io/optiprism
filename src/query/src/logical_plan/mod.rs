use arrow::datatypes::DataType;

pub mod dictionary_decode;
pub mod expr;
pub mod merge;
pub mod pivot;
// pub mod _segmentation;
pub mod add_string_column;
pub mod aggregate_columns;
pub mod db_parquet;
pub mod funnel;
pub mod limit_groups;
pub mod partitioned_aggregate;
pub mod rename_column_rows;
pub mod rename_columns;
pub mod reorder_columns;
pub mod segment;
pub mod unpivot;

#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub struct SortField {
    pub data_type: DataType,
}
