use std::sync::Arc;

use arrow::record_batch::RecordBatch;
use datafusion::physical_expr::hash_utils::create_hashes;
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_expr::PhysicalExprRef;
use datafusion_common::Result as DFResult;

mod dictionary_decode;
pub mod expressions;
// mod funnel;
pub mod merge;
mod pivot;
// mod _segmentation;
// mod _segmentation;
// mod partitioned_aggregate;
// mod partitioned_aggregate;
// mod segmented_aggregate;
mod partition;
pub mod planner;
mod segment;
mod segmented_aggregate;
mod unpivot;
// pub mod merge;
// pub mod planner;
