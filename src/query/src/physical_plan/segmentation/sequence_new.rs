use std::sync::Arc;
use arrow::array::{Array, BooleanArray};
use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use chrono::Duration;
use datafusion::physical_expr::expressions::Column;
use datafusion::physical_expr::PhysicalExpr;
use crate::error::QueryError;
use crate::error::Result;
use crate::physical_plan::segmentation::Expr;

#[derive(Debug, Clone)]
pub enum Filter {
    DropOffOnAnyStep,
    DropOffOnStep(usize),
    TimeToConvert(Duration, Duration),
}

pub struct Sequence {
    schema: SchemaRef,
    ts_col: Column,
    window: Duration,
    steps: Vec<Arc<dyn PhysicalExpr>>,
    exclude: Option<Vec<(Arc<dyn PhysicalExpr>, Vec<usize>)>>,
    // expr and vec of step ids
    constants: Option<Vec<Column>>,
    // vec of col ids
    filter: Option<Filter>,
}

impl Expr for Sequence {
    fn evaluate(&mut self, spans: &[usize], batch: &RecordBatch, is_last: bool) -> Result<Option<Vec<i64>>> {
        let steps = self
            .steps
            .iter()
            .map(|step| {
                step
                    .evaluate(batch)
                    .map(|v| v.into_array(0).as_any().downcast_ref::<BooleanArray>().unwrap().clone())
                    .map_err(|e|e.into())
            })
            .collect::<Result<Vec<_>>>()?;

        let explude =
    }
}