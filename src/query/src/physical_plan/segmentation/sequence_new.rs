use std::collections::HashMap;
use std::sync::Arc;
use arrow::array::{Array, ArrayRef, BooleanArray, TimestampSecondArray};
use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use chrono::Duration;
use datafusion::physical_expr::expressions::Column;
use datafusion::physical_expr::{PhysicalExpr, PhysicalExprRef};
use crate::error::QueryError;
use crate::error::Result;
use crate::physical_plan::segmentation::Expr;

#[derive(Debug, Clone)]
pub enum Filter {
    DropOffOnAnyStep,
    DropOffOnStep(usize),
    TimeToConvert(Duration, Duration),
}

struct State {
    step_id: usize,
    row_id: usize,
    step_row: Vec<usize>,
    window_start_ts: i64,
    is_completed: bool,
}

impl State {
    pub fn new(steps: usize) -> Self {
        Self {
            step_id: 0,
            row_id: 0,
            step_row: vec![0; steps],
            window_start_ts: 0,
            is_completed: false,
        }
    }
}

pub struct Sequence {
    schema: SchemaRef,
    ts_col: Column,
    window: Duration,
    steps: Vec<PhysicalExprRef>,
    exclude: Option<Vec<(PhysicalExprRef, Option<Vec<usize>>)>>,
    // expr and vec of step ids
    constants: Option<Vec<Column>>,
    // vec of col ids
    filter: Option<Filter>,
    state: State,
    result:
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
                    .map_err(|e| e.into())
            })
            .collect::<Result<Vec<_>>>()?;

        let mut exclude = vec![Vec::new(); steps.len()];

        if let Some(e) = self.exclude.as_ref() {
            for (expr, s) in e.iter() {
                let arr = expr
                    .evaluate(batch)?
                    .into_array(0)
                    .as_any()
                    .downcast_ref::<BooleanArray>()
                    .unwrap();
                match s {
                    None => {
                        for step_id in 0..steps.len() {
                            exclude[step_id].push(arr.clone());
                        }
                    }
                    Some(steps) => {
                        for step_id in steps {
                            exclude[*step_id].push(arr.clone());
                        }
                    }
                }
            }
        }

        let ts_col = self
            .ts_col
            .evaluate(batch)?
            .into_array(0)
            .as_any()
            .downcast_ref::<TimestampSecondArray>()
            .unwrap();

        Ok(())
    }
}