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

        let exclude = self
            .exclude
            .map(|v| {
                let mut res = HashMap::<usize, Vec<ArrayRef>>::new();

                for (expr, steps) in v.into_iter() {
                    let expr_res = expr.evaluate(batch)?.into_array(0);
                    let matched_steps = match steps {
                        None => (0..self.steps.len()).collect::<Vec<_>>(),
                        Some(steps) => steps,
                    };
                    for step_id in matched_steps {
                        res
                            .entry(step_id)
                            .and_modify(|e| e.push(expr_res.clone()))
                            .or_insert(vec![expr_res.clone()]);
                    }
                }

                res
            }).map(|v| {
            (0..self.steps.len()).into_iter().map(|step_id| {
                v.get(&step_id).cloned()
            }).collect::<Vec<_>>()
        });

        let ts_col = self.ts_col.evaluate(batch)?.into_array(0).as_any().downcast_ref::<TimestampSecondArray>().unwrap();

        Ok(())
    }
}