use std::sync::Arc;

use arrow::datatypes::Schema;
use datafusion::physical_expr::create_physical_expr;
use datafusion::physical_expr::execution_props::ExecutionProps;
use datafusion::physical_expr::PhysicalExpr;
use datafusion_common::DFSchema;
use datafusion_expr::Expr;

mod partitioned_aggregate;
pub mod planner;
mod segment;
use crate::error::Result;

fn build_filter(
    filter: Option<Expr>,
    dfschema: &DFSchema,
    schema: &Schema,
    execution_props: &ExecutionProps,
) -> Result<Option<Arc<dyn PhysicalExpr>>> {
    let ret = filter
        .clone()
        .map(|e| create_physical_expr(&e, &dfschema, schema, &execution_props))
        .transpose()?;

    Ok(ret)
}
