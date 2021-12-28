use std::any::Any;
use std::fmt;
use std::fmt::{Debug, Formatter};
use std::sync::Arc;
use datafusion::logical_plan::{DFSchemaRef, Expr as DFExpr, LogicalPlan as DFLogicalPlan, UserDefinedLogicalNode};
use crate::logical_plan::expr::{Expr};
use crate::logical_plan::plan::LogicalPlan;
use crate::error::Result;

pub struct FastAggregateNode {
    input: Arc<LogicalPlan>,
    df_input: Arc<DFLogicalPlan>,
    schema: DFSchemaRef,
    /// Grouping expressions
    group_expr: Vec<Expr>,
    /// Aggregate expressions
    aggr_expr: Vec<Expr>,
}

impl Debug for FastAggregateNode {
    /// For TopK, use explain format for the Debug format. Other types
    /// of nodes may
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        self.fmt_for_explain(f)
    }
}

impl UserDefinedLogicalNode for FastAggregateNode {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn inputs(&self) -> Vec<&DFLogicalPlan> {
        vec![&self.df_input]
    }

    fn schema(&self) -> &DFSchemaRef {
        &self.schema
    }

    fn expressions(&self) -> Vec<DFExpr> {
        todo!()
    }

    fn fmt_for_explain(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(
            f,
            "FastAggregate: groupBy=[{:?}], aggr=[{:?}]",
            self.group_expr, self.aggr_expr
        );
        Ok(())
    }

    fn from_template(&self, exprs: &[DFExpr], inputs: &[DFLogicalPlan]) -> Arc<dyn UserDefinedLogicalNode + Send + Sync> {
        panic!("unimplemented");
    }
}

impl FastAggregateNode {
    pub fn try_new(
        input: Arc<LogicalPlan>,
        group_expr: Vec<Expr>,
        aggr_expr: Vec<Expr>,
        schema: DFSchemaRef) -> Result<Self> {
        Ok(FastAggregateNode {
            input: input.clone(),
            group_expr,
            aggr_expr,
            schema,
            df_input: Arc::new(input.to_df_plan()?),
        })
    }
}