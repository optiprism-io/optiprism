use std::sync::Arc;
use datafusion::datasource::TableProvider;
use datafusion::logical_plan::{DFSchemaRef, LogicalPlan as DFLogicalPlan, Expr as DFExpr};
use crate::error::Result;
use crate::logical_plan::expr::Expr;

#[derive(Clone)]
pub enum LogicalPlan {
    Aggregate {
        /// The incoming logical plan
        input: Arc<LogicalPlan>,
        /// Grouping expressions
        group_expr: Vec<Expr>,
        /// Aggregate expressions
        aggr_expr: Vec<Expr>,
        /// The schema description of the aggregate output
        schema: DFSchemaRef,
    },
    /// Produces rows from a table provider by reference or from the context
    TableScan {
        /// The name of the table
        table_name: String,
        /// The source of the table
        source: Arc<dyn TableProvider>,
        /// Optional column indices to use as a projection
        projection: Option<Vec<usize>>,
        /// The schema description of the output
        projected_schema: DFSchemaRef,
        /// Optional expressions to be used as filters by the table provider
        filters: Vec<Expr>,
        /// Optional limit to skip reading
        limit: Option<usize>,
    },
    /// Filters rows from its input that do not match an
    /// expression (essentially a WHERE clause with a predicate
    /// expression).
    ///
    /// Semantically, `<predicate>` is evaluated for each row of the input;
    /// If the value of `<predicate>` is true, the input row is passed to
    /// the output. If the value of `<predicate>` is false, the row is
    /// discarded.
    Filter {
        /// The predicate expression, which must have Boolean type.
        predicate: Expr,
        /// The incoming logical plan
        input: Arc<LogicalPlan>,
    },
}

impl LogicalPlan {
    pub fn build(&self) -> Result<LogicalPlan> {
        Ok(self.clone())
    }
}