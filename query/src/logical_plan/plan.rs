use crate::error::Result;
use crate::logical_plan::expr::Expr;
use crate::logical_plan::nodes::FastAggregateNode;
use crate::logical_plan::plan::LogicalPlan::FastAggregate;
use datafusion::datasource::TableProvider;
use datafusion::logical_plan::{DFSchemaRef, Expr as DFExpr, LogicalPlan as DFLogicalPlan};
use std::fmt::{Debug, Formatter};
use std::sync::Arc;

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
    FastAggregate {
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
    /// Get a reference to the logical plan's schema
    pub fn schema(&self) -> &DFSchemaRef {
        match self {
            LogicalPlan::TableScan {
                projected_schema, ..
            } => projected_schema,
            LogicalPlan::Filter { input, .. } => input.schema(),
            LogicalPlan::Aggregate { schema, .. } => schema,
            LogicalPlan::FastAggregate { schema, .. } => schema,
        }
    }

    /// returns all inputs of this `LogicalPlan` node. Does not
    /// include inputs to inputs.
    pub fn inputs(self: &LogicalPlan) -> Vec<&LogicalPlan> {
        match self {
            LogicalPlan::Filter { input, .. } => vec![input],
            LogicalPlan::Aggregate { input, .. } => vec![input],
            LogicalPlan::FastAggregate { input, .. } => vec![input],
            // plans without inputs
            LogicalPlan::TableScan { .. } => vec![],
        }
    }

    /// returns all expressions (non-recursively) in the current
    /// logical plan node. This does not include expressions in any
    /// children
    pub fn expressions(self: &LogicalPlan) -> Vec<Expr> {
        match self {
            LogicalPlan::Filter { predicate, .. } => vec![predicate.clone()],
            LogicalPlan::Aggregate {
                group_expr,
                aggr_expr,
                ..
            } => group_expr.iter().chain(aggr_expr.iter()).cloned().collect(),
            LogicalPlan::FastAggregate {
                group_expr,
                aggr_expr,
                ..
            } => group_expr.iter().chain(aggr_expr.iter()).cloned().collect(),
            // plans without expressions
            LogicalPlan::TableScan { .. } => vec![],
        }
    }

    pub fn to_df_plan(&self) -> Result<DFLogicalPlan> {
        match self {
            LogicalPlan::Aggregate {
                input,
                group_expr,
                aggr_expr,
                schema,
            } => Ok(DFLogicalPlan::Aggregate {
                input: Arc::new(input.to_df_plan()?),
                group_expr: group_expr
                    .iter()
                    .map(|e| e.to_owned().to_df_expr(input.schema()))
                    .collect::<Result<_>>()?,
                aggr_expr: aggr_expr
                    .iter()
                    .map(|e| e.to_owned().to_df_expr(input.schema()))
                    .collect::<Result<_>>()?,
                schema: schema.clone(),
            }),
            LogicalPlan::FastAggregate {
                input,
                group_expr,
                aggr_expr,
                schema,
            } => {
                let node = FastAggregateNode::try_new(
                    input.clone(),
                    group_expr.clone(),
                    aggr_expr.clone(),
                    input.schema().clone(),
                )?;
                Ok(DFLogicalPlan::Extension {
                    node: Arc::new(node),
                })
            }
            LogicalPlan::TableScan {
                table_name,
                source,
                projection,
                projected_schema,
                filters,
                limit,
            } => Ok(DFLogicalPlan::TableScan {
                table_name: table_name.clone(),
                source: source.clone(),
                projection: projection.clone(),
                projected_schema: projected_schema.clone(),
                filters: filters
                    .iter()
                    .map(|f| f.to_owned().to_df_expr(self.schema()))
                    .collect::<Result<_>>()?,
                limit: limit.clone(),
            }),
            LogicalPlan::Filter { predicate, input } => Ok(DFLogicalPlan::Filter {
                predicate: predicate.clone().to_df_expr(self.schema())?,
                input: Arc::new(input.to_df_plan()?),
            }),
        }
    }
    pub fn build(&self) -> Result<LogicalPlan> {
        Ok(self.clone())
    }
}

impl Debug for LogicalPlan {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        self.to_df_plan().fmt(f)
    }
}
