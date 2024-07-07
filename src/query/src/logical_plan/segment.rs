use std::any::Any;
use std::fmt::Debug;
use std::fmt::Formatter;
use std::hash::Hasher;
use std::sync::Arc;

use common::query;
use common::query::{PropValueOperation, QueryAggregate};
use common::query::SegmentTime;
use datafusion_common::Column;
use datafusion_common::DFSchema;
use datafusion_common::DFSchemaRef;
use datafusion_common::ScalarValue;
use datafusion_expr::Expr;
use datafusion_expr::LogicalPlan;
use datafusion_expr::UserDefinedLogicalNode;

use crate::error::QueryError;
use crate::Result;

#[derive(Clone, PartialEq, Eq, Hash)]
pub enum AggregateFunction {
    Sum,
    Min,
    Max,
    Avg,
    Count,
}

impl From<&QueryAggregate> for AggregateFunction {
    fn from(_value: &QueryAggregate) -> Self {
        todo!()
    }
}
#[derive(Clone, PartialEq, Eq, Hash)]
pub enum Operator {
    Eq,
    NotEq,
    Lt,
    LtEq,
    Gt,
    GtEq,
}

impl From<&query::PropValueOperation> for Operator {
    fn from(_value: &PropValueOperation) -> Self {
        todo!()
    }
}
#[derive(Clone, PartialEq, Eq, Hash)]
pub enum TimeRange {
    Between(i64, i64),
    From(i64),
    Last(i64, i64),
    None,
}

impl From<&SegmentTime> for TimeRange {
    fn from(_value: &SegmentTime) -> Self {
        todo!()
    }
}
#[derive(Clone, PartialEq, Eq, Hash)]
pub enum SegmentExpr {
    And(Box<SegmentExpr>, Box<SegmentExpr>),
    Or(Box<SegmentExpr>, Box<SegmentExpr>),
    Count {
        filter: Expr,
        ts_col: Column,
        partition_col:Column,
        time_range: TimeRange,
        op: Operator,
        right: i64,
        time_window: Option<i64>,
    },
    Aggregate {
        filter: Expr,
        predicate: Column,
        ts_col: Column,
        partition_col:Column,
        time_range: TimeRange,
        agg: AggregateFunction,
        op: Operator,
        right: ScalarValue,
        time_window: Option<i64>,
    },
}

#[derive(Clone, PartialEq, Eq, Hash)]
pub struct SegmentNode {
    pub input: LogicalPlan,
    pub expr: SegmentExpr,
    pub partition_col: Column,
    pub schema: DFSchemaRef,
}

impl SegmentNode {
    pub fn try_new(input: LogicalPlan, expr: SegmentExpr, partition_col: Column) -> Result<Self> {
        let field = input.schema().field_from_column(&partition_col)?.to_owned();
        let schema = DFSchema::from_unqualifed_fields(vec![field].into(), Default::default())?;
        Ok(Self {
            input,
            expr,
            partition_col,
            schema: Arc::new(schema),
        })
    }
}

impl Debug for SegmentNode {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        self.fmt_for_explain(f)
    }
}

impl UserDefinedLogicalNode for SegmentNode {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "Segment"
    }

    fn inputs(&self) -> Vec<&LogicalPlan> {
        vec![&self.input]
    }

    fn schema(&self) -> &DFSchemaRef {
        &self.schema
    }

    fn expressions(&self) -> Vec<Expr> {
        vec![]
    }

    fn fmt_for_explain(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "Segment")
    }

    fn from_template(&self, _: &[Expr], inputs: &[LogicalPlan]) -> Arc<dyn UserDefinedLogicalNode> {
        let node = SegmentNode::try_new(
            inputs[0].clone(),
            self.expr.clone(),
            self.partition_col.clone(),
        )
        .map_err(QueryError::into_datafusion_plan_error)
        .unwrap();
        Arc::new(node)
    }

    fn with_exprs_and_inputs(
        &self,
        exprs: Vec<Expr>,
        inputs: Vec<LogicalPlan>,
    ) -> datafusion_common::Result<Arc<dyn UserDefinedLogicalNode>> {
        let node = SegmentNode::try_new(
            inputs[0].clone(),
            self.expr.clone(),
            self.partition_col.clone(),
        )
        .map_err(QueryError::into_datafusion_plan_error)?;
        Ok(Arc::new(node))
    }

    fn dyn_hash(&self, state: &mut dyn Hasher) {
        use std::hash::Hash;
        let mut s = state;
        self.hash(&mut s);
    }

    fn dyn_eq(&self, other: &dyn UserDefinedLogicalNode) -> bool {
        match other.as_any().downcast_ref::<Self>() {
            Some(o) => self == o,

            None => false,
        }
    }
}
