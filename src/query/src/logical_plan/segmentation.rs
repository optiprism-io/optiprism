use std::any::Any;
use std::collections::HashMap;
use std::fmt::Debug;
use std::fmt::Formatter;
use std::hash::Hasher;
use std::sync::Arc;

use arrow::datatypes::DataType;
use datafusion_common::Column;
use datafusion_common::DFField;
use datafusion_common::DFSchema;
use datafusion_common::DFSchemaRef;
use datafusion_common::Result as DFResult;
use datafusion_expr::Expr;
use datafusion_expr::LogicalPlan;
use datafusion_expr::UserDefinedLogicalNode;

use crate::error::QueryError;
use crate::Result;

#[derive(Hash, Debug, Clone, Eq, PartialEq)]
pub enum AggregateFunction {
    Sum(Column),
    Min(Column),
    Max(Column),
    Avg(Column),
    Count,
}

#[derive(Hash, Debug, Clone, Eq, PartialEq)]
pub enum TimeRange {
    Between(i64, i64),
    From(i64),
    Last(i64, i64),
}

#[derive(Hash, Eq, PartialEq)]
pub struct SegmentationNode {
    pub input: LogicalPlan,
    pub ts_col: Column,
    pub schema: DFSchemaRef,
    pub partition_cols: Vec<Column>,
    pub exprs: Vec<SegmentationExpr>,
}

#[derive(Hash, Debug, Clone, Eq, PartialEq)]
pub struct SegmentationExpr {
    pub agg_fn: AggregateFunction,
    pub time_range: Option<TimeRange>,
}

impl SegmentationNode {
    pub fn try_new(
        input: LogicalPlan,
        ts_col: Column,
        partition_cols: Vec<Column>,
        exprs: Vec<SegmentationExpr>,
    ) -> Result<Self> {
        let cols = partition_cols
            .iter()
            .map(|c| {
                input
                    .schema()
                    .field_from_column(c)
                    .and_then(|v| Ok(v.to_owned()))
            })
            .collect::<DFResult<Vec<_>>>()?;
        let schema = DFSchema::new_with_metadata(
            vec![cols, vec![DFField::new_unqualified(
                "segment",
                DataType::UInt8,
                false,
            )]]
            .concat(),
            HashMap::new(),
        )?;
        Ok(Self {
            input,
            ts_col,
            schema: Arc::new(schema),
            partition_cols,
            exprs,
        })
    }
}

impl Debug for SegmentationNode {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        self.fmt_for_explain(f)
    }
}

impl UserDefinedLogicalNode for SegmentationNode {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "Segmentation"
    }

    fn inputs(&self) -> Vec<&LogicalPlan> {
        vec![&self.input]
    }

    fn schema(&self) -> &DFSchemaRef {
        &self.input.schema()
    }

    fn expressions(&self) -> Vec<Expr> {
        vec![]
    }

    fn fmt_for_explain(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "Segmentation")
    }

    fn from_template(&self, _: &[Expr], inputs: &[LogicalPlan]) -> Arc<dyn UserDefinedLogicalNode> {
        Arc::new(
            SegmentationNode::try_new(
                inputs[0].clone(),
                self.ts_col.clone(),
                self.partition_cols.clone(),
                self.exprs.clone(),
            )
            .map_err(QueryError::into_datafusion_plan_error)
            .unwrap(),
        )
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
