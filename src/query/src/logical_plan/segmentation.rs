use std::any::Any;
use std::collections::HashMap;
use std::fmt::Debug;
use std::fmt::Formatter;
use std::hash::Hasher;
use std::sync::Arc;

use arrow::datatypes::DataType;
use chrono::DateTime;
use chrono::Utc;
use common::query::event_segmentation::SegmentTime;
use common::query::Operator;
use datafusion_common::Column;
use datafusion_common::DFField;
use datafusion_common::DFSchema;
use datafusion_common::DFSchemaRef;
use datafusion_common::Result as DFResult;
use datafusion_common::ScalarValue;
use datafusion_expr::Expr;
use datafusion_expr::LogicalPlan;
use datafusion_expr::UserDefinedLogicalNode;

use crate::error::QueryError;
use crate::Result;

#[derive(Hash, Debug, Clone, Eq, PartialEq)]
pub struct Agg {
    pub left: Column,
    pub op: Operator,
    pub right: ScalarValue,
}

#[derive(Hash, Debug, Clone, Eq, PartialEq)]
pub enum AggregateFunction {
    Sum(Agg),
    Min(Agg),
    Max(Agg),
    Avg(Agg),
    Count { op: Operator, right: i64 },
}

#[derive(Hash, Debug, Clone, Eq, PartialEq)]
pub enum TimeRange {
    Between(i64, i64),
    From(i64),
    Last(i64, i64),
    None,
}

impl TimeRange {
    pub fn from_segment_time(t: SegmentTime, cur_time: DateTime<Utc>) -> TimeRange {
        match t {
            // todo make common
            SegmentTime::Between { from, to } => {
                TimeRange::Between(from.timestamp_millis(), to.timestamp_millis())
            }
            SegmentTime::From(from) => TimeRange::From(from.timestamp_millis()),
            SegmentTime::Last { n, unit } => TimeRange::Last(
                unit.duration(*n).num_milliseconds(),
                cur_time.num_milliseconds(),
            ),
            SegmentTime::Each { n, unit } => TimeRange::None,
            _ => unimplemented!(),
        }
    }
}

impl From<SegmentTime> for TimeRange {
    fn from(value: SegmentTime) -> Self {}
}

#[derive(Hash, Eq, PartialEq)]
pub struct SegmentationNode {
    pub input: LogicalPlan,
    pub ts_col: Column,
    pub schema: DFSchemaRef,
    pub partition_cols: Vec<Column>,
    pub exprs: Vec<Vec<SegmentationExpr>>,
}

#[derive(Hash, Debug, Clone, Eq, PartialEq)]
pub struct SegmentationExpr {
    pub filter: Expr,
    pub agg_fn: AggregateFunction,
    pub time_range: TimeRange,
    pub time_window: Option<i64>,
}

impl SegmentationNode {
    pub fn try_new(
        input: LogicalPlan,
        ts_col: Column,
        partition_cols: Vec<Column>,
        exprs: Vec<Vec<SegmentationExpr>>,
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
