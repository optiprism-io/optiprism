use std::any::Any;
use std::collections::HashMap;
use std::fmt::Debug;
use std::fmt::Formatter;
use std::hash::Hash;
use std::hash::Hasher;
use std::sync::Arc;

use arrow::datatypes::DataType;
use arrow::datatypes::Field;
use common::DECIMAL_PRECISION;
use common::DECIMAL_SCALE;
use datafusion_common::DFSchema;
use datafusion_common::DFSchemaRef;
use datafusion_expr::Expr;
use datafusion_expr::LogicalPlan;
use datafusion_expr::UserDefinedLogicalNode;

use crate::error::QueryError;
use crate::Result;

#[derive(Hash, Eq, PartialEq)]
pub struct AggregateAndSortColumnsNode {
    input: LogicalPlan,
    pub groups: usize,
    schema: DFSchemaRef,
}

impl AggregateAndSortColumnsNode {
    pub fn try_new(input: LogicalPlan, groups: usize) -> Result<Self> {
        let schema = input.schema();

        let mut cols = vec![];

        for (idx, f) in schema.fields().iter().enumerate() {
            cols.push(f.to_owned());
            if idx == groups - 1 {
                let col = Field::new(
                    "Average",
                    DataType::Decimal128(DECIMAL_PRECISION, DECIMAL_SCALE),
                    false,
                );
                cols.push(Arc::new(col));
            }
        }

        Ok(Self {
            input,
            groups,
            schema: Arc::new(DFSchema::from_unqualifed_fields(
                cols.into(),
                HashMap::default(),
            )?),
        })
    }
}

impl Debug for AggregateAndSortColumnsNode {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        self.fmt_for_explain(f)
    }
}

impl UserDefinedLogicalNode for AggregateAndSortColumnsNode {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "AggregateColumns"
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
        write!(f, "AggregateColumns")
    }

    fn from_template(
        &self,
        _exprs: &[Expr],
        inputs: &[LogicalPlan],
    ) -> Arc<dyn UserDefinedLogicalNode> {
        Arc::new(AggregateAndSortColumnsNode::try_new(inputs[0].clone(), self.groups).unwrap())
    }

    fn with_exprs_and_inputs(
        &self,
        _: Vec<Expr>,
        inputs: Vec<LogicalPlan>,
    ) -> datafusion_common::Result<Arc<dyn UserDefinedLogicalNode>> {
        Ok(Arc::new(
            Self::try_new(inputs[0].clone(), self.groups)
                .map_err(QueryError::into_datafusion_plan_error)?,
        ))
    }

    fn dyn_hash(&self, state: &mut dyn Hasher) {
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
