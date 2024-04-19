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
use datafusion_common::DFField;
use datafusion_common::DFSchema;
use datafusion_common::DFSchemaRef;
use datafusion_expr::Expr;
use datafusion_expr::LogicalPlan;
use datafusion_expr::UserDefinedLogicalNode;

use crate::Result;

#[derive(Hash, Eq, PartialEq)]
pub struct AggregateColumnsNode {
    input: LogicalPlan,
    pub groups: usize,
    schema: DFSchemaRef,
}

impl AggregateColumnsNode {
    pub fn try_new(input: LogicalPlan, groups: usize) -> Result<Self> {
        let schema = input.schema();

        let mut cols = vec![];

        for (idx, f) in schema.fields().iter().enumerate() {
            cols.push(f.to_owned());
            if idx == groups - 1 {
                let col = DFField::new_unqualified(
                    "Average",
                    DataType::Decimal128(DECIMAL_PRECISION, DECIMAL_SCALE),
                    false,
                );
                cols.push(col);
            }
        }

        Ok(Self {
            input,
            groups,
            schema: Arc::new(DFSchema::new_with_metadata(cols, HashMap::default())?),
        })
    }
}

impl Debug for AggregateColumnsNode {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        self.fmt_for_explain(f)
    }
}

impl UserDefinedLogicalNode for AggregateColumnsNode {
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
        write!(f, "AddStringColumn")
    }

    fn from_template(
        &self,
        _exprs: &[Expr],
        inputs: &[LogicalPlan],
    ) -> Arc<dyn UserDefinedLogicalNode> {
        Arc::new(AggregateColumnsNode::try_new(inputs[0].clone(), self.groups).unwrap())
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
