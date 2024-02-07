use std::any::Any;
use std::collections::HashMap;
use std::fmt::Debug;
use std::fmt::Formatter;
use std::hash::Hash;
use std::hash::Hasher;
use std::sync::Arc;

use arrow::datatypes::DataType;
use datafusion_common::DFField;
use datafusion_common::DFSchema;
use datafusion_common::DFSchemaRef;
use datafusion_expr::Expr;
use datafusion_expr::LogicalPlan;
use datafusion_expr::UserDefinedLogicalNode;

use crate::logical_plan::merge::MergeNode;
use crate::Result;

#[derive(Hash, Eq, PartialEq)]
pub struct AddStringColumnNode {
    input: LogicalPlan,
    pub col: (String, String),
    schema: DFSchemaRef,
}

impl AddStringColumnNode {
    pub fn try_new(input: LogicalPlan, col: (String, String)) -> Result<Self> {
        let schema = input.schema();
        let fields = vec![
            vec![DFField::new_unqualified(&col.0, DataType::Utf8, false)],
            schema.fields().to_vec(),
        ]
        .concat();

        Ok(Self {
            input,
            col,
            schema: Arc::new(DFSchema::new_with_metadata(fields, HashMap::default())?),
        })
    }
}

impl Debug for AddStringColumnNode {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        self.fmt_for_explain(f)
    }
}

impl UserDefinedLogicalNode for AddStringColumnNode {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "AddStringColumn"
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
        exprs: &[Expr],
        inputs: &[LogicalPlan],
    ) -> Arc<dyn UserDefinedLogicalNode> {
        Arc::new(AddStringColumnNode::try_new(inputs[0].clone(), self.col.clone()).unwrap())
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
