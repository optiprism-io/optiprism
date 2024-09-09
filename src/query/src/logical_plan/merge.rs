use std::any::Any;
use std::collections::HashMap;
use std::fmt::Debug;
use std::fmt::Formatter;
use std::hash::Hash;
use std::hash::Hasher;
use std::sync::Arc;

use arrow::datatypes::DataType;
use arrow::datatypes::Field;
use datafusion_common::DFSchema;
use datafusion_common::DFSchemaRef;
use datafusion_expr::Expr;
use datafusion_expr::LogicalPlan;
use datafusion_expr::UserDefinedLogicalNode;

use crate::error::QueryError;
use crate::Result;

#[derive(Hash, Eq, PartialEq)]
pub struct MergeNode {
    inputs: Vec<LogicalPlan>,
    pub names: Option<(String, Vec<String>)>,
    schema: DFSchemaRef,
}

impl MergeNode {
    pub fn try_new(inputs: Vec<LogicalPlan>, names: Option<(String, Vec<String>)>) -> Result<Self> {
        let mut schema = DFSchema::new_with_metadata(vec![], HashMap::new())?;
        for input in inputs.iter() {
            schema.merge(input.schema());
        }

        schema = if let Some((col_name, _)) = names.clone() {
            let fields = [
                vec![Arc::new(Field::new(col_name, DataType::Utf8, false))],
                schema.fields().to_vec(),
            ]
            .concat();

            DFSchema::from_unqualifed_fields(fields.into(), HashMap::default())?
        } else {
            schema
        };
        Ok(Self {
            inputs,
            names,
            schema: Arc::new(schema),
        })
    }
}

impl Debug for MergeNode {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        self.fmt_for_explain(f)
    }
}

impl UserDefinedLogicalNode for MergeNode {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "Merge"
    }

    fn inputs(&self) -> Vec<&LogicalPlan> {
        self.inputs.iter().collect()
    }

    fn schema(&self) -> &DFSchemaRef {
        &self.schema
    }

    fn expressions(&self) -> Vec<Expr> {
        vec![]
    }

    fn fmt_for_explain(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "Merge")
    }

    fn from_template(&self, _: &[Expr], inputs: &[LogicalPlan]) -> Arc<dyn UserDefinedLogicalNode> {
        Arc::new(
            MergeNode::try_new(inputs.to_vec(), self.names.clone())
                .map_err(QueryError::into_datafusion_plan_error)
                .unwrap(),
        )
    }

    fn with_exprs_and_inputs(
        &self,
        _: Vec<Expr>,
        inputs: Vec<LogicalPlan>,
    ) -> datafusion_common::Result<Arc<dyn UserDefinedLogicalNode>> {
        Ok(Arc::new(
            Self::try_new(inputs, self.names.clone())
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
