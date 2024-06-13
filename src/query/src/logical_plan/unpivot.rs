use std::any::Any;
use std::collections::HashMap;
use std::fmt::Debug;
use std::fmt::Formatter;
use std::hash::Hasher;
use std::sync::Arc;

use arrow::datatypes::DataType;
use arrow::datatypes::Field;
use arrow::datatypes::FieldRef;
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
pub struct UnpivotNode {
    input: LogicalPlan,
    schema: DFSchemaRef,
    pub cols: Vec<String>,
    pub name_col: String,
    pub value_col: String,
}

impl UnpivotNode {
    pub fn try_new(
        input: LogicalPlan,
        cols: Vec<String>,
        name_col: String,
        value_col: String,
    ) -> Result<Self> {
        let value_type = DataType::Decimal128(DECIMAL_PRECISION, DECIMAL_SCALE);

        let schema = {
            let mut fields: Vec<_> = input
                .schema()
                .fields()
                .iter()
                .filter_map(|f| match cols.contains(f.name()) {
                    true => None,
                    false => Some(f.to_owned()),
                })
                .collect();

            let name_field = Arc::new(Field::new(name_col.as_str(), DataType::Utf8, false));
            fields.push(name_field);
            let value_field = Arc::new(Field::new(value_col.as_str(), value_type, false));
            fields.push(value_field);

            Arc::new(DFSchema::from_unqualifed_fields(
                fields.into(),
                HashMap::new(),
            )?)
        };

        Ok(Self {
            input,
            schema,
            cols,
            name_col,
            value_col,
        })
    }
}

impl Debug for UnpivotNode {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        self.fmt_for_explain(f)
    }
}

impl UserDefinedLogicalNode for UnpivotNode {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "Unpivot"
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
        write!(f, "Unpivot")
    }

    fn from_template(&self, _: &[Expr], inputs: &[LogicalPlan]) -> Arc<dyn UserDefinedLogicalNode> {
        Arc::new(
            UnpivotNode::try_new(
                inputs[0].clone(),
                self.cols.clone(),
                self.name_col.clone(),
                self.value_col.clone(),
            )
            .map_err(QueryError::into_datafusion_plan_error)
            .unwrap(),
        )
    }

    fn with_exprs_and_inputs(
        &self,
        exprs: Vec<Expr>,
        inputs: Vec<LogicalPlan>,
    ) -> datafusion_common::Result<Arc<dyn UserDefinedLogicalNode>> {
        Ok(Arc::new(
            UnpivotNode::try_new(
                inputs[0].clone(),
                self.cols.clone(),
                self.name_col.clone(),
                self.value_col.clone(),
            )
            .map_err(QueryError::into_datafusion_plan_error)?,
        ))
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
