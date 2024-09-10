use std::any::Any;
use std::collections::HashMap;
use std::fmt::Debug;
use std::fmt::Formatter;
use std::hash::Hasher;
use std::sync::Arc;

use arrow::datatypes::Field;
use arrow::datatypes::FieldRef;
use datafusion_common::Column;
use datafusion_common::DFSchema;
use datafusion_common::DFSchemaRef;
use datafusion_expr::Expr;
use datafusion_expr::LogicalPlan;
use datafusion_expr::UserDefinedLogicalNode;

use crate::error::QueryError;
use crate::Result;

#[derive(Hash, Eq, PartialEq)]
pub struct PivotNode {
    pub input: LogicalPlan,
    pub schema: DFSchemaRef,
    pub name_col: Column,
    pub value_col: Column,
    pub result_cols: Vec<String>,
}

impl PivotNode {
    pub fn try_new(
        input: LogicalPlan,
        name_col: Column,
        value_col: Column,
        result_cols: Vec<String>,
    ) -> Result<Self> {
        let schema = {
            let mut fields = input
                .schema()
                .fields()
                .iter()
                .filter_map(|f| {
                    match f.name().clone() == name_col.name || f.name().clone() == value_col.name {
                        true => None,
                        false => Some(f.to_owned()),
                    }
                })
                .collect::<Vec<_>>();

            let value_type = input
                .schema()
                .field_with_name(None, value_col.flat_name().as_str())?
                .data_type()
                .clone();

            let mut result_fields = result_cols
                .iter()
                .map(|col| FieldRef::new(Field::new(col, value_type.clone(), true)))
                .collect();

            fields.append(&mut result_fields);

            Arc::new(DFSchema::from_unqualifed_fields(
                fields.into(),
                HashMap::new(),
            )?)
        };

        Ok(Self {
            input,
            schema,
            name_col,
            value_col,
            result_cols,
        })
    }
}

impl Debug for PivotNode {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        self.fmt_for_explain(f)
    }
}

impl UserDefinedLogicalNode for PivotNode {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "Pivot"
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
        write!(f, "Pivot")
    }

    fn from_template(&self, _: &[Expr], inputs: &[LogicalPlan]) -> Arc<dyn UserDefinedLogicalNode> {
        Arc::new(
            PivotNode::try_new(
                inputs[0].clone(),
                self.name_col.clone(),
                self.value_col.clone(),
                self.result_cols.clone(),
            )
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
            Self::try_new(
                inputs[0].clone(),
                self.name_col.clone(),
                self.value_col.clone(),
                self.result_cols.clone(),
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
