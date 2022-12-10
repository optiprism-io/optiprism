use std::any::Any;
use std::collections::HashMap;
use std::fmt::Debug;
use std::fmt::Formatter;
use std::sync::Arc;

use datafusion_common::Column;
use datafusion_common::DFField;
use datafusion_common::DFSchema;
use datafusion_common::DFSchemaRef;
use datafusion_expr::Expr;
use datafusion_expr::LogicalPlan;
use datafusion_expr::UserDefinedLogicalNode;

use crate::error::QueryError;
use crate::Result;

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
            let mut fields: Vec<DFField> = input
                .schema()
                .fields()
                .iter()
                .filter_map(|f| {
                    match f.name().clone() == name_col.name || f.name().clone() == value_col.name {
                        true => None,
                        false => Some(DFField::new(None, f.name(), f.data_type().clone(), true)),
                    }
                })
                .collect();

            let value_type = input
                .schema()
                .field_with_name(None, value_col.flat_name().as_str())?
                .data_type()
                .clone();

            let result_fields: Vec<DFField> = result_cols
                .iter()
                .map(|col| DFField::new(None, col, value_type.clone(), true))
                .collect();

            fields.extend_from_slice(&result_fields);

            Arc::new(DFSchema::new_with_metadata(fields, HashMap::new())?)
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
}
