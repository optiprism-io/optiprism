use std::any::Any;
use std::collections::HashMap;
use std::fmt::{Debug, Formatter};
use std::sync::Arc;

use arrow::datatypes::DataType;
use datafusion::logical_plan::{DFSchemaRef, LogicalPlan, UserDefinedLogicalNode};
use datafusion_common::{DFField, DFSchema};
use datafusion_expr::Expr;

use common::{DECIMAL_PRECISION, DECIMAL_SCALE};

use crate::error::QueryError;
use crate::Result;

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
            let mut fields: Vec<DFField> = input
                .schema()
                .fields()
                .iter()
                .filter_map(|f| match cols.contains(f.name()) {
                    true => None,
                    false => Some(f.clone()),
                })
                .collect();

            let name_field = DFField::new(None, name_col.as_str(), DataType::Utf8, false);
            fields.push(name_field);
            let value_field = DFField::new(None, value_col.as_str(), value_type, false);
            fields.push(value_field);

            Arc::new(DFSchema::new_with_metadata(fields,HashMap::new())?)
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

    fn from_template(
        &self,
        _: &[Expr],
        inputs: &[LogicalPlan],
    ) -> Arc<dyn UserDefinedLogicalNode> {
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
}
