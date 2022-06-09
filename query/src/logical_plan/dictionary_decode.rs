use std::any::Any;
use std::fmt::{Debug, Formatter};
use std::sync::Arc;
use arrow::datatypes::{DataType, Field};
use datafusion::logical_plan::{LogicalPlan, DFSchemaRef, UserDefinedLogicalNode};
use datafusion_common::{Column, DFSchema};
use datafusion_expr::Expr;
use crate::{Error, Result};

pub struct DictionaryDecodeNode {
    input: LogicalPlan,
    decode_cols: Vec<String>,
    schema: DFSchemaRef,
}

impl DictionaryDecodeNode {
    pub fn try_new(input: LogicalPlan, decode_cols: Vec<String>) -> Result<Self> {
        let fields = input
            .schema()
            .fields()
            .iter()
            .map(|field| match decode_cols.contains(field.name()) {
                true => Field::new(field.name().as_str(), DataType::Utf8, field.is_nullable()),
                false => field.clone()
            })
            .collect();

        let schema = Arc::new(DFSchema::new(fields)?);

        Ok(Self {
            input,
            decode_cols,
            schema,
        })
    }
}

impl Debug for DictionaryDecodeNode {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        self.fmt_for_explain(f)
    }
}

impl UserDefinedLogicalNode for DictionaryDecodeNode {
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
        write!(f, "DictionaryDecode")
    }

    fn from_template(&self, _exprs: &[Expr], inputs: &[LogicalPlan]) -> Arc<dyn UserDefinedLogicalNode + Send + Sync> {
        Arc::new(DictionaryDecodeNode::new(
            inputs[0].clone(),
            self.decode_cols.clone(),
        ))
    }
}