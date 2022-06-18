use crate::{Error, Result};
use arrow::datatypes::{DataType, Field};
use datafusion::logical_plan::{DFSchemaRef, LogicalPlan, UserDefinedLogicalNode};
use datafusion_common::{Column, DFField, DFSchema};
use datafusion_expr::Expr;
use metadata::dictionaries::provider::SingleDictionaryProvider;
use std::any::Any;
use std::fmt::{Debug, Formatter};
use std::sync::Arc;

pub struct DictionaryDecodeNode {
    pub input: LogicalPlan,
    pub decode_cols: Vec<(Column, Arc<SingleDictionaryProvider>)>,
    pub schema: DFSchemaRef,
}

impl DictionaryDecodeNode {
    pub fn try_new(
        input: LogicalPlan,
        decode_cols: Vec<(Column, Arc<SingleDictionaryProvider>)>,
    ) -> Result<Self> {
        let fields = input
            .schema()
            .fields()
            .iter()
            .map(|field| {
                match decode_cols
                    .iter()
                    .find(|(col, _)| *field.name() == col.name)
                {
                    Some(_) => DFField::new(
                        field.qualifier().map(|q| q.as_str()),
                        field.name().as_str(),
                        DataType::Utf8,
                        field.is_nullable(),
                    ),
                    None => field.to_owned(),
                }
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

    fn from_template(
        &self,
        _exprs: &[Expr],
        inputs: &[LogicalPlan],
    ) -> Arc<dyn UserDefinedLogicalNode + Send + Sync> {
        Arc::new(
            DictionaryDecodeNode::try_new(inputs[0].clone(), self.decode_cols.clone()).unwrap(),
        )
    }
}
