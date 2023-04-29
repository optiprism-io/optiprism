use std::any::Any;
use std::collections::HashMap;
use std::fmt::Debug;
use std::fmt::Formatter;
use std::hash::Hasher;
use std::sync::Arc;
use std::hash::Hash;

use arrow::datatypes::DataType;
use datafusion_common::{Column, TableReference};
use datafusion_common::DFField;
use datafusion_common::DFSchema;
use datafusion_common::DFSchemaRef;
use datafusion_expr::logical_plan::LogicalPlan;
use datafusion_expr::logical_plan::UserDefinedLogicalNode;
use datafusion_expr::Expr;
use metadata::dictionaries::provider_impl::SingleDictionaryProvider;

use crate::Result;

#[derive(Hash)]
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
                        field.qualifier(),
                        field.name().as_str(),
                        DataType::Utf8,
                        field.is_nullable(),
                    ),
                    None => field.to_owned(),
                }
            })
            .collect();

        let schema = Arc::new(DFSchema::new_with_metadata(fields, HashMap::new())?);

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

    fn name(&self) -> &str {
        "DictionaryDecode"
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
    ) -> Arc<dyn UserDefinedLogicalNode> {
        Arc::new(
            DictionaryDecodeNode::try_new(inputs[0].clone(), self.decode_cols.clone()).unwrap(),
        )
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
