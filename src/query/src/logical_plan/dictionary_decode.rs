use std::any::Any;
use std::collections::HashMap;
use std::fmt::Debug;
use std::fmt::Formatter;
use std::hash::Hash;
use std::hash::Hasher;
use std::sync::Arc;

use arrow::datatypes::DataType;
use arrow::datatypes::Field;
use arrow::datatypes::Fields;
use datafusion_common::Column;
use datafusion_common::DFSchema;
use datafusion_common::DFSchemaRef;
use datafusion_expr::logical_plan::LogicalPlan;
use datafusion_expr::logical_plan::UserDefinedLogicalNode;
use datafusion_expr::Expr;
use metadata::dictionaries::SingleDictionaryProvider;

use crate::Result;

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
                    Some(_) => Arc::new(Field::new(
                        field.name().as_str(),
                        DataType::Utf8,
                        field.is_nullable(),
                    )),
                    None => field.to_owned(),
                }
            })
            .collect::<Vec<_>>();

        let schema = Arc::new(DFSchema::from_unqualifed_fields(
            Fields::from(fields),
            HashMap::new(),
        )?);

        Ok(Self {
            input,
            decode_cols,
            schema,
        })
    }
}

impl Hash for DictionaryDecodeNode {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.input.hash(state);
        self.decode_cols.hash(state);
        self.schema.hash(state);
    }
}

impl PartialEq for DictionaryDecodeNode {
    fn eq(&self, other: &Self) -> bool {
        let res = self.input == other.input && self.schema == other.schema;
        if !res {
            return false;
        }
        let cols = self
            .decode_cols
            .iter()
            .map(|v| v.0.clone())
            .collect::<Vec<_>>();
        let other_cols = other
            .decode_cols
            .iter()
            .map(|v| v.0.clone())
            .collect::<Vec<_>>();
        cols == other_cols
    }
}

impl Eq for DictionaryDecodeNode {}

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

    fn with_exprs_and_inputs(
        &self,
        exprs: Vec<Expr>,
        inputs: Vec<LogicalPlan>,
    ) -> datafusion_common::Result<Arc<dyn UserDefinedLogicalNode>> {
        Ok(Arc::new(
            Self::try_new(inputs[0].clone(), self.decode_cols.clone())
                .map_err(|e| datafusion_common::DataFusionError::Execution(e.to_string()))?,
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
