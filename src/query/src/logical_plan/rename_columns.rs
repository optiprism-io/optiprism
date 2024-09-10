use std::fmt::Debug;
use std::fmt::Formatter;
use std::hash::Hash;
use std::hash::Hasher;
use std::sync::Arc;

use arrow::datatypes::Field;
use arrow::datatypes::FieldRef;
use datafusion_common::DFSchema;
use datafusion_common::DFSchemaRef;
use datafusion_expr::Expr;
use datafusion_expr::LogicalPlan;
use datafusion_expr::UserDefinedLogicalNode;

use crate::error::QueryError;
use crate::Result;

#[derive(Hash, Eq, PartialEq)]
pub struct RenameColumnsNode {
    pub input: LogicalPlan,
    pub columns: Vec<(String, String)>,
    schema: DFSchemaRef,
}

impl RenameColumnsNode {
    pub fn try_new(input: LogicalPlan, columns: Vec<(String, String)>) -> Result<Self> {
        let schema = input.schema();

        let fields = schema
            .fields()
            .iter()
            .map(|v| {
                for col in columns.iter() {
                    if *v.name() == col.0 {
                        return FieldRef::new(Field::new(
                            &col.1,
                            v.data_type().clone(),
                            v.is_nullable(),
                        ));
                    }
                }
                v.to_owned()
            })
            .collect::<Vec<_>>();

        Ok(Self {
            input,
            columns,
            schema: Arc::new(DFSchema::from_unqualifed_fields(
                fields.into(),
                Default::default(),
            )?),
        })
    }
}

impl Debug for RenameColumnsNode {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        self.fmt_for_explain(f)
    }
}

impl UserDefinedLogicalNode for RenameColumnsNode {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn name(&self) -> &str {
        "RenameColumnsNode"
    }

    fn inputs(&self) -> Vec<&LogicalPlan> {
        vec![&self.input]
    }

    fn schema(&self) -> &DFSchemaRef {
        &self.schema
    }

    fn expressions(&self) -> Vec<datafusion_expr::Expr> {
        vec![]
    }

    fn fmt_for_explain(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "RenameColumnsNode")
    }

    fn from_template(&self, _: &[Expr], inputs: &[LogicalPlan]) -> Arc<dyn UserDefinedLogicalNode> {
        Arc::new(
            RenameColumnsNode::try_new(inputs[0].to_owned(), self.columns.clone())
                .map_err(QueryError::into_datafusion_plan_error)
                .unwrap(),
        )
    }

    fn with_exprs_and_inputs(
        &self,
        _exprs: Vec<Expr>,
        inputs: Vec<LogicalPlan>,
    ) -> datafusion_common::Result<Arc<dyn UserDefinedLogicalNode>> {
        Ok(Arc::new(
            RenameColumnsNode::try_new(inputs[0].to_owned(), self.columns.clone())
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
