use std::fmt::Debug;
use std::fmt::Formatter;
use std::hash::Hash;
use std::hash::Hasher;
use std::sync::Arc;

use datafusion_common::DFSchema;
use datafusion_common::DFSchemaRef;
use datafusion_expr::Expr;
use datafusion_expr::LogicalPlan;
use datafusion_expr::UserDefinedLogicalNode;

use crate::error::QueryError;
use crate::logical_plan::merge::MergeNode;
use crate::Result;

#[derive(Hash, Eq, PartialEq)]
pub struct ReorderColumnsNode {
    pub input: LogicalPlan,
    pub columns: Vec<String>,
    schema: DFSchemaRef,
}

impl ReorderColumnsNode {
    pub fn try_new(input: LogicalPlan, columns: Vec<String>) -> Result<Self> {
        let schema = input.schema();
        let mut reordered_cols = vec![];

        for group_col in columns.iter() {
            reordered_cols.push(Arc::new(
                schema.field_with_unqualified_name(group_col)?.to_owned(),
            ));
        }
        for field in schema.fields().iter() {
            if !columns.contains(&field.name()) {
                reordered_cols.push(field.clone());
            }
        }

        Ok(Self {
            input,
            columns,
            schema: Arc::new(DFSchema::from_unqualifed_fields(
                reordered_cols.into(),
                Default::default(),
            )?),
        })
    }
}

impl Debug for ReorderColumnsNode {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        self.fmt_for_explain(f)
    }
}

impl UserDefinedLogicalNode for ReorderColumnsNode {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn name(&self) -> &str {
        "ReorderColumns"
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
        write!(f, "ReorderColumns")
    }

    fn from_template(&self, _: &[Expr], inputs: &[LogicalPlan]) -> Arc<dyn UserDefinedLogicalNode> {
        Arc::new(
            ReorderColumnsNode::try_new(inputs[0].to_owned(), self.columns.clone())
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
            ReorderColumnsNode::try_new(inputs[0].to_owned(), self.columns.clone())
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
