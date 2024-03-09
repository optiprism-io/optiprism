use std::fmt::Debug;
use std::fmt::Formatter;
use std::hash::Hash;
use std::hash::Hasher;
use std::sync::Arc;

use datafusion_common::Column;
use datafusion_common::DFField;
use datafusion_common::DFSchema;
use datafusion_common::DFSchemaRef;
use datafusion_expr::Expr;
use datafusion_expr::LogicalPlan;
use datafusion_expr::UserDefinedLogicalNode;
use metadata::MetadataProvider;

use crate::error::QueryError;
use crate::logical_plan::merge::MergeNode;
use crate::Result;

#[derive(Hash, Eq, PartialEq)]
pub struct RenameColumnRowsNode {
    pub input: LogicalPlan,
    pub column: Column,
    pub rename: Vec<(String, String)>,
}

impl RenameColumnRowsNode {
    pub fn try_new(
        input: LogicalPlan,
        column: Column,
        rename: Vec<(String, String)>,
    ) -> Result<Self> {
        let schema = input.schema();

        Ok(Self {
            input,
            column,
            rename,
        })
    }
}

impl Debug for RenameColumnRowsNode {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        self.fmt_for_explain(f)
    }
}

impl UserDefinedLogicalNode for RenameColumnRowsNode {
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
        self.input.schema()
    }

    fn expressions(&self) -> Vec<datafusion_expr::Expr> {
        vec![]
    }

    fn fmt_for_explain(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "RenameColumnsNode")
    }

    fn from_template(&self, _: &[Expr], inputs: &[LogicalPlan]) -> Arc<dyn UserDefinedLogicalNode> {
        Arc::new(
            RenameColumnRowsNode::try_new(
                inputs[0].to_owned(),
                self.column.clone(),
                self.rename.to_vec(),
            )
            .map_err(QueryError::into_datafusion_plan_error)
            .unwrap(),
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
