use std::fmt::Debug;


use ahash::HashMap;

use arrow::array::ArrayRef;
use arrow::datatypes::Field;
use arrow::record_batch::RecordBatch;
use arrow_row::OwnedRow;
use arrow_row::RowConverter;
use arrow_row::SortField;

use datafusion::physical_expr::PhysicalExprRef;










use crate::error::Result;

pub mod aggregate;
pub mod count;
pub mod partitioned;

#[derive(Debug)]
struct Groups<T> {
    exprs: Vec<PhysicalExprRef>,
    names: Vec<String>,
    sort_fields: Vec<SortField>,
    row_converter: RowConverter,
    groups: HashMap<OwnedRow, T>,
}

impl<T> Groups<T> {
    pub fn try_make_new(&self) -> Result<Self> {
        Ok(Groups {
            exprs: self.exprs.clone(),
            names: self.names.clone(),
            sort_fields: self.sort_fields.clone(),
            row_converter: RowConverter::new(self.sort_fields.clone())?,
            groups: Default::default(),
        })
    }

    pub fn maybe_from(
        groups: Option<Vec<(PhysicalExprRef, String, SortField)>>,
    ) -> Result<Option<Self>> {
        if let Some(groups) = groups {
            Ok(Some(Self {
                exprs: groups.iter().map(|(v, _, _)| v.clone()).collect::<Vec<_>>(),
                names: groups.iter().map(|(_, v, _)| v.to_owned()).collect(),
                sort_fields: groups.iter().map(|(_, _, v)| v.clone()).collect(),
                row_converter: RowConverter::new(
                    groups.iter().map(|(_, _, s)| s.clone()).collect(),
                )?,
                groups: Default::default(),
            }))
        } else {
            Ok(None)
        }
    }
}

pub trait PartitionedAggregateExpr: Send + Sync + Debug {
    fn group_columns(&self) -> Vec<(PhysicalExprRef, String)>;
    fn fields(&self) -> Vec<Field>;
    fn evaluate(
        &mut self,
        batch: &RecordBatch,
        partition_exist: Option<&HashMap<i64, ()>>,
    ) -> Result<()>;
    fn finalize(&mut self) -> Result<Vec<ArrayRef>>;
    fn make_new(&self) -> Result<Box<dyn PartitionedAggregateExpr>>;
}
