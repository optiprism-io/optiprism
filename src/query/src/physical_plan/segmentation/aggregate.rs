use std::fmt::{Debug, Display, Formatter};
use std::marker::PhantomData;
use std::ops::Add;
use std::sync::Arc;

use arrow::array::{Array, Float16Array, Int64Array, Int8Array, Int16Array, Int32Array, UInt8Array, UInt16Array, UInt32Array, UInt64Array, Float32Array, Float64Array,Decimal128Array};
use arrow::array::ArrayRef;
use arrow::array::BooleanArray;
use arrow::compute::kernels;
use arrow::compute::kernels::arithmetic::add;
use arrow::compute::kernels::arithmetic::divide;
use arrow::compute::kernels::arithmetic::divide_scalar;
use arrow::compute::kernels::arithmetic::multiply;
use arrow::compute::kernels::arithmetic::subtract;
use arrow::datatypes::{ArrowNativeType, DataType};
use arrow::datatypes::DataType::Duration;
use arrow::datatypes::{Schema};
use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use datafusion::physical_expr::expressions::Column;
use datafusion::physical_plan::ColumnarValue;
use datafusion::physical_plan::PhysicalExpr;
use datafusion_expr::Literal;
use crate::error::QueryError;
use crate::error::Result;
use crate::physical_plan::segmentation::boolean_op::BooleanOp;
use super::RowResult;
use crate::physical_plan::segmentation::{Expr, Spans};
use num_traits::{AsPrimitive, Bounded, FromPrimitive, Num, NumAssign, NumCast, PrimInt};

#[derive(Debug)]
enum AggregateFunction {
    Sum,
    Min,
    Max,
    Avg,
}

pub trait Primitive: Copy + Num + Bounded + NumCast + PartialOrd + Clone {}

pub trait Accumulator<T>: Send + Sync {
    fn perform(acc: T, v: T) -> T;
    fn fn_name() -> AggregateFunction;
}

#[derive(Debug)]
struct Sum;

impl<T> Accumulator<T> for Sum where T: Add<Output=T> {
    fn perform(acc: T, v: T) -> T {
        return acc + v;
    }

    fn fn_name() -> AggregateFunction {
        AggregateFunction::Sum
    }
}

#[derive(Debug)]
pub struct Aggregate<T, Arr, Op, Acc> where T: Debug {
    predicate: Arc<dyn PhysicalExpr>,
    op: PhantomData<Op>,
    acc: PhantomData<Acc>,
    arr: PhantomData<Arr>,
    left_col: Column,
    right: T,
    left: T,
    spans: Spans,
}


impl<T, Arr, Op, Acc> Aggregate<T, Arr, Op, Acc> where Acc: Accumulator<T>, T: Debug + Default, Op: Debug {
    pub fn try_new(schema: &Schema, left: Column, predicate: Arc<dyn PhysicalExpr>, right: T) -> Result<Self> {
        if !predicate.data_type(schema)?.equals_datatype(&DataType::Boolean) {
            return Err(QueryError::Plan("Predicate should have boolean type".to_string()));
        }

        match Acc::fn_name() {
            AggregateFunction::Sum => {
                if !left.data_type(&schema)?.is_numeric() {
                    return Err(QueryError::Plan("Column should be numeric".to_string()));
                }
            }
            _ => unimplemented!("Aggregate function not implemented"),
        }

        Ok(Aggregate {
            predicate,
            op: PhantomData,
            acc: PhantomData,
            arr: PhantomData,
            left_col: left,
            right,
            left: T::default(),
            spans: Spans::new(100),
        })
    }
}

impl<T, Arr, Op, Acc> Display for Aggregate<T, Arr, Op, Acc> where T: Debug, Op: BooleanOp<T>, Acc: Accumulator<T> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}({:?}){:?}{:?}", Acc::fn_name(), self.left_col, Op::op(), self.right)
    }
}


macro_rules! gen_evaluate_int {
    ($acc_ty:ty,$array_type:ident) => {
        impl<Op, Acc> Expr for Aggregate<$acc_ty, $array_type,Op, Acc> where Op: BooleanOp<$acc_ty>, Acc: Accumulator<$acc_ty> + Debug {
            fn evaluate(&mut self, spans: &[usize], batch: &RecordBatch, is_last: bool) -> Result<Option<Vec<i64>>> {
                self.spans.reset(spans.to_vec(), batch.columns()[0].len());
                let predicate_arr = self.predicate.evaluate(batch)?.into_array(0);
                let predicate_arr = predicate_arr.as_any().downcast_ref::<BooleanArray>().unwrap();
                let left_arr = self.left_col.evaluate(batch)?.into_array(0);
                let left_arr = left_arr.as_any().downcast_ref::<$array_type>().unwrap();

                while self.spans.next_span() {
                    while let Some(row) = self.spans.next_row() {
                        match row {
                            RowResult::NextPartition(row_id) => {
                                if Op::perform(self.left, self.right) || self.spans.cur_span() == 0 {
                                    self.spans.push_result();
                                }

                                self.left = 0;

                                println!("left {}", self.left);

                                break;
                            }
                            RowResult::NextRow(row_id) => {
                                if predicate_arr.value(row_id as usize) {
                                    self.left = Acc::perform(self.left, left_arr.value(row_id as usize) as $acc_ty);
                                }
                                println!("left {}", self.left);
                            }
                        }
                    }
                }

                if is_last && self.spans.check_last_span() && Op::perform(self.left, self.right) {
                    self.spans.push_final_result()
                }
                Ok(self.spans.result())
            }
        }
    }
}

macro_rules! gen_evaluate_float {
    ($acc_ty:ty,$array_type:ident) => {
        impl<Op, Acc> Expr for Aggregate<$acc_ty, $array_type,Op, Acc> where Op: BooleanOp<$acc_ty>, Acc: Accumulator<$acc_ty> + Debug {
            fn evaluate(&mut self, spans: &[usize], batch: &RecordBatch, is_last: bool) -> Result<Option<Vec<i64>>> {
                self.spans.reset(spans.to_vec(), batch.columns()[0].len());
                let predicate_arr = self.predicate.evaluate(batch)?.into_array(0);
                let predicate_arr = predicate_arr.as_any().downcast_ref::<BooleanArray>().unwrap();
                let left_arr = self.left_col.evaluate(batch)?.into_array(0);
                let left_arr = left_arr.as_any().downcast_ref::<$array_type>().unwrap();

                while self.spans.next_span() {
                    while let Some(row) = self.spans.next_row() {
                        match row {
                            RowResult::NextPartition(row_id) => {
                                if Op::perform(self.left, self.right) || self.spans.cur_span() == 0 {
                                    self.spans.push_result();
                                }

                                self.left = 0.0;

                                println!("left {}", self.left);

                                break;
                            }
                            RowResult::NextRow(row_id) => {
                                if predicate_arr.value(row_id as usize) {
                                    self.left = Acc::perform(self.left, left_arr.value(row_id as usize) as $acc_ty);
                                }
                                println!("left {}", self.left);
                            }
                        }
                    }
                }

                if is_last && self.spans.check_last_span() && Op::perform(self.left, self.right) {
                    self.spans.push_final_result()
                }
                Ok(self.spans.result())
            }
        }
    }
}

gen_evaluate_int!(i64,Int8Array);
gen_evaluate_int!(i64,Int16Array);
gen_evaluate_int!(i64,Int32Array);
gen_evaluate_int!(i128,Int64Array);
gen_evaluate_int!(u64,UInt8Array);
gen_evaluate_int!(u64,UInt16Array);
gen_evaluate_int!(u64,UInt32Array);
gen_evaluate_int!(u128,UInt64Array);
// todo add decimal 256
gen_evaluate_int!(i128,Decimal128Array);
gen_evaluate_float!(f64,Float32Array);
gen_evaluate_float!(f64,Float64Array);

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use arrow::array::{ArrayRef, BooleanArray, Int8Array};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use datafusion::physical_expr::expressions::Column;
    use datafusion::physical_expr::PhysicalExpr;
    use datafusion::sql::sqlparser::ast::BinaryOperator;
    use crate::physical_plan::segmentation::aggregate::{Accumulator, Aggregate, Sum};
    use crate::physical_plan::segmentation::boolean_op::{BooleanEq, BooleanGt};
    use crate::physical_plan::segmentation::Expr;

    #[test]
    fn sum() -> anyhow::Result<()> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Boolean, false),
            Field::new("b", DataType::Int8, false),
        ]));

        let bcol = Arc::new(Column::new_with_schema("a", &schema)?) as Arc<dyn PhysicalExpr>;
        let valcol = Column::new_with_schema("b", &schema)?;
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(BooleanArray::from(vec![
                    true, true, true, true, true,
                ])),
                Arc::new(Int8Array::from(vec![2, 2, 3, 4, 5])),
            ],
        )?;
        let mut op = Aggregate::<i64, Int8Array, BooleanGt, Sum>::try_new(&schema, valcol.clone(), bcol.clone(), 1)?;
        assert_eq!(op.evaluate(&[1, 3], &batch, false)?, Some(vec![0, 2]));

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(BooleanArray::from(vec![
                    true, true, true, true, true,
                ])),
                //                                         1     5  9
                Arc::new(Int8Array::from(vec![1, 2, 3, 4, 5])),
            ],
        )?;
        let mut op = Aggregate::<i64, Int8Array, BooleanGt, Sum>::try_new(&schema, valcol, bcol.clone(), 2)?;
        assert_eq!(op.evaluate(&[1, 3], &batch, true)?, Some(vec![2, 4]));
        Ok(())
    }
}
