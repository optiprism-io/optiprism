use std::fmt::Debug;
use std::fmt::Display;
use std::fmt::Formatter;
use std::marker::PhantomData;
use std::ops::Add;
use std::sync::Arc;

use arrow::array::ArrayRef;
use arrow::record_batch::RecordBatch;
use datafusion::physical_expr::PhysicalExpr;
use datafusion_expr::ColumnarValue;
use datafusion_expr::Operator;
use num_traits::Bounded;
use num_traits::Num;
use num_traits::NumCast;
use tracing::debug;

use crate::error::Result;
use crate::Column;

// pub mod binary_op;
// pub mod _count;
mod boolean_op;
// pub mod sequence;
// pub mod sum;
// pub mod aggregate;
pub mod accumulator;
pub mod aggregate;
pub mod time_window;
// mod sequence;
// mod test_value;
// mod boolean_op;
