pub mod aggregate;
pub mod count;
pub mod funnel;

use std::collections::HashMap;
use std::fmt::Debug;
use std::fmt::Display;
use std::fmt::Formatter;
use std::marker::PhantomData;
use std::ops::Add;
use std::sync::Arc;

use arrow::array::Array;
use arrow::array::ArrayRef;
use arrow::array::BooleanArray;
use arrow::array::TimestampMillisecondArray;
use arrow::buffer::ScalarBuffer;
use arrow::datatypes::DataType;
use arrow::datatypes::Field;
use arrow::record_batch::RecordBatch;
// mod count_hash;
// mod aggregate;
// pub mod aggregate;
use common::DECIMAL_SCALE;
use datafusion::physical_expr::expressions::Column;
use datafusion::physical_expr::PhysicalExpr;
use datafusion_expr::ColumnarValue;
use datafusion_expr::Operator;
use num::Integer;
use num_traits::Bounded;
use num_traits::Num;
use num_traits::NumCast;
use num_traits::PrimInt;
use num_traits::Zero;
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use tracing::debug;

use crate::error::Result;
