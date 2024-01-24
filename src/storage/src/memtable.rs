use arrow2::array::Array;
use arrow2::array::MutableArray;
use arrow2::array::MutableBooleanArray;
use arrow2::array::MutablePrimitiveArray;
use arrow2::array::MutableUtf8Array;
use arrow2::chunk::Chunk;
use arrow2::compute::merge_sort::SortOptions;
use arrow2::compute::sort::lexsort_to_indices;
use arrow2::compute::sort::SortColumn;
use arrow2::compute::take;
use arrow2::datatypes::DataType;
use arrow2::datatypes::TimeUnit;
use common::types::DType;
use common::DECIMAL_PRECISION;
use common::DECIMAL_SCALE;

use crate::Value;

macro_rules! memory_col_to_arrow {
    ($col:expr, $dt:ident,$arr_ty:ident) => {{
        let vals = $col
            .into_iter()
            .map(|v| match v {
                Value::$dt(b) => b,
                Value::Null => None,
                _ => unreachable!(),
            })
            .collect::<Vec<_>>();
        $arr_ty::from(vals).as_box()
    }};
}

#[derive(Debug, Clone)]
pub(crate) struct Column {
    values: Vec<Value>,
    dt: DType,
}

impl Column {
    fn new_null(len: usize, dt: DType) -> Self {
        Self {
            values: vec![Value::Null; len],
            dt,
        }
    }

    fn clone(&self) -> Self {
        Self {
            values: self.values.clone(),
            dt: self.dt.clone(),
        }
    }

    fn len(&self) -> usize {
        self.values.len()
    }

    fn into_array(self) -> Box<dyn Array> {
        match self.dt.try_into().unwrap() {
            DataType::Boolean => {
                memory_col_to_arrow!(self.values, Boolean, MutableBooleanArray)
            }
            DataType::Int8 => memory_col_to_arrow!(self.values, Int8, MutablePrimitiveArray),
            DataType::Int16 => memory_col_to_arrow!(self.values, Int16, MutablePrimitiveArray),
            DataType::Int32 => memory_col_to_arrow!(self.values, Int32, MutablePrimitiveArray),
            DataType::Int64 => memory_col_to_arrow!(self.values, Int64, MutablePrimitiveArray),
            DataType::Timestamp(_, _) => {
                let vals = self
                    .values
                    .into_iter()
                    .map(|v| match v {
                        Value::Timestamp(b) => b,
                        Value::Null => None,
                        _ => unreachable!("{:?}", v),
                    })
                    .collect::<Vec<_>>();
                MutablePrimitiveArray::<i64>::from(vals)
                    .to(DataType::Timestamp(TimeUnit::Nanosecond, None))
                    .as_box()
            }
            DataType::Utf8 => {
                let vals = self
                    .values
                    .into_iter()
                    .map(|v| match v {
                        Value::String(b) => b,
                        Value::Null => None,
                        _ => unreachable!("{:?}", v),
                    })
                    .collect::<Vec<_>>();
                MutableUtf8Array::<i32>::from(vals).as_box()
            }
            DataType::Decimal(_, _) => {
                let vals = self
                    .values
                    .into_iter()
                    .map(|v| match v {
                        Value::Decimal(b) => b,
                        Value::Null => None,
                        _ => unreachable!("{:?}", v),
                    })
                    .collect::<Vec<_>>();
                MutablePrimitiveArray::<i128>::from(vals)
                    .to(DataType::Decimal(
                        DECIMAL_PRECISION as usize,
                        DECIMAL_SCALE as usize,
                    ))
                    .as_box()
            }
            DataType::List(_) => unimplemented!(),
            _ => unimplemented!(),
        }
    }
}

impl Memtable {
    pub(crate) fn new() -> Self {
        Self { cols: vec![] }
    }
    pub(crate) fn chunk(
        &self,
        cols: Option<Vec<usize>>,
        index_cols: usize,
    ) -> crate::error::Result<Option<Chunk<Box<dyn Array>>>> {
        if self.len() == 0 {
            return Ok(None);
        }

        let arrs = match cols {
            None => self
                .cols
                .iter()
                .map(|c| c.clone().into_array())
                .collect::<Vec<_>>(),
            Some(cols) => self
                .cols
                .iter()
                .enumerate()
                .filter(|(idx, _)| cols.contains(idx))
                .map(|(_, c)| c.clone().into_array())
                .collect::<Vec<_>>(),
        };

        let sort_cols = (0..index_cols)
            .map(|v| SortColumn {
                values: arrs[v].as_ref(),
                options: Some(SortOptions {
                    descending: false,
                    nulls_first: false,
                }),
            })
            .collect::<Vec<_>>();

        let indices = lexsort_to_indices::<i32>(&sort_cols, None)?;

        let arrs = arrs
            .iter()
            .map(|arr| take::take(arr.as_ref(), &indices))
            .collect::<std::result::Result<Vec<_>, _>>()?;

        let chunk = Chunk::new(arrs);
        Ok(Some(chunk))
    }

    pub(crate) fn push_value(&mut self, col: usize, val: Value) {
        self.cols[col].values.push(val);
    }

    pub(crate) fn len(&self) -> usize {
        if self.cols_len() == 0 {
            return 0;
        }
        self.cols[0].len()
    }

    pub(crate) fn cols_len(&self) -> usize {
        self.cols.len()
    }

    pub(crate) fn add_column(&mut self, dt: DType) {
        self.cols.push(Column::new_null(self.len(), dt));
    }

    pub(crate) fn create_empty(&self) -> Self {
        Self {
            cols: self
                .cols
                .iter()
                .map(|c| Column::new_null(0, c.dt.clone()))
                .collect::<Vec<_>>(),
        }
    }
}
#[derive(Debug, Clone)]
pub(crate) struct Memtable {
    pub(crate) cols: Vec<Column>,
}
