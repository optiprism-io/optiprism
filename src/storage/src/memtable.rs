use arrow2::array::Array;
use arrow2::array::Int64Array;
use arrow2::array::MutableArray;
use arrow2::array::MutableBooleanArray;
use arrow2::array::MutablePrimitiveArray;
use arrow2::array::MutableUtf8Array;
use arrow2::array::PrimitiveArray;
use arrow2::chunk::Chunk;
use arrow2::compute::merge_sort::SortOptions;
use arrow2::compute::sort::lexsort_to_indices;
use arrow2::compute::sort::SortColumn;
use arrow2::compute::take;
use arrow2::compute::take::take;
use arrow2::datatypes::DataType;
use arrow2::datatypes::TimeUnit;
use common::types::DType;
use common::DECIMAL_PRECISION;
use common::DECIMAL_SCALE;

use crate::KeyValue;
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
                    .to(DataType::Timestamp(TimeUnit::Millisecond, None))
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
        is_replacing: bool,
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

        let mut arrs = arrs
            .iter()
            .map(|arr| take::take(arr.as_ref(), &indices))
            .collect::<std::result::Result<Vec<_>, _>>()?;

        if is_replacing {
            let mut indices = MutablePrimitiveArray::<i64>::new();
            if index_cols == 2 {
                let a = arrs[0]
                    .as_any()
                    .downcast_ref::<arrow2::array::Int64Array>()
                    .unwrap();

                let mut last = None;
                for row_id in 0..arrs[0].len() {
                    if last.is_none() {
                        last = Some(a.value(row_id));
                        continue;
                    }
                    if last != Some(a.value(row_id)) {
                        indices.push(Some(row_id as i64 - 1));
                        last = Some(a.value(row_id));
                    }
                }
                indices.push(Some(arrs[0].len() as i64 - 1));
            }

            let idx = indices
                .as_box()
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .to_owned();
            arrs = arrs
                .iter()
                .map(|arr| take::take(arr.as_ref(), &idx))
                .collect::<std::result::Result<Vec<_>, _>>()?;
        }
        let chunk = Chunk::new(arrs);
        Ok(Some(chunk))
    }

    pub(crate) fn push_value(&mut self, col: usize, val: Value) {
        self.cols[col].values.push(val);
    }
    pub(crate) fn push_row(&mut self, val: Vec<Value>) {
        for (idx, v) in val.iter().enumerate() {
            self.cols[idx].values.push(v.clone());
        }
    }

    pub(crate) fn get(&self, key: &[KeyValue]) -> Option<Vec<Value>> {
        let key = key.iter().map(|k| Value::from(k)).collect::<Vec<_>>();
        let mut found = false;
        let mut last_idx = None;
        for idx in 0..self.len() {
            found = true;
            for (kid, k) in key.iter().enumerate() {
                if &self.cols[kid].values[idx] != k {
                    found = false;
                    break;
                }
            }

            if found {
                last_idx = Some(idx);
            }
        }

        if let Some(idx) = last_idx {
            let vals = self
                .cols
                .iter()
                .map(|c| c.values[idx].clone())
                .collect::<Vec<_>>();
            return Some(vals);
        }

        None
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

#[cfg(test)]
mod tests {
    use common::types::DType;

    use crate::memtable::Memtable;
    use crate::KeyValue;
    use crate::Value;

    #[test]
    fn test_get() {
        let mut mt = Memtable::new();
        mt.add_column(DType::Int32);
        mt.add_column(DType::Int32);
        mt.add_column(DType::Int32);
        mt.push_row(vec![
            Value::Int32(Some(1)),
            Value::Int32(Some(1)),
            Value::Int32(Some(1)),
        ]);
        mt.push_row(vec![
            Value::Int32(Some(1)),
            Value::Int32(Some(2)),
            Value::Int32(Some(2)),
        ]);
        mt.push_row(vec![
            Value::Int32(Some(2)),
            Value::Int32(Some(1)),
            Value::Int32(Some(3)),
        ]);
        mt.push_row(vec![
            Value::Int32(Some(2)),
            Value::Int32(Some(2)),
            Value::Int32(Some(4)),
        ]);

        let res = mt.get(&[KeyValue::Int32(1)]);

        assert_eq!(
            res,
            Some(vec![
                Value::Int32(Some(1)),
                Value::Int32(Some(2)),
                Value::Int32(Some(2))
            ])
        );
    }
}
