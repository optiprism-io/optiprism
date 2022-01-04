use arrow::array::ArrayRef;
use datafusion::logical_plan::Operator;
use datafusion::physical_plan::ColumnarValue;

pub fn into_array(cv: ColumnarValue) -> ArrayRef {
    match cv {
        ColumnarValue::Array(array) => array,
        _ => panic!("unexpected"),
    }
}

pub fn break_on_false(op: Operator) -> bool {
    match op {
        Operator::Eq | Operator::NotEq => false,
        Operator::Lt | Operator::LtEq => true,
        Operator::Gt | Operator::GtEq => false,
        _ => {
            panic!("unexpected")
        }
    }
}

pub fn break_on_true(op: Operator) -> bool {
    match op {
        Operator::Eq | Operator::NotEq => true,
        Operator::Lt | Operator::LtEq => false,
        Operator::Gt | Operator::GtEq => true,
        _ => {
            panic!("unexpected")
        }
    }
}
