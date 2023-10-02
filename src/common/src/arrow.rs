use arrow::array::Decimal128Builder;
use arrow::array::PrimitiveBuilder;
use arrow::datatypes::Decimal128Type;

use crate::DECIMAL_PRECISION;
use crate::DECIMAL_SCALE;

pub struct DecimalBuilder {}

impl DecimalBuilder {
    pub fn with_capacity(capacity: usize) -> PrimitiveBuilder<Decimal128Type> {
        let a = Decimal128Builder::with_capacity(capacity)
            .with_precision_and_scale(DECIMAL_PRECISION, DECIMAL_SCALE)
            .unwrap();
        a
    }
}
