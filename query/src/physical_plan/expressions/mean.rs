use datafusion::physical_plan::Accumulator;
use datafusion::scalar::ScalarValue;
use datafusion::error::Result;
/// A UDAF has state across multiple rows, and thus we require a `struct` with that state.
#[derive(Debug,Clone,Copy)]
pub struct GeometricMean {
    n: u32,
    prod: f64,
}

impl GeometricMean {
    // how the struct is initialized
    pub fn new() -> Self {
        GeometricMean { n: 0, prod: 1.0 }
    }
}

// UDAFs are built using the trait `Accumulator`, that offers DataFusion the necessary functions
// to use them.
impl Accumulator for GeometricMean {
    // this function serializes our state to `ScalarValue`, which DataFusion uses
    // to pass this state between execution stages.
    // Note that this can be arbitrary data.
    fn state(&self) -> Result<Vec<ScalarValue>> {
        Ok(vec![
            ScalarValue::from(self.prod),
            ScalarValue::from(self.n),
        ])
    }

    // this function receives one entry per argument of this accumulator.
    // DataFusion calls this function on every row, and expects this function to update the accumulator's state.
    fn update(&mut self, values: &[ScalarValue]) -> Result<()> {
        // this is a one-argument UDAF, and thus we use `0`.
        let value = &values[0];
        match value {
            // here we map `ScalarValue` to our internal state. `Float64` indicates that this function
            // only accepts Float64 as its argument (DataFusion does try to coerce arguments to this type)
            //
            // Note that `.map` here ensures that we ignore Nulls.
            ScalarValue::Float64(e) => e.map(|value| {
                self.prod *= value;
                self.n += 1;
            }),
            _ => unreachable!(""),
        };
        Ok(())
    }

    // this function receives states from other accumulators (Vec<ScalarValue>)
    // and updates the accumulator.
    fn merge(&mut self, states: &[ScalarValue]) -> Result<()> {
        let prod = &states[0];
        let n = &states[1];
        match (prod, n) {
            (ScalarValue::Float64(Some(prod)), ScalarValue::UInt32(Some(n))) => {
                self.prod *= prod;
                self.n += n;
            }
            _ => unreachable!(""),
        };
        Ok(())
    }

    // DataFusion expects this function to return the final value of this aggregator.
    // in this case, this is the formula of the geometric mean
    fn evaluate(&self) -> Result<ScalarValue> {
        let value = self.prod.powf(1.0 / self.n as f64);
        Ok(ScalarValue::from(value))
    }

    // Optimization hint: this trait also supports `update_batch` and `merge_batch`,
    // that can be used to perform these operations on arrays instead of single values.
    // By default, these methods call `update` and `merge` row by row
}
