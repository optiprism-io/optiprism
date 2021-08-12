use crate::error::{Result, Error};
use datafusion::scalar::ScalarValue;
use chrono::{Date, Utc};

struct Property {
    id: u64,
    value: ScalarValue,
}

pub struct Event {
    id: u64,
    created_at: Date<Utc>,
    user_id: u64,
    device_id: Option<u64>,
    country_id: Option<u64>,
    user_props: Vec<Property>,
    props: Vec<Property>,
}

trait EventWriter {
    fn write(&mut self, event: &Event) -> Result<()>;
}