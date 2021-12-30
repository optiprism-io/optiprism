use crate::exprtree::error::{Error, Result};
use chrono::{Date, Utc};
use datafusion::scalar::ScalarValue;

struct Property {
    id: u64,
    value: ScalarValue,
}

pub struct User {
    id: u64,
    created_at: Date<Utc>,
    device_id: Option<u64>,
    country_id: Option<u64>,
    props: Vec<Property>,
}

trait UserProvider {
    fn u(&mut self, id: String) -> Result<u64>;
    fn map_from_internal_id(&mut self, id: u64) -> Result<String>;
    fn get_user_by_id(&mut self, id: u64) -> Result<User>;
    fn create_user(&mut self, user: &User) -> Result<User>;
    fn update_user(&mut self, user: &User) -> Result<User>;
    fn delete_user(&mut self, id: u64) -> Result<()>;
    fn get_value(
        &mut self,
        user_id: u64,
        property_id: u64,
        value: ScalarValue,
    ) -> Result<ScalarValue>;
    fn update_value(&mut self, user_id: u64, property_id: u64, value: ScalarValue) -> Result<()>;
    fn append_value_to_list(
        &mut self,
        user_id: u64,
        property_id: u64,
        value: ScalarValue,
    ) -> Result<()>;
    fn remove_value_to_list(
        &mut self,
        user_id: u64,
        property_id: u64,
        value: ScalarValue,
    ) -> Result<()>;
    fn increment_value(&mut self, user_id: u64, property_id: u64, delta: ScalarValue)
        -> Result<()>;
}
