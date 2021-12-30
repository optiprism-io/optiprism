use crate::exprtree::error::{Error, Result};
use crate::ifaces::event;
use crate::ifaces::user;
use chrono::{Date, Utc};
use datafusion::scalar::ScalarValue;

struct Property {
    name: String,
    value: ScalarValue,
}

struct Event {
    event_name: String,
    created_at: Option<Date<Utc>>,
    user_id: String,
    device: Option<String>,
    country: Option<String>,
    user_props: Vec<Property>,
    event_props: Vec<Property>,
}

struct User {
    id: String,
    created_at: Date<Utc>,
    device: Option<String>,
    country: Option<String>,
}

trait Resolver {
    fn resolve_event(&self, event: &Event) -> Result<event::Event>;
    fn resolve_user(&self, user: &User) -> Result<user::User>;
}
