use chrono::Duration;
use lazy_static::lazy_static;

lazy_static! {
    pub static ref SESSION_DURATION: Duration = Duration::days(1);
}
