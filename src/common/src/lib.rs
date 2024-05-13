pub mod arrow;
pub mod config;
pub mod error;
pub mod http;
pub mod query;
pub mod rbac;
pub mod scalar;
pub mod types;

pub use types::DECIMAL_MULTIPLIER;
pub use types::DECIMAL_PRECISION;
pub use types::DECIMAL_SCALE;

pub const GROUPS_COUNT: usize = 5;
pub const GROUP_USER_ID: usize = 0;
pub const GROUP_USER: &str = "user";

pub fn group_col(group_id: usize) -> String {
    format!("group_{group_id}")
}
