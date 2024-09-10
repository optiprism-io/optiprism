pub mod arrow;
pub mod config;
pub mod error;
pub mod event_segmentation;
pub mod funnel;
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
pub const ADMIN_ID: u64 = 1;
pub fn group_col(group_id: usize) -> String {
    format!("group_{group_id}")
}

pub const DATA_PATH_METADATA: &str = "md/data";
pub const DATA_PATH_STORAGE: &str = "data";
pub const DATA_PATH_BACKUP_TMP: &str = "backup_tmp";
pub const DATA_PATH_BACKUPS: &str = "backups";
pub const DATA_PATH_RECOVERS: &str = "recovers";
