use std::sync::Arc;

use bincode::deserialize;
use bincode::serialize;
use chrono::DateTime;
use chrono::Utc;
use datafusion::physical_plan::DisplayFormatType::Default;
use rocksdb::TransactionDB;
use serde::Deserialize;
use serde::Serialize;

use crate::accounts::Accounts;
use crate::error::MetadataError;
use crate::make_data_value_key;
use crate::org_proj_ns;
use crate::Result;

const NAMESPACE: &[u8] = b"sessions";

fn make_key(organization_id: u64, project_id: u64, account_id: u64) -> Vec<u8> {
    org_proj_ns(
        organization_id,
        project_id,
        account_id.to_le_bytes().as_slice(),
    )
}

pub struct Sessions {
    db: Arc<TransactionDB>,
}

impl Sessions {
    pub fn new(db: Arc<TransactionDB>) -> Self {
        Self { db }
    }

    pub fn get_by_id(
        &self,
        organization_id: u64,
        project_id: u64,
        account_id: u64,
    ) -> Result<Session> {
        let tx = self.db.transaction();
        let key = make_key(organization_id, project_id, account_id);
        match tx.get(key)? {
            None => Err(MetadataError::NotFound("account not found".to_string())),
            Some(value) => Ok(deserialize(&value)?),
        }
    }

    // returns true if session is new
    pub fn set_current_time(
        &self,
        organization_id: u64,
        project_id: u64,
        account_id: u64,
        time: DateTime<Utc>,
    ) -> Result<bool> {
        let tx = self.db.transaction();
        let key = make_key(organization_id, project_id, account_id);
        let mut is_new = false;
        let mut session = match tx.get(key)? {
            None => {
                is_new = true;
                Session {
                    created_at: Utc::now(),
                }
            }
            Some(value) => deserialize(&value)?,
        };

        session.created_at = time;

        let data = serialize(&session)?;
        tx.put(make_key(organization_id, project_id, account_id), &data)?;
        tx.commit()?;

        Ok(is_new)
    }
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq, Default)]
struct Session {
    created_at: DateTime<Utc>,
}
