use std::sync::Arc;

use bincode::deserialize;
use bincode::serialize;
use chrono::DateTime;
use chrono::Utc;
use rocksdb::TransactionDB;
use serde::Deserialize;
use serde::Serialize;

use crate::error::MetadataError;
use crate::list;
use crate::metadata::ListResponse;
use crate::project_ns;
use crate::Result;

const NAMESPACE: &[u8] = b"sessions";

fn make_data_key(project_id: u64, user_id: u64) -> Vec<u8> {
    [
        project_ns(project_id, NAMESPACE).as_ref(),
        b"/".as_slice(),
        user_id.to_le_bytes().as_ref(),
    ]
    .concat()
}

pub struct Sessions {
    db: Arc<TransactionDB>,
}

impl Sessions {
    pub fn new(db: Arc<TransactionDB>) -> Self {
        Self { db }
    }

    pub fn get_by_id(&self, project_id: u64, user_id: u64) -> Result<Session> {
        let tx = self.db.transaction();
        let key = make_data_key(project_id, user_id);
        match tx.get(key)? {
            None => Err(MetadataError::NotFound("account not found".to_string())),
            Some(value) => Ok(deserialize(&value)?),
        }
    }

    // returns true if session is new
    pub fn set_current_time(
        &self,

        project_id: u64,
        user_id: u64,
        time: DateTime<Utc>,
    ) -> Result<bool> {
        let tx = self.db.transaction();
        let key = make_data_key(project_id, user_id);
        let mut is_new = false;
        let mut session = match tx.get(key)? {
            None => {
                is_new = true;
                Session {
                    user_id,
                    created_at: Utc::now(),
                }
            }
            Some(value) => deserialize(&value)?,
        };

        session.created_at = time;

        let data = serialize(&session)?;
        tx.put(make_data_key(project_id, user_id), data)?;
        tx.commit()?;

        Ok(is_new)
    }

    pub fn check_for_deletion(
        &self,

        project_id: u64,
        callback: impl Fn(&Session) -> Result<bool>,
    ) -> Result<()> {
        let tx = self.db.transaction();
        let list: ListResponse<Session> = list(&tx, project_ns(project_id, NAMESPACE).as_ref())?;
        let tx = self.db.transaction();
        for s in list.into_iter() {
            if callback(&s)? {
                tx.delete(make_data_key(project_id, s.user_id).as_slice())?;
            }
        }
        tx.commit()?;

        Ok(())
    }
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq, Default)]
pub struct Session {
    pub user_id: u64,
    pub created_at: DateTime<Utc>,
}
