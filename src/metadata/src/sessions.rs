use std::str::from_utf8;
use std::str::pattern::Pattern;
use std::sync::Arc;

use chrono::DateTime;
use chrono::Utc;
use prost::Message;
use rocksdb::TransactionDB;
use serde::Deserialize;
use serde::Serialize;

use crate::error::MetadataError;
use crate::session;
use crate::project_ns;
use crate::Result;

const NAMESPACE: &[u8] = b"sessions";

fn make_data_key(project_id: u64, user_id: u64) -> Vec<u8> {
    [
        project_ns(project_id, NAMESPACE).as_ref(),
        b"/data".as_slice(),
        user_id.to_string().as_bytes(),
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

    pub fn get_by_user_id(&self, project_id: u64, user_id: u64) -> Result<Session> {
        let tx = self.db.transaction();
        let key = make_data_key(project_id, user_id);
        match tx.get(key)? {
            None => Err(MetadataError::NotFound(
                format!("account {user_id} not found").to_string(),
            )),
            Some(value) => Ok(deserialize(&value)?),
        }
    }

    pub fn clear_project(&self, project_id: u64) -> Result<()> {
        let tx = self.db.transaction();

        let prefix = crate::make_data_key(project_ns(project_id, NAMESPACE).as_slice());
        let iter = tx.prefix_iterator(prefix.clone());
        let mut list = vec![];
        for kv in iter {
            let (key, value) = kv?;
            // check if key contains the prefix
            if !from_utf8(&prefix).unwrap().is_prefix_of(from_utf8(&key).unwrap()) {
                break;
            }
            list.push(deserialize(&value)?);
        }

        let tx = self.db.transaction();
        for s in list.into_iter() {
            tx.delete(make_data_key(project_id, s.user_id).as_slice())?;
        }
        tx.commit()?;

        Ok(())
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

        let prefix = format!("projects/{project_id}/sessions/data");
        let iter = tx.prefix_iterator(prefix.clone());
        let mut list = vec![];
        for kv in iter {
            let (key, value) = kv?;
            // check if key contains the prefix
            if !prefix.is_prefix_of(from_utf8(&key).unwrap()) {
                break;
            }
            list.push(deserialize(&value)?);
        }

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

// serialize session to protobuf
fn serialize(session: &Session) -> Result<Vec<u8>> {
    let v = session::Session {
        user_id: session.user_id,
        created_at: session.created_at.timestamp(),
    };

    Ok(v.encode_to_vec())
}

// deserialize protobuf to session
fn deserialize(data: &[u8]) -> Result<Session> {
    let v = session::Session::decode(data)?;
    Ok(Session {
        user_id: v.user_id,
        created_at: chrono::DateTime::from_timestamp(v.created_at, 0).unwrap(),
    })
}

#[cfg(test)]
mod tests {
    use chrono::DateTime;
    use crate::sessions::{deserialize, serialize};

    #[test]
    fn test_roundtrip() {
        let session = super::Session {
            user_id: 1,
            created_at: DateTime::from_timestamp(1, 0).unwrap(),
        };

        let data = serialize(&session).unwrap();
        let session2 = deserialize(&data).unwrap();

        assert_eq!(session, session2);
    }
}