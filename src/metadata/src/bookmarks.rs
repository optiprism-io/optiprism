use std::sync::Arc;

use bincode::deserialize;
use bincode::serialize;
use chrono::DateTime;
use chrono::Utc;
use rand::distributions::Alphanumeric;
use rand::Rng;
use rocksdb::Transaction;
use rocksdb::TransactionDB;
use serde::Deserialize;
use serde::Serialize;
use common::event_segmentation::EventSegmentationRequest;
use common::funnel::Funnel;

use crate::error::MetadataError;
use crate::project_ns;
use crate::reports::Query;
use crate::Result;

const NAMESPACE: &str = "bookmarks";

pub struct Bookmarks {
    db: Arc<TransactionDB>,
}

impl Bookmarks {
    pub fn new(db: Arc<TransactionDB>) -> Self {
        Bookmarks { db }
    }

    fn get_by_id_(
        &self,
        tx: &Transaction<TransactionDB>,
        project_id: u64,
        account_id: u64,
        id: &str,
    ) -> Result<Bookmark> {
        let key = format!("projects/{project_id}/{NAMESPACE}/accounts/{account_id}/{id}");
        match tx.get(key)? {
            None => Err(MetadataError::NotFound(
                format!("bookmark {id} not found").to_string(),
            )),
            Some(value) => Ok(deserialize(&value)?),
        }
    }

    pub fn create(&self, project_id: u64, req: CreateBookmarkRequest) -> Result<Bookmark> {
        let tx = self.db.transaction();

        let id: String = rand::thread_rng()
            .sample_iter(&Alphanumeric)
            .take(7)
            .map(char::from)
            .collect();

        let created_at = Utc::now();

        let bookmark = Bookmark {
            id:id.clone(),
            created_at,
            created_by: req.created_by,
            project_id,
            query: req.query,
        };
        let data = serialize(&bookmark)?;
        let key = format!("projects/{project_id}/{NAMESPACE}/accounts/{}/{id}", req.created_by);
        tx.put(
            key,
            data,
        )?;
        tx.commit()?;
        Ok(bookmark)
    }

    pub fn get_by_id(&self, project_id: u64, account_id: u64, id: &str) -> Result<Bookmark> {
        let tx = self.db.transaction();

        self.get_by_id_(&tx, project_id, account_id, id)
    }

    pub fn delete(&self, project_id: u64, account_id: u64, id: &str) -> Result<Bookmark> {
        let tx = self.db.transaction();
        let bookmark = self.get_by_id_(&tx, project_id,account_id, id)?;
        let key = format!("projects/{project_id}/{NAMESPACE}/accounts/{account_id}/{id}");
        tx.delete(key)?;
        tx.commit()?;
        Ok(bookmark)
    }
}


#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct Bookmark {
    pub id: String,
    pub created_at: DateTime<Utc>,
    pub created_by: u64,
    pub project_id: u64,
    pub query: Option<Query>,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct CreateBookmarkRequest {
    pub created_by: u64,
    pub query: Option<Query>,
}