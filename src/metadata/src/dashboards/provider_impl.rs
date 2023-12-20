use std::sync::Arc;
use std::sync::RwLock;

use async_trait::async_trait;
use bincode::deserialize;
use bincode::serialize;
use chrono::Utc;
use common::types::OptionalProperty;
use rocksdb::Transaction;
use rocksdb::TransactionDB;

use crate::dashboards::CreateDashboardRequest;
use crate::dashboards::Dashboard;
use crate::dashboards::Provider;
use crate::dashboards::UpdateDashboardRequest;
use crate::error;
use crate::error::MetadataError;
use crate::index::next_seq;
use crate::metadata::ListResponse;
use crate::store::path_helpers::list;
use crate::store::path_helpers::make_data_value_key;
use crate::store::path_helpers::make_id_seq_key;
use crate::store::path_helpers::org_proj_ns;
use crate::Result;

const NAMESPACE: &[u8] = b"dashboards";

pub struct ProviderImpl {
    db: Arc<TransactionDB>,
}

impl ProviderImpl {
    pub fn new(db: Arc<TransactionDB>) -> Self {
        ProviderImpl { db }
    }

    fn _get_by_id(
        &self,
        tx: &Transaction<TransactionDB>,
        organization_id: u64,
        project_id: u64,
        id: u64,
    ) -> Result<Dashboard> {
        let key = make_data_value_key(
            org_proj_ns(organization_id, project_id, NAMESPACE).as_slice(),
            id,
        );

        match tx.get(key)? {
            None => Err(MetadataError::NotFound(
                "dashboard not found".to_string(),
            )),
            Some(value) => Ok(deserialize(&value)?),
        }
    }
}

impl Provider for ProviderImpl {
    fn create(
        &self,
        organization_id: u64,
        project_id: u64,
        req: CreateDashboardRequest,
    ) -> Result<Dashboard> {
        let tx = self.db.transaction();
        let created_at = Utc::now();
        let id = next_seq(
            &tx,
            make_id_seq_key(org_proj_ns(organization_id, project_id, NAMESPACE).as_slice()),
        )?;

        let dashboard = Dashboard {
            id,
            created_at,
            updated_at: None,
            created_by: req.created_by,
            updated_by: None,
            project_id,
            tags: req.tags,
            name: req.name,
            description: req.description,
            panels: req.panels,
        };
        let data = serialize(&dashboard)?;
        tx.put(
            make_data_value_key(
                org_proj_ns(organization_id, project_id, NAMESPACE).as_slice(),
                dashboard.id,
            ),
            &data,
        )?;

        tx.commit()?;
        Ok(dashboard)
    }

    fn get_by_id(&self, organization_id: u64, project_id: u64, id: u64) -> Result<Dashboard> {
        let tx = self.db.transaction();

        self._get_by_id(&tx, organization_id, project_id, id)
    }

    fn list(&self, organization_id: u64, project_id: u64) -> Result<ListResponse<Dashboard>> {
        let tx = self.db.transaction();
        list(
            &tx,
            org_proj_ns(organization_id, project_id, NAMESPACE).as_slice(),
        )
    }

    fn update(
        &self,
        organization_id: u64,
        project_id: u64,
        dashboard_id: u64,
        req: UpdateDashboardRequest,
    ) -> Result<Dashboard> {
        let tx = self.db.transaction();

        let prev_dashboard = self._get_by_id(&tx, organization_id, project_id, dashboard_id)?;
        let mut dashboard = prev_dashboard.clone();

        dashboard.updated_at = Some(Utc::now());
        dashboard.updated_by = Some(req.updated_by);
        if let OptionalProperty::Some(name) = req.name {
            dashboard.name = name;
        }
        if let OptionalProperty::Some(description) = req.description {
            dashboard.description = description;
        }
        if let OptionalProperty::Some(tags) = req.tags {
            dashboard.tags = tags;
        }
        if let OptionalProperty::Some(panels) = req.panels {
            dashboard.panels = panels;
        }

        let data = serialize(&dashboard)?;
        tx.put(
            make_data_value_key(
                org_proj_ns(organization_id, project_id, NAMESPACE).as_slice(),
                dashboard.id,
            ),
            &data,
        )?;
        tx.commit()?;
        Ok(dashboard)
    }

    fn delete(&self, organization_id: u64, project_id: u64, id: u64) -> Result<Dashboard> {
        let tx = self.db.transaction();
        let dashboard = self._get_by_id(&tx, organization_id, project_id, id)?;
        tx.delete(make_data_value_key(
            org_proj_ns(organization_id, project_id, NAMESPACE).as_slice(),
            id,
        ))?;
        tx.commit()?;
        Ok(dashboard)
    }
}
