use std::sync::Arc;

use async_trait::async_trait;
use bincode::deserialize;
use bincode::serialize;
use chrono::Utc;
use common::types::OptionalProperty;
use tokio::sync::RwLock;

use crate::dashboards::CreateDashboardRequest;
use crate::dashboards::Dashboard;
use crate::dashboards::Provider;
use crate::dashboards::UpdateDashboardRequest;
use crate::error;
use crate::error::DashboardError;
use crate::metadata::ListResponse;
use crate::store::path_helpers::list;
use crate::store::path_helpers::make_data_value_key;
use crate::store::path_helpers::make_id_seq_key;
use crate::store::path_helpers::org_proj_ns;
use crate::store::Store;
use crate::Result;

const NAMESPACE: &[u8] = b"dashboards";

pub struct ProviderImpl {
    store: Arc<Store>,
    guard: RwLock<()>,
}

impl ProviderImpl {
    pub fn new(kv: Arc<Store>) -> Self {
        ProviderImpl {
            store: kv.clone(),
            guard: RwLock::new(()),
        }
    }
}

#[async_trait]
impl Provider for ProviderImpl {
    async fn create(
        &self,
        organization_id: u64,
        project_id: u64,
        req: CreateDashboardRequest,
    ) -> Result<Dashboard> {
        let _guard = self.guard.write().await;

        let created_at = Utc::now();
        let id = self
            .store
            .next_seq(make_id_seq_key(
                org_proj_ns(organization_id, project_id, NAMESPACE).as_slice(),
            ))
            .await?;

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
        self.store
            .put(
                make_data_value_key(
                    org_proj_ns(organization_id, project_id, NAMESPACE).as_slice(),
                    dashboard.id,
                ),
                &data,
            )
            .await?;

        Ok(dashboard)
    }

    async fn get_by_id(&self, organization_id: u64, project_id: u64, id: u64) -> Result<Dashboard> {
        let key = make_data_value_key(
            org_proj_ns(organization_id, project_id, NAMESPACE).as_slice(),
            id,
        );

        match self.store.get(key).await? {
            None => Err(
                DashboardError::DashboardNotFound(error::Dashboard::new_with_id(
                    organization_id,
                    project_id,
                    id,
                ))
                .into(),
            ),
            Some(value) => Ok(deserialize(&value)?),
        }
    }

    async fn list(&self, organization_id: u64, project_id: u64) -> Result<ListResponse<Dashboard>> {
        list(
            self.store.clone(),
            org_proj_ns(organization_id, project_id, NAMESPACE).as_slice(),
        )
        .await
    }

    async fn update(
        &self,
        organization_id: u64,
        project_id: u64,
        dashboard_id: u64,
        req: UpdateDashboardRequest,
    ) -> Result<Dashboard> {
        let _guard = self.guard.write().await;

        let prev_dashboard = self
            .get_by_id(organization_id, project_id, dashboard_id)
            .await?;
        let mut dashboard = prev_dashboard.clone();

        dashboard.updated_at = Some(Utc::now());
        dashboard.updated_by = Some(req.updated_by);
        if let OptionalProperty::Some(tags) = req.tags {
            dashboard.tags = tags;
        }
        if let OptionalProperty::Some(description) = req.description {
            dashboard.description = description;
        }
        if let OptionalProperty::Some(panels) = req.panels {
            dashboard.panels = panels;
        }

        let data = serialize(&dashboard)?;
        self.store
            .put(
                make_data_value_key(
                    org_proj_ns(organization_id, project_id, NAMESPACE).as_slice(),
                    dashboard.id,
                ),
                &data,
            )
            .await?;

        Ok(dashboard)
    }

    async fn delete(&self, organization_id: u64, project_id: u64, id: u64) -> Result<Dashboard> {
        let _guard = self.guard.write().await;
        let dashboard = self.get_by_id(organization_id, project_id, id).await?;
        self.store
            .delete(make_data_value_key(
                org_proj_ns(organization_id, project_id, NAMESPACE).as_slice(),
                id,
            ))
            .await?;

        Ok(dashboard)
    }
}
