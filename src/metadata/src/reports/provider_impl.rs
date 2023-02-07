use std::sync::Arc;

use async_trait::async_trait;
use bincode::deserialize;
use bincode::serialize;
use chrono::Utc;
use common::types::OptionalProperty;
use tokio::sync::RwLock;

use crate::error;
use crate::error::ReportError;
use crate::metadata::ListResponse;
use crate::reports::CreateReportRequest;
use crate::reports::Provider;
use crate::reports::Report;
use crate::reports::UpdateReportRequest;
use crate::store::path_helpers::list;
use crate::store::path_helpers::make_data_value_key;
use crate::store::path_helpers::make_id_seq_key;
use crate::store::path_helpers::org_proj_ns;
use crate::store::Store;
use crate::Result;

const NAMESPACE: &[u8] = b"reports";

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
        req: CreateReportRequest,
    ) -> Result<Report> {
        let _guard = self.guard.write().await;

        let created_at = Utc::now();
        let id = self
            .store
            .next_seq(make_id_seq_key(
                org_proj_ns(organization_id, project_id, NAMESPACE).as_slice(),
            ))
            .await?;

        let report = Report {
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
        let data = serialize(&report)?;
        self.store
            .put(
                make_data_value_key(
                    org_proj_ns(organization_id, project_id, NAMESPACE).as_slice(),
                    report.id,
                ),
                &data,
            )
            .await?;

        Ok(report)
    }

    async fn get_by_id(&self, organization_id: u64, project_id: u64, id: u64) -> Result<Report> {
        let key = make_data_value_key(
            org_proj_ns(organization_id, project_id, NAMESPACE).as_slice(),
            id,
        );

        match self.store.get(key).await? {
            None => Err(ReportError::ReportNotFound(error::Report::new_with_id(
                organization_id,
                project_id,
                id,
            ))
            .into()),
            Some(value) => Ok(deserialize(&value)?),
        }
    }

    async fn list(&self, organization_id: u64, project_id: u64) -> Result<ListResponse<Report>> {
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
        report_id: u64,
        req: UpdateReportRequest,
    ) -> Result<Report> {
        let _guard = self.guard.write().await;

        let prev_report = self
            .get_by_id(organization_id, project_id, report_id)
            .await?;
        let mut report = prev_report.clone();

        report.updated_at = Some(Utc::now());
        report.updated_by = Some(req.updated_by);
        if let OptionalProperty::Some(name) = req.name {
            report.name = name;
        }
        if let OptionalProperty::Some(description) = req.description {
            report.description = description;
        }
        if let OptionalProperty::Some(tags) = req.tags {
            report.tags = tags;
        }
        if let OptionalProperty::Some(panels) = req.panels {
            report.panels = panels;
        }

        let data = serialize(&report)?;
        self.store
            .put(
                make_data_value_key(
                    org_proj_ns(organization_id, project_id, NAMESPACE).as_slice(),
                    report.id,
                ),
                &data,
            )
            .await?;

        Ok(report)
    }

    async fn delete(&self, organization_id: u64, project_id: u64, id: u64) -> Result<Report> {
        let _guard = self.guard.write().await;
        let report = self.get_by_id(organization_id, project_id, id).await?;
        self.store
            .delete(make_data_value_key(
                org_proj_ns(organization_id, project_id, NAMESPACE).as_slice(),
                id,
            ))
            .await?;

        Ok(report)
    }
}
