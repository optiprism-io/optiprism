use std::sync::Arc;
use std::sync::RwLock;

use async_trait::async_trait;
use bincode::deserialize;
use bincode::serialize;
use chrono::Utc;
use common::types::OptionalProperty;

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
            store: kv,
            guard: RwLock::new(()),
        }
    }
}

impl Provider for ProviderImpl {
    fn create(
        &self,
        organization_id: u64,
        project_id: u64,
        req: CreateReportRequest,
    ) -> Result<Report> {
        let _guard = self.guard.write().unwrap();

        let created_at = Utc::now();
        let id = self.store.next_seq(make_id_seq_key(
            org_proj_ns(organization_id, project_id, NAMESPACE).as_slice(),
        ))?;

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
            typ: req.typ,
            query: req.query,
        };
        let data = serialize(&report)?;
        self.store.put(
            make_data_value_key(
                org_proj_ns(organization_id, project_id, NAMESPACE).as_slice(),
                report.id,
            ),
            &data,
        )?;

        Ok(report)
    }

    fn get_by_id(&self, organization_id: u64, project_id: u64, id: u64) -> Result<Report> {
        let key = make_data_value_key(
            org_proj_ns(organization_id, project_id, NAMESPACE).as_slice(),
            id,
        );

        match self.store.get(key)? {
            None => Err(ReportError::ReportNotFound(error::Report::new_with_id(
                organization_id,
                project_id,
                id,
            ))
            .into()),
            Some(value) => {
                return Ok(deserialize(&value)?);
            }
        }
    }

    fn list(&self, organization_id: u64, project_id: u64) -> Result<ListResponse<Report>> {
        list(
            self.store.clone(),
            org_proj_ns(organization_id, project_id, NAMESPACE).as_slice(),
        )
    }

    fn update(
        &self,
        organization_id: u64,
        project_id: u64,
        report_id: u64,
        req: UpdateReportRequest,
    ) -> Result<Report> {
        let _guard = self.guard.write().unwrap();
        let prev_report = self.get_by_id(organization_id, project_id, report_id)?;
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
        if let OptionalProperty::Some(query) = req.query {
            report.query = query;
        }

        let data = serialize(&report)?;
        self.store.put(
            make_data_value_key(
                org_proj_ns(organization_id, project_id, NAMESPACE).as_slice(),
                report.id,
            ),
            &data,
        )?;

        Ok(report)
    }

    fn delete(&self, organization_id: u64, project_id: u64, id: u64) -> Result<Report> {
        let _guard = self.guard.write().unwrap();
        let report = self.get_by_id(organization_id, project_id, id)?;
        self.store.delete(make_data_value_key(
            org_proj_ns(organization_id, project_id, NAMESPACE).as_slice(),
            id,
        ))?;

        Ok(report)
    }
}
