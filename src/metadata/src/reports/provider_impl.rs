use std::sync::Arc;

use bincode::deserialize;
use bincode::serialize;
use chrono::Utc;
use common::types::OptionalProperty;
use rocksdb::Transaction;
use rocksdb::TransactionDB;

use crate::error::MetadataError;
use crate::index::next_seq;
use crate::metadata::ListResponse;
use crate::reports::CreateReportRequest;
use crate::reports::Provider;
use crate::reports::Report;
use crate::reports::UpdateReportRequest;
use crate::store::path_helpers::list;
use crate::store::path_helpers::make_data_value_key;
use crate::store::path_helpers::make_id_seq_key;
use crate::store::path_helpers::org_proj_ns;
use crate::Result;

const NAMESPACE: &[u8] = b"reports";

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
    ) -> Result<Report> {
        let key = make_data_value_key(
            org_proj_ns(organization_id, project_id, NAMESPACE).as_slice(),
            id,
        );

        match tx.get(key)? {
            None => Err(MetadataError::NotFound("report not found".to_string())),
            Some(value) => Ok(deserialize(&value)?),
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
        let tx = self.db.transaction();

        let created_at = Utc::now();
        let id = next_seq(
            &tx,
            make_id_seq_key(org_proj_ns(organization_id, project_id, NAMESPACE).as_slice()),
        )?;

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
        tx.put(
            make_data_value_key(
                org_proj_ns(organization_id, project_id, NAMESPACE).as_slice(),
                report.id,
            ),
            data,
        )?;
        tx.commit()?;
        Ok(report)
    }

    fn get_by_id(&self, organization_id: u64, project_id: u64, id: u64) -> Result<Report> {
        let tx = self.db.transaction();

        self._get_by_id(&tx, organization_id, project_id, id)
    }

    fn list(&self, organization_id: u64, project_id: u64) -> Result<ListResponse<Report>> {
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
        report_id: u64,
        req: UpdateReportRequest,
    ) -> Result<Report> {
        let tx = self.db.transaction();
        let prev_report = self._get_by_id(&tx, organization_id, project_id, report_id)?;
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
        tx.put(
            make_data_value_key(
                org_proj_ns(organization_id, project_id, NAMESPACE).as_slice(),
                report.id,
            ),
            data,
        )?;
        tx.commit()?;
        Ok(report)
    }

    fn delete(&self, organization_id: u64, project_id: u64, id: u64) -> Result<Report> {
        let tx = self.db.transaction();
        let report = self._get_by_id(&tx, organization_id, project_id, id)?;
        tx.delete(make_data_value_key(
            org_proj_ns(organization_id, project_id, NAMESPACE).as_slice(),
            id,
        ))?;
        tx.commit()?;
        Ok(report)
    }
}
