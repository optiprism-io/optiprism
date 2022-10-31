use std::sync::Arc;

use bincode::deserialize;
use bincode::serialize;
use chrono::Utc;
use common::types::EventRef;
use common::types::OptionalProperty;
use futures::future::BoxFuture;
use futures::future::FutureExt;
use tokio::sync::RwLock;

use crate::custom_events::types::CreateCustomEventRequest;
use crate::custom_events::types::Event;
use crate::custom_events::types::UpdateCustomEventRequest;
use crate::custom_events::CreateCustomEventRequest;
use crate::custom_events::CustomEvent;
use crate::custom_events::Event;
use crate::custom_events::Provider;
use crate::custom_events::UpdateCustomEventRequest;
use crate::error;
use crate::error::CustomEventError;
use crate::error::MetadataError;
use crate::error::StoreError;
use crate::events;
use crate::metadata::ListResponse;
use crate::store::index::hash_map::HashMap;
use crate::store::path_helpers::list;
use crate::store::path_helpers::make_data_value_key;
use crate::store::path_helpers::make_id_seq_key;
use crate::store::path_helpers::make_index_key;
use crate::store::path_helpers::org_proj_ns;
use crate::store::Store;
use crate::Result;

const NAMESPACE: &[u8] = b"custom_events";
const IDX_NAME: &[u8] = b"name";
const MAX_EVENTS_LEVEL: usize = 3;

fn index_keys(organization_id: u64, project_id: u64, name: &str) -> Vec<Option<Vec<u8>>> {
    [index_name_key(organization_id, project_id, name)].to_vec()
}

fn index_name_key(organization_id: u64, project_id: u64, name: &str) -> Option<Vec<u8>> {
    Some(
        make_index_key(
            org_proj_ns(organization_id, project_id, NAMESPACE).as_slice(),
            IDX_NAME,
            name,
        )
        .to_vec(),
    )
}

pub struct ProviderImpl {
    store: Arc<Store>,
    events: Arc<dyn events::Provider>,
    idx: HashMap,
    guard: RwLock<()>,
    max_events_level: usize,
}

impl ProviderImpl {
    pub fn new(kv: Arc<Store>, events: Arc<dyn events::Provider>) -> Self {
        ProviderImpl {
            store: kv.clone(),
            events,
            idx: HashMap::new(kv),
            guard: RwLock::new(()),
            max_events_level: MAX_EVENTS_LEVEL,
        }
    }

    pub fn with_max_events_level(self, max_events_level: usize) -> Self {
        Self {
            store: self.store,
            events: self.events,
            idx: self.idx,
            guard: self.guard,
            max_events_level,
        }
    }

    fn validate_events<'a>(
        &'a self,
        organization_id: u64,
        project_id: u64,
        events: &'a [Event],
        level: usize,
        ids: &'a mut Vec<u64>,
    ) -> BoxFuture<'a, Result<()>> {
        async move {
            if level > self.max_events_level {
                return Err(CustomEventError::RecursionLevelExceeded(self.max_events_level).into());
            }

            for event in events.iter() {
                match &event.event {
                    EventRef::RegularName(name) => {
                        self.events
                            .get_by_name(organization_id, project_id, name.as_str())
                            .await?;
                    }
                    EventRef::Regular(id) => {
                        self.events
                            .get_by_id(organization_id, project_id, *id)
                            .await?;
                    }
                    EventRef::Custom(id) => {
                        if ids.contains(id) {
                            return Err(CustomEventError::DuplicateEvent.into());
                        }
                        let custom_event = self.get_by_id(organization_id, project_id, *id).await?;
                        ids.push(custom_event.id);
                        self.validate_events(
                            organization_id,
                            project_id,
                            &custom_event.events,
                            level + 1,
                            ids,
                        )
                        .await?;
                    }
                }
            }

            Ok(())
        }
        .boxed()
    }
}
impl Provider for ProviderImpl {
    async fn create(
        &self,
        organization_id: u64,
        project_id: u64,
        req: CreateCustomEventRequest,
    ) -> Result<CustomEvent> {
        let _guard = self.guard.write().await;

        if req.events.is_empty() {
            return Err(CustomEventError::EmptyEvents.into());
        }

        let mut ids = Vec::new();
        self.validate_events(organization_id, project_id, &req.events, 0, &mut ids)
            .await?;

        let idx_keys = index_keys(organization_id, project_id, &req.name);

        match self.idx.check_insert_constraints(idx_keys.as_ref()).await {
            Err(MetadataError::Store(StoreError::KeyAlreadyExists(_))) => {
                return Err(CustomEventError::EventAlreadyExist(
                    error::CustomEvent::new_with_name(organization_id, project_id, req.name),
                )
                .into());
            }
            Err(other) => return Err(other),
            Ok(_) => {}
        }

        let created_at = Utc::now();
        let id = self
            .store
            .next_seq(make_id_seq_key(
                org_proj_ns(organization_id, project_id, NAMESPACE).as_slice(),
            ))
            .await?;

        let event = CustomEvent {
            id,
            created_at,
            updated_at: None,
            created_by: req.created_by,
            updated_by: None,
            project_id,
            tags: req.tags,
            name: req.name,
            description: req.description,
            status: req.status,
            is_system: req.is_system,
            events: req.events,
        };
        let data = serialize(&event)?;
        self.store
            .put(
                make_data_value_key(
                    org_proj_ns(organization_id, project_id, NAMESPACE).as_slice(),
                    event.id,
                ),
                &data,
            )
            .await?;

        self.idx.insert(idx_keys.as_ref(), &data).await?;

        Ok(event)
    }

    async fn get_by_id(
        &self,
        organization_id: u64,
        project_id: u64,
        id: u64,
    ) -> Result<CustomEvent> {
        let key = make_data_value_key(
            org_proj_ns(organization_id, project_id, NAMESPACE).as_slice(),
            id,
        );

        match self.store.get(key).await? {
            None => Err(
                CustomEventError::EventNotFound(error::CustomEvent::new_with_id(
                    organization_id,
                    project_id,
                    id,
                ))
                .into(),
            ),
            Some(value) => Ok(deserialize(&value)?),
        }
    }

    async fn get_by_name(
        &self,
        organization_id: u64,
        project_id: u64,
        name: &str,
    ) -> Result<CustomEvent> {
        match self
            .idx
            .get(make_index_key(
                org_proj_ns(organization_id, project_id, NAMESPACE).as_slice(),
                IDX_NAME,
                name,
            ))
            .await
        {
            Err(MetadataError::Store(StoreError::KeyNotFound(name))) => Err(
                CustomEventError::EventNotFound(error::CustomEvent::new_with_name(
                    organization_id,
                    project_id,
                    name,
                ))
                .into(),
            ),
            Err(other) => Err(other),
            Ok(data) => Ok(deserialize(&data)?),
        }
    }

    async fn list(
        &self,
        organization_id: u64,
        project_id: u64,
    ) -> Result<ListResponse<CustomEvent>> {
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
        event_id: u64,
        req: UpdateCustomEventRequest,
    ) -> Result<CustomEvent> {
        let _guard = self.guard.write().await;

        let prev_event = self
            .get_by_id(organization_id, project_id, event_id)
            .await?;
        let mut event = prev_event.clone();

        let mut idx_keys: Vec<Option<Vec<u8>>> = Vec::new();
        let mut idx_prev_keys: Vec<Option<Vec<u8>>> = Vec::new();
        if let OptionalProperty::Some(name) = &req.name {
            idx_keys.push(index_name_key(organization_id, project_id, name.as_str()));
            idx_prev_keys.push(index_name_key(
                organization_id,
                project_id,
                prev_event.name.as_str(),
            ));
            event.name = name.to_owned();
        }

        match self
            .idx
            .check_update_constraints(idx_keys.as_ref(), idx_prev_keys.as_ref())
            .await
        {
            Err(MetadataError::Store(StoreError::KeyAlreadyExists(_))) => {
                return Err(
                    CustomEventError::EventAlreadyExist(error::CustomEvent::new_with_id(
                        organization_id,
                        project_id,
                        event_id,
                    ))
                    .into(),
                );
            }
            Err(other) => return Err(other),
            Ok(_) => {}
        }

        event.updated_at = Some(Utc::now());
        event.updated_by = Some(req.updated_by);
        if let OptionalProperty::Some(tags) = req.tags {
            event.tags = tags;
        }
        if let OptionalProperty::Some(description) = req.description {
            event.description = description;
        }
        if let OptionalProperty::Some(status) = req.status {
            event.status = status;
        }
        if let OptionalProperty::Some(is_system) = req.is_system {
            event.is_system = is_system;
        }

        if let OptionalProperty::Some(events) = req.events {
            if events.is_empty() {
                return Err(CustomEventError::EmptyEvents.into());
            }

            let mut ids = vec![event.id];
            self.validate_events(organization_id, project_id, &events, 0, &mut ids)
                .await?;

            event.events = events;
        }

        let data = serialize(&event)?;
        self.store
            .put(
                make_data_value_key(
                    org_proj_ns(organization_id, project_id, NAMESPACE).as_slice(),
                    event.id,
                ),
                &data,
            )
            .await?;

        self.idx
            .update(idx_keys.as_ref(), idx_prev_keys.as_ref(), &data)
            .await?;
        Ok(event)
    }

    async fn delete(&self, organization_id: u64, project_id: u64, id: u64) -> Result<CustomEvent> {
        let _guard = self.guard.write().await;
        let event = self.get_by_id(organization_id, project_id, id).await?;
        self.store
            .delete(make_data_value_key(
                org_proj_ns(organization_id, project_id, NAMESPACE).as_slice(),
                id,
            ))
            .await?;

        self.idx
            .delete(index_keys(organization_id, project_id, &event.name).as_ref())
            .await?;

        Ok(event)
    }
}
