use std::sync::Arc;

use async_trait::async_trait;
use bincode::deserialize;
use bincode::serialize;
use chrono::Utc;
use common::types::OptionalProperty;
use tokio::sync::RwLock;

use crate::error;
use crate::error::EventError;
use crate::error::MetadataError;
use crate::error::StoreError;
use crate::events::types::CreateEventRequest;
use crate::events::types::UpdateEventRequest;
use crate::events::CreateEventRequest;
use crate::events::Event;
use crate::events::Provider;
use crate::events::UpdateEventRequest;
use crate::metadata::ListResponse;
use crate::properties::provider_impl::Namespace;
use crate::store::index::hash_map::HashMap;
use crate::store::path_helpers::list;
use crate::store::path_helpers::make_data_value_key;
use crate::store::path_helpers::make_id_seq_key;
use crate::store::path_helpers::make_index_key;
use crate::store::path_helpers::org_proj_ns;
use crate::store::Store;
use crate::Result;
const NAMESPACE: &[u8] = b"events";
const IDX_NAME: &[u8] = b"name";
const IDX_DISPLAY_NAME: &[u8] = b"display_name";

fn index_keys(
    organization_id: u64,
    project_id: u64,
    name: &str,
    display_name: Option<String>,
) -> Vec<Option<Vec<u8>>> {
    [
        index_name_key(organization_id, project_id, name),
        index_display_name_key(organization_id, project_id, display_name),
    ]
    .to_vec()
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

fn index_display_name_key(
    organization_id: u64,
    project_id: u64,
    display_name: Option<String>,
) -> Option<Vec<u8>> {
    display_name.map(|v| {
        make_index_key(
            org_proj_ns(organization_id, project_id, NAMESPACE).as_slice(),
            IDX_DISPLAY_NAME,
            v.as_str(),
        )
        .to_vec()
    })
}

pub struct ProviderImpl {
    store: Arc<Store>,
    idx: HashMap,
    guard: RwLock<()>,
}

impl ProviderImpl {
    pub fn new(kv: Arc<Store>) -> Self {
        ProviderImpl {
            store: kv.clone(),
            idx: HashMap::new(kv),
            guard: RwLock::new(()),
        }
    }

    async fn _create(
        &self,
        organization_id: u64,
        project_id: u64,
        req: CreateEventRequest,
    ) -> Result<Event> {
        let idx_keys = index_keys(
            organization_id,
            project_id,
            &req.name,
            req.display_name.clone(),
        );

        match self.idx.check_insert_constraints(idx_keys.as_ref()).await {
            Err(MetadataError::Store(StoreError::KeyAlreadyExists(_))) => {
                return Err(EventError::EventAlreadyExist(error::Event::new_with_name(
                    organization_id,
                    project_id,
                    req.name,
                ))
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

        let event = Event {
            id,
            created_at,
            updated_at: None,
            created_by: req.created_by,
            updated_by: None,
            project_id,
            tags: req.tags,
            name: req.name,
            display_name: req.display_name,
            description: req.description,
            status: req.status,
            is_system: req.is_system,
            properties: req.properties,
            custom_properties: req.custom_properties,
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

    async fn _get_by_name(
        &self,
        organization_id: u64,
        project_id: u64,
        name: &str,
    ) -> Result<Event> {
        match self
            .idx
            .get(make_index_key(
                org_proj_ns(organization_id, project_id, NAMESPACE).as_slice(),
                IDX_NAME,
                name,
            ))
            .await
        {
            Err(MetadataError::Store(StoreError::KeyNotFound(_))) => {
                Err(EventError::EventNotFound(error::Event::new_with_name(
                    organization_id,
                    project_id,
                    name.to_string(),
                ))
                .into())
            }
            Err(other) => Err(other),
            Ok(data) => Ok(deserialize(&data)?),
        }
    }
}

#[async_trait]
impl Provider for ProviderImpl {
    async fn create(
        &self,
        organization_id: u64,
        project_id: u64,
        req: CreateEventRequest,
    ) -> Result<Event> {
        let _guard = self.guard.write().await;
        self._create(organization_id, project_id, req).await
    }

    async fn get_or_create(
        &self,
        organization_id: u64,
        project_id: u64,
        req: CreateEventRequest,
    ) -> Result<Event> {
        let _guard = self.guard.write().await;
        match self
            ._get_by_name(organization_id, project_id, req.name.as_str())
            .await
        {
            Ok(event) => return Ok(event),
            Err(MetadataError::Event(EventError::EventNotFound(_))) => {}
            other => return other,
        }

        self._create(organization_id, project_id, req).await
    }

    async fn get_by_id(&self, organization_id: u64, project_id: u64, id: u64) -> Result<Event> {
        let key = make_data_value_key(
            org_proj_ns(organization_id, project_id, NAMESPACE).as_slice(),
            id,
        );

        match self.store.get(key).await? {
            None => Err(EventError::EventNotFound(error::Event::new_with_id(
                organization_id,
                project_id,
                id,
            ))
            .into()),
            Some(value) => Ok(deserialize(&value)?),
        }
    }

    async fn get_by_name(
        &self,
        organization_id: u64,
        project_id: u64,
        name: &str,
    ) -> Result<Event> {
        let _guard = self.guard.read().await;
        self._get_by_name(organization_id, project_id, name).await
    }

    async fn list(&self, organization_id: u64, project_id: u64) -> Result<ListResponse<Event>> {
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
        req: UpdateEventRequest,
    ) -> Result<Event> {
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
        if let OptionalProperty::Some(display_name) = &req.display_name {
            idx_keys.push(index_display_name_key(
                organization_id,
                project_id,
                display_name.to_owned(),
            ));
            idx_prev_keys.push(index_display_name_key(
                organization_id,
                project_id,
                prev_event.display_name,
            ));
            event.display_name = display_name.to_owned();
        }
        match self
            .idx
            .check_update_constraints(idx_keys.as_ref(), idx_prev_keys.as_ref())
            .await
        {
            Err(MetadataError::Store(StoreError::KeyAlreadyExists(_))) => {
                return Err(EventError::EventAlreadyExist(error::Event::new_with_id(
                    organization_id,
                    project_id,
                    event_id,
                ))
                .into());
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
        if let OptionalProperty::Some(properties) = req.properties {
            event.properties = properties;
        }
        if let OptionalProperty::Some(custom_properties) = req.custom_properties {
            event.custom_properties = custom_properties;
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

    async fn attach_property(
        &self,
        organization_id: u64,
        project_id: u64,
        event_id: u64,
        prop_id: u64,
    ) -> Result<Event> {
        let _guard = self.guard.write().await;
        let mut event = self
            .get_by_id(organization_id, project_id, event_id)
            .await?;
        event.properties = match event.properties {
            None => Some(vec![prop_id]),
            Some(props) => match props.iter().find(|x| prop_id == **x) {
                None => Some([props, vec![prop_id]].concat()),
                Some(_) => {
                    return Err(EventError::PropertyAlreadyExist(error::Property {
                        organization_id,
                        project_id,
                        namespace: Namespace::Event,
                        event_id: Some(event_id),
                        property_id: Some(prop_id),
                        property_name: None,
                    })
                    .into());
                }
            },
        };

        self.store
            .put(
                make_data_value_key(
                    org_proj_ns(organization_id, project_id, NAMESPACE).as_slice(),
                    event.id,
                ),
                serialize(&event)?,
            )
            .await?;
        Ok(event)
    }

    async fn detach_property(
        &self,
        organization_id: u64,
        project_id: u64,
        event_id: u64,
        prop_id: u64,
    ) -> Result<Event> {
        let _guard = self.guard.write().await;
        let mut event = self
            .get_by_id(organization_id, project_id, event_id)
            .await?;
        event.properties = match event.properties {
            None => {
                return Err(EventError::PropertyNotFound(error::Property {
                    organization_id,
                    project_id,
                    namespace: Namespace::Event,
                    event_id: Some(event_id),
                    property_id: Some(prop_id),
                    property_name: None,
                })
                .into());
            }
            Some(props) => match props.iter().find(|x| prop_id == **x) {
                None => {
                    return Err(EventError::PropertyAlreadyExist(error::Property {
                        organization_id,
                        project_id,
                        namespace: Namespace::Event,
                        event_id: Some(event_id),
                        property_id: Some(prop_id),
                        property_name: None,
                    })
                    .into());
                }
                Some(_) => Some(props.into_iter().filter(|x| prop_id != *x).collect()),
            },
        };

        self.store
            .put(
                make_data_value_key(
                    org_proj_ns(organization_id, project_id, NAMESPACE).as_slice(),
                    event.id,
                ),
                serialize(&event)?,
            )
            .await?;
        Ok(event)
    }

    async fn delete(&self, organization_id: u64, project_id: u64, id: u64) -> Result<Event> {
        let _guard = self.guard.write().await;
        let event = self.get_by_id(organization_id, project_id, id).await?;
        self.store
            .delete(make_data_value_key(
                org_proj_ns(organization_id, project_id, NAMESPACE).as_slice(),
                id,
            ))
            .await?;

        self.idx
            .delete(
                index_keys(
                    organization_id,
                    project_id,
                    &event.name,
                    event.display_name.clone(),
                )
                .as_ref(),
            )
            .await?;

        Ok(event)
    }
}
