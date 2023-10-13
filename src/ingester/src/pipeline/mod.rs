//! Ingester pipeline setup.

mod metadata_update;

use std::sync::Arc;

use metadata::atomic_counters::Provider as AtomicCountersProvider;
use metadata::events::Provider as EventsMetadataProvider;
use metadata::properties::Provider as PropertiesMetadataProvider;

use crate::error::Error;
use crate::input::IngesterInput;

pub struct Ingester {
    pub events_metadata_provider: Arc<dyn EventsMetadataProvider>,
    pub event_properties_metadata_provider: Arc<dyn PropertiesMetadataProvider>,
    pub user_properties_metadata_provider: Arc<dyn PropertiesMetadataProvider>,
    pub atomic_counters_provider: Arc<dyn AtomicCountersProvider>,
}

type EventRecordId = u64;

impl Ingester {
    pub async fn ingest(&self, input: impl Into<IngesterInput>) -> Result<EventRecordId, Error> {
        let ingester_input: IngesterInput = input.into();

        // Create event properties metadata
        let event_properties_ids = if let Some(properties) = ingester_input.event_properties {
            Some(
                metadata_update::create_properties_metadata(
                    self.event_properties_metadata_provider.as_ref(),
                    ingester_input.organization_id,
                    ingester_input.project_id,
                    properties,
                )
                .await?,
            )
        } else {
            None
        };

        // Create user properties metadata
        let _user_properties_ids = if let Some(properties) = ingester_input.user_properties {
            Some(
                metadata_update::create_properties_metadata(
                    self.user_properties_metadata_provider.as_ref(),
                    ingester_input.organization_id,
                    ingester_input.project_id,
                    properties,
                )
                .await?,
            )
        } else {
            None
        };

        // Create event metadata
        metadata_update::create_event_metadata(
            self.events_metadata_provider.as_ref(),
            ingester_input.organization_id,
            ingester_input.project_id,
            ingester_input.event_name,
            event_properties_ids,
        )
        .await?;

        let event_record_id = self
            .atomic_counters_provider
            .next_event_record(ingester_input.organization_id, ingester_input.project_id)
            .await?;

        // TODO store event and user records

        Ok(event_record_id)
    }
}
