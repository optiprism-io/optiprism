//! Module for ingester's metadata/store interaction.

use metadata::arrow::datatypes::DataType;
use metadata::error::MetadataError;
use metadata::events::CreateEventRequest;
use metadata::events::Provider as EventsProvider;
use metadata::events::Status as EventStatus;
use metadata::properties::CreatePropertyRequest;
use metadata::properties::Provider as PropertiesProvider;
use metadata::properties::Status as PropertyStatus;

use super::Properties;
use super::TrackRequest;
use super::DEFAULT_USER_ID;

/// Get properties' metadata IDs from tracking request part, creating any if necessary.
async fn create_properties_metadata<PP>(
    properties_provider: &PP,
    organization_id: u64,
    project_id: u64,
    properties: Properties,
) -> Result<Vec<u64>, MetadataError>
where
    PP: PropertiesProvider + ?Sized,
{
    let mut event_properties = Vec::new();
    for (name, _prop_value) in properties.into_iter() {
        let create_property_metadata_req = CreatePropertyRequest {
            created_by: DEFAULT_USER_ID,
            tags: None,
            name,
            description: None,
            display_name: None,
            typ: DataType::Null, // TODO: figure out datatypes from provided data
            status: PropertyStatus::Enabled,
            is_system: false,
            nullable: false,
            is_array: false,
            is_dictionary: false,
            dictionary_type: None,
        };
        event_properties.push(
            properties_provider
                .get_or_create(organization_id, project_id, create_property_metadata_req)
                .await?
                .id,
        );
    }
    Ok(event_properties)
}

/// Get event's metadata ID based of tracking request data, creating one if needed.
pub(crate) async fn create_event_metadata<EP, PP>(
    events_provider: &EP,
    properties_provider: &PP,
    organization_id: u64,
    project_id: u64,
    request: TrackRequest,
) -> Result<u64, MetadataError>
where
    EP: EventsProvider + ?Sized,
    PP: PropertiesProvider + ?Sized,
{
    let properties = if let Some(properties) = request.properties {
        Some(
            create_properties_metadata(
                properties_provider,
                organization_id,
                project_id,
                properties,
            )
            .await?,
        )
    } else {
        None
    };

    let create_event_metadata_req = CreateEventRequest {
        created_by: DEFAULT_USER_ID,
        tags: None,
        name: request.event,
        display_name: None,
        description: None,
        status: EventStatus::Enabled,
        is_system: false,
        properties,
        custom_properties: None,
    };
    events_provider
        .get_or_create(organization_id, project_id, create_event_metadata_req)
        .await
        .map(|event| event.id)
}
