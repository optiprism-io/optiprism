use std::sync::Arc;

use arrow::array::ArrayRef;
use metadata::MetadataProvider;
use serde::Deserialize;
use serde::Serialize;
use serde_json::Value;

use crate::array_ref_to_json_values;
use crate::json_value_to_scalar;
use crate::queries::event_records_search::EventRecordsSearchRequest;
use crate::queries::validation::validate_event_filter;
use crate::queries::validation::validate_filter_property;
use crate::EventRef;
use crate::ListResponse;
use crate::PlatformError;
use crate::PropValueOperation;
use crate::PropertyRef;
use crate::ResponseMetadata;
use crate::Result;

#[derive(Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Filter {
    pub operation: PropValueOperation,
    pub value: Option<Vec<Value>>,
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ListPropertyValuesRequest {
    #[serde(flatten)]
    pub property: PropertyRef,
    #[serde(flatten)]
    pub event: Option<EventRef>,
    pub filter: Option<Filter>,
    pub group: usize,
}

impl Into<query::queries::property_values::PropertyValues> for ListPropertyValuesRequest {
    fn into(self) -> query::queries::property_values::PropertyValues {
        query::queries::property_values::PropertyValues {
            property: self.property.into(),
            group_id: self.group,
            event: self.event.map(|event| event.into()),
            filter: self.filter.map(|filter| filter.into()),
        }
    }
}

impl Into<query::queries::property_values::Filter> for Filter {
    fn into(self) -> query::queries::property_values::Filter {
        query::queries::property_values::Filter {
            operation: self.operation.into(),
            value: self
                .value
                .map(|values| values.iter().map(json_value_to_scalar).collect::<Vec<_>>()),
        }
    }
}

impl Into<ListResponse<Value>> for ArrayRef {
    fn into(self) -> ListResponse<Value> {
        ListResponse {
            data: array_ref_to_json_values(&self),
            meta: ResponseMetadata { next: None },
        }
    }
}

pub(crate) fn validate(
    md: &Arc<MetadataProvider>,
    project_id: u64,
    req: &ListPropertyValuesRequest,
) -> Result<()> {
    match &req.property {
        PropertyRef::Group {
            property_name,
            group,
        } => {
            md.group_properties[*group]
                .get_by_name(project_id, &property_name)
                .map_err(|err| PlatformError::BadRequest(format!("{err}")))?;
        }
        PropertyRef::Event { property_name } => {
            md.event_properties
                .get_by_name(project_id, &property_name)
                .map_err(|err| PlatformError::BadRequest(format!("{err}")))?;
        }
        PropertyRef::System { property_name } => {
            md.system_properties
                .get_by_name(project_id, &property_name)
                .map_err(|err| PlatformError::BadRequest(format!("{err}")))?;
        }
        PropertyRef::Custom { .. } => {
            return Err(PlatformError::Unimplemented(
                "custom property is unimplemented".to_string(),
            ));
        }
    }

    if let Some(event) = &req.event {
        match event {
            EventRef::Regular { event_name } => {
                md.events
                    .get_by_name(project_id, &event_name)
                    .map_err(|err| PlatformError::BadRequest(format!("{err}")))?;
            }
            EventRef::Custom { event_id } => {
                md.custom_events
                    .get_by_id(project_id, *event_id)
                    .map_err(|err| PlatformError::BadRequest(format!("{err}")))?;
            }
        }
    }

    if let Some(filter) = &req.filter {
        validate_filter_property(
            md,
            project_id,
            &req.property,
            &filter.operation,
            &filter.value,
            "".to_string(),
        )?;
    }
    Ok(())
}
