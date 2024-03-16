use std::sync::Arc;

use common::types::DType;
use metadata::MetadataProvider;
use serde_json::Value;

use crate::EventFilter;
use crate::EventRef;
use crate::PlatformError;
use crate::PropValueOperation;
use crate::PropertyRef;

pub fn validate_property(
    md: &Arc<MetadataProvider>,
    project_id: u64,
    property: &PropertyRef,
    err_prefix: String,
) -> crate::Result<()> {
    match property {
        PropertyRef::User { property_name } => {
            md.user_properties
                .get_by_name(project_id, &property_name)
                .map_err(|err| PlatformError::BadRequest(format!("{err_prefix}: {err}")))?;
        }
        PropertyRef::Event { property_name } => {
            md.event_properties
                .get_by_name(project_id, &property_name)
                .map_err(|err| PlatformError::BadRequest(format!("{err_prefix}: {err}")))?;
        }
        PropertyRef::System { property_name } => {
            md.system_properties
                .get_by_name(project_id, &property_name)
                .map_err(|err| PlatformError::BadRequest(format!("{err_prefix}: {err}")))?;
        }
        PropertyRef::Custom { .. } => {
            return Err(PlatformError::Unimplemented(
                "custom property is unimplemented".to_string(),
            ));
        }
    }
    Ok(())
}
pub fn validate_filter_property(
    md: &Arc<MetadataProvider>,
    project_id: u64,
    property: &PropertyRef,
    operation: &PropValueOperation,
    value: &Option<Vec<Value>>,
    err_prefix: String,
) -> crate::Result<()> {
    let prop = match property {
        PropertyRef::User { property_name } => md
            .user_properties
            .get_by_name(project_id, &property_name)
            .map_err(|err| PlatformError::BadRequest(format!("{err_prefix}: {err}")))?,
        PropertyRef::Event { property_name } => md
            .event_properties
            .get_by_name(project_id, &property_name)
            .map_err(|err| PlatformError::BadRequest(format!("{err_prefix}: {err}")))?,
        PropertyRef::System { property_name } => md
            .system_properties
            .get_by_name(project_id, &property_name)
            .map_err(|err| PlatformError::BadRequest(format!("{err_prefix}: {err}")))?,
        PropertyRef::Custom { .. } => {
            return Err(PlatformError::Unimplemented(
                "custom property is unimplemented".to_string(),
            ));
        }
    };
    match operation {
        PropValueOperation::Gt
        | PropValueOperation::Gte
        | PropValueOperation::Lt
        | PropValueOperation::Lte => match prop.data_type {
            DType::Int8 | DType::Int16 | DType::Int64 | DType::Decimal | DType::Timestamp => {}
            _ => {
                return Err(PlatformError::BadRequest(format!(
                    "{err_prefix}property \"{}\" is not numeric, but operation is \"{:?}\"",
                    prop.name, operation
                )));
            }
        },
        PropValueOperation::True | PropValueOperation::False => {
            if prop.data_type != DType::Boolean {
                return Err(PlatformError::BadRequest(format!(
                    "{err_prefix}\"{}\" is not boolean, but operation is \"{:?}\"",
                    prop.name, operation
                )));
            }
        }
        PropValueOperation::Exists | PropValueOperation::Empty => {
            if !prop.nullable {
                return Err(PlatformError::BadRequest(format!(
                    "{err_prefix}property \"{}\" is not nullable, but operation is \"{:?}\"",
                    prop.name, operation
                )));
            }
        }
        PropValueOperation::Regex
        | PropValueOperation::Like
        | PropValueOperation::NotLike
        | PropValueOperation::NotRegex => {
            if prop.data_type != DType::String {
                return Err(PlatformError::BadRequest(format!(
                    "{err_prefix}property \"{}\" is not string, but operation is \"{:?}\"",
                    prop.name, operation
                )));
            }
        }
        _ => {}
    }
    match value {
        None => {
            if !prop.nullable {
                return Err(PlatformError::BadRequest(format!(
                    "{err_prefix}property \"{}\" is not nullable",
                    prop.name
                )));
            }
        }
        Some(values) => {
            for (vid, value) in values.iter().enumerate() {
                match value {
                    Value::Null => {
                        if !prop.nullable {
                            return Err(PlatformError::BadRequest(format!(
                                "{err_prefix}property \"{}\" is not nullable, but value {vid} is \"{value:?}\"",
                                prop.name
                            )));
                        }
                    }
                    Value::Bool(_) => {
                        if prop.data_type != DType::Boolean {
                            return Err(PlatformError::BadRequest(format!(
                                "{err_prefix}property \"{}\" is not boolean, but value {vid} is \"{value:?}\"",
                                prop.name
                            )));
                        }
                    }
                    Value::Number(_) => match prop.data_type {
                        DType::Int8
                        | DType::Int16
                        | DType::Int32
                        | DType::Int64
                        | DType::Decimal
                        | DType::Timestamp => {}
                        _ => {
                            return Err(PlatformError::BadRequest(format!(
                                "{err_prefix}property \"{}\" is not numeric, but value {vid} is \"{value:?}\"",
                                prop.name
                            )));
                        }
                    },
                    Value::String(_) => {
                        if prop.data_type != DType::String {
                            return Err(PlatformError::BadRequest(format!(
                                "{err_prefix}property \"{}\" is not string, but value {vid} is \"{value:?}\"",
                                prop.name
                            )));
                        }
                    }
                    Value::Array(_) => {
                        return Err(PlatformError::Unimplemented(
                            "array value is unimplemented".to_string(),
                        ));
                    }
                    Value::Object(_) => {
                        return Err(PlatformError::Unimplemented(
                            "object value is unimplemented".to_string(),
                        ));
                    }
                }
            }
        }
    }
    Ok(())
}

pub(crate) fn validate_event(
    md: &Arc<MetadataProvider>,
    project_id: u64,
    event: &EventRef,
    event_id: usize,
    err_prefix: String,
) -> crate::Result<()> {
    match event {
        EventRef::Regular { event_name } => {
            md.events
                .get_by_name(project_id, &event_name)
                .map_err(|err| PlatformError::BadRequest(format!("event {event_id}: {err}")))?;
        }
        EventRef::Custom { event_id } => {
            md.custom_events
                .get_by_id(project_id, *event_id)
                .map_err(|err| PlatformError::BadRequest(format!("event {event_id}: {err}")))?;
        }
    }

    Ok(())
}

pub(crate) fn validate_event_filter(
    md: &Arc<MetadataProvider>,
    project_id: u64,
    filter: &EventFilter,
    filter_id: usize,
    err_prefix: String,
) -> crate::Result<()> {
    match filter {
        EventFilter::Property {
            property,
            operation,
            value,
        } => {
            validate_filter_property(
                md,
                project_id,
                property,
                operation,
                value,
                format!("{err_prefix}filter #{filter_id}"),
            )?;
        }
        EventFilter::Group { .. } => {}
        EventFilter::Cohort { .. } => {
            return Err(PlatformError::Unimplemented(
                "filter by cohort is unimplemented yet".to_string(),
            ));
        }
        EventFilter::Group { .. } => {
            return Err(PlatformError::Unimplemented(
                "filter by group is unimplemented yet".to_string(),
            ));
        }
    }

    Ok(())
}
