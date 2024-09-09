use std::sync::Arc;
use arrow::array::ArrayRef;

use chrono::DateTime;
use chrono::Utc;
use common::rbac::ProjectPermission;
use common::types::DType;
use common::types::OptionalProperty;
use metadata::{MetadataProvider, properties};
use serde::Deserialize;
use serde::Serialize;
use serde_json::Value;
use query::properties::{PropertiesProvider, PropertyValues};

use crate::{array_ref_to_json_values, Context, EventRef, json_value_to_scalar, PropertyRef, PropValueOperation, ResponseMetadata, validate_event_filter_property};
use crate::ListResponse;
use crate::PlatformError;
use crate::Result;

pub struct Properties {
    props: Arc<metadata::properties::Properties>,
    md: Arc<MetadataProvider>,
    prov: Arc<PropertiesProvider>,
}

impl Properties {
    pub fn new(props: Arc<metadata::properties::Properties>, md: Arc<MetadataProvider>, prov: Arc<PropertiesProvider>) -> Self {
        Self { props, md, prov }
    }
    pub fn new_group(props: Arc<metadata::properties::Properties>, md: Arc<MetadataProvider>, prov: Arc<PropertiesProvider>) -> Self {
        Self { props, md, prov }
    }
    pub fn new_event(props: Arc<metadata::properties::Properties>, md: Arc<MetadataProvider>, prov: Arc<PropertiesProvider>) -> Self {
        Self { props, md, prov }
    }
    pub fn new_system(props: Arc<metadata::properties::Properties>, md: Arc<MetadataProvider>, prov: Arc<PropertiesProvider>) -> Self {
        Self { props, md, prov }
    }
    pub async fn get_by_id(&self, ctx: Context, project_id: u64, id: u64) -> Result<Property> {
        ctx.check_project_permission(
            ctx.organization_id,
            project_id,
            ProjectPermission::ViewSchema,
        )?;

        Ok(self.props.get_by_id(project_id, id)?.into())
    }

    pub async fn get_by_name(&self, ctx: Context, project_id: u64, name: &str) -> Result<Property> {
        ctx.check_project_permission(
            ctx.organization_id,
            project_id,
            ProjectPermission::ViewSchema,
        )?;

        let event = self.props.get_by_name(project_id, name)?;

        Ok(event.into())
    }

    pub async fn list(&self, ctx: Context, project_id: u64) -> Result<ListResponse<Property>> {
        ctx.check_project_permission(
            ctx.organization_id,
            project_id,
            ProjectPermission::ViewSchema,
        )?;
        let resp = self.props.list(project_id)?;

        Ok(resp.into())
    }

    pub async fn update(
        &self,
        ctx: Context,
        project_id: u64,
        property_id: u64,
        req: UpdatePropertyRequest,
    ) -> Result<Property> {
        ctx.check_project_permission(
            ctx.organization_id,
            project_id,
            ProjectPermission::ManageSchema,
        )?;

        let md_req = metadata::properties::UpdatePropertyRequest {
            updated_by: ctx.account_id,
            tags: req.tags,
            description: req.description,
            display_name: req.display_name,
            status: req.status.into(),
            is_dictionary: Default::default(),
            dictionary_type: Default::default(),
            ..Default::default()
        };

        let prop = self.props.update(project_id, property_id, md_req)?;

        Ok(prop.into())
    }

    pub async fn delete(&self, ctx: Context, project_id: u64, id: u64) -> Result<Property> {
        ctx.check_project_permission(
            ctx.organization_id,
            project_id,
            ProjectPermission::DeleteSchema,
        )?;

        Ok(self.props.delete(project_id, id)?.into())
    }

    pub async fn values(
        &self,
        ctx: Context,
        project_id: u64,
        req: ListPropertyValuesRequest,
    ) -> Result<ListResponse<Value>> {
        ctx.check_project_permission(
            ctx.organization_id,
            project_id,
            ProjectPermission::ExploreReports,
        )?;

        validate_request(&self.md, project_id, &req)?;
        let lreq = req.into();
        let result = self
            .prov
            .values(query::Context::new(project_id), lreq)
            .await?;

        Ok(result.into())
    }
}

#[derive(Serialize, Deserialize, Default, Debug, Clone, Eq, PartialEq)]
#[serde(rename_all = "camelCase")]
pub enum Status {
    #[default]
    Enabled,
    Disabled,
}

#[derive(Serialize, Deserialize, Debug, Clone, Eq, PartialEq)]
#[serde(rename_all = "camelCase")]
pub enum Type {
    System,
    Event,
    Group,
}
impl From<metadata::properties::Status> for Status {
    fn from(s: metadata::properties::Status) -> Self {
        match s {
            metadata::properties::Status::Enabled => Status::Enabled,
            metadata::properties::Status::Disabled => Status::Disabled,
        }
    }
}

impl From<Status> for metadata::properties::Status {
    fn from(s: Status) -> Self {
        match s {
            Status::Enabled => metadata::properties::Status::Enabled,
            Status::Disabled => metadata::properties::Status::Disabled,
        }
    }
}

#[derive(Serialize, Deserialize, Clone, Debug, Eq, PartialEq)]
#[serde(rename_all = "camelCase")]
pub enum DictionaryType {
    #[serde(rename = "int8")]
    Int8,
    #[serde(rename = "int16")]
    Int16,
    #[serde(rename = "int32")]
    Int32,
    #[serde(rename = "int64")]
    Int64,
}

impl Into<properties::DictionaryType> for DictionaryType {
    fn into(self) -> properties::DictionaryType {
        match self {
            DictionaryType::Int8 => properties::DictionaryType::Int8,
            DictionaryType::Int16 => properties::DictionaryType::Int16,
            DictionaryType::Int32 => properties::DictionaryType::Int32,
            DictionaryType::Int64 => properties::DictionaryType::Int64,
        }
    }
}

impl Into<DictionaryType> for properties::DictionaryType {
    fn into(self) -> DictionaryType {
        match self {
            properties::DictionaryType::Int8 => DictionaryType::Int8,
            properties::DictionaryType::Int16 => DictionaryType::Int16,
            properties::DictionaryType::Int32 => DictionaryType::Int32,
            properties::DictionaryType::Int64 => DictionaryType::Int64,
        }
    }
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct Property {
    pub id: u64,
    pub created_at: DateTime<Utc>,
    pub updated_at: Option<DateTime<Utc>>,
    pub created_by: u64,
    pub updated_by: Option<u64>,
    pub project_id: u64,
    pub events: Option<Vec<u64>>,
    pub tags: Option<Vec<String>>,
    pub name: String,
    pub display_name: Option<String>,
    pub description: Option<String>,
    pub typ: Type,
    pub group_id: Option<usize>,
    pub order: u64,
    pub data_type: DType,
    pub status: Status,
    pub hidden: bool,
    pub is_system: bool,
    pub nullable: bool,
    // this also defines whether property is required or not
    pub is_array: bool,
    pub is_dictionary: bool,
    pub dictionary_type: Option<DictionaryType>,
}

impl Into<metadata::properties::Property> for Property {
    fn into(self) -> metadata::properties::Property {
        let typ = if let Some(gid) = self.group_id {
            metadata::properties::Type::Group(gid)
        } else {
            match self.typ {
                Type::Event => metadata::properties::Type::Event,
                _ => unreachable!(),
            }
        };
        metadata::properties::Property {
            id: self.id,
            created_at: self.created_at,
            updated_at: self.updated_at,
            created_by: self.created_by,
            updated_by: self.updated_by,
            project_id: self.project_id,
            tags: self.tags,
            name: self.name,
            description: self.description,
            display_name: self.display_name,
            order: self.order,
            typ,
            data_type: self.data_type,
            status: self.status.into(),
            is_system: self.is_system,
            nullable: self.nullable,
            hidden: self.hidden,
            is_array: self.is_array,
            is_dictionary: self.is_dictionary,
            dictionary_type: self.dictionary_type.map(|v| v.into()),
        }
    }
}

impl Into<Property> for metadata::properties::Property {
    fn into(self) -> Property {
        let (typ, group_id) = match self.typ {
            metadata::properties::Type::Event => (Type::Event, None),
            metadata::properties::Type::Group(gid) => (Type::Group, Some(gid)),
        };

        Property {
            id: self.id,
            created_at: self.created_at,
            updated_at: self.updated_at,
            created_by: self.created_by,
            updated_by: self.updated_by,
            project_id: self.project_id,
            events: None,
            tags: self.tags,
            name: self.name,
            description: self.description,
            display_name: self.display_name,
            data_type: self.data_type,
            status: self.status.into(),
            hidden: self.hidden,
            is_system: self.is_system,
            nullable: self.nullable,
            is_array: self.is_array,
            is_dictionary: self.is_dictionary,
            dictionary_type: self.dictionary_type.map(|v| v.into()),
            typ,
            group_id,
            order: self.order,
        }
    }
}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct UpdatePropertyRequest {
    #[serde(default, skip_serializing_if = "OptionalProperty::is_none")]
    pub tags: OptionalProperty<Option<Vec<String>>>,
    #[serde(default, skip_serializing_if = "OptionalProperty::is_none")]
    pub display_name: OptionalProperty<Option<String>>,
    #[serde(default, skip_serializing_if = "OptionalProperty::is_none")]
    pub description: OptionalProperty<Option<String>>,
    #[serde(default, skip_serializing_if = "OptionalProperty::is_none")]
    pub status: OptionalProperty<Status>,
}

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
}

impl Into<query::properties::PropertyValues> for ListPropertyValuesRequest {
    fn into(self) -> PropertyValues {
        query::properties::PropertyValues {
            property: self.property.into(),
            event: self.event.map(|event| event.into()),
            filter: self.filter.map(|filter| filter.into()),
        }
    }
}

impl Into<query::properties::Filter> for Filter {
    fn into(self) -> query::properties::Filter {
        query::properties::Filter {
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

pub(crate) fn validate_request(
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
                .get_by_name(project_id, property_name)
                .map_err(|err| PlatformError::BadRequest(format!("{err}")))?;
        }
        PropertyRef::Event { property_name } => {
            md.event_properties
                .get_by_name(project_id, property_name)
                .map_err(|err| PlatformError::BadRequest(format!("{err}")))?;
        }
        _ => {
            return Err(PlatformError::Unimplemented(
                "invalid property type".to_string(),
            ));
        }
    }

    if let Some(event) = &req.event {
        match event {
            EventRef::Regular { event_name } => {
                md.events
                    .get_by_name(project_id, event_name)
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
        validate_event_filter_property(
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