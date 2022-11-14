use lazy_static::lazy_static;
use common::rbac::OrganizationRole;
use common::rbac::ProjectRole;
use common::rbac::Role;
use common::types::DictionaryDataType;
use common::DataType;
use crate::{accounts, dashboards};
use crate::accounts::Account;
use crate::accounts::CreateAccountRequest;
use crate::accounts::UpdateAccountRequest;
use crate::auth;
use crate::auth::LogInRequest;
use crate::auth::SignUpRequest;
use crate::auth::TokensResponse;
use crate::custom_events;
use crate::custom_events::CreateCustomEventRequest;
use crate::custom_events::CustomEvent;
use crate::custom_events::UpdateCustomEventRequest;
use crate::events;
use crate::events::CreateEventRequest;
use crate::events::Event;
use crate::events::UpdateEventRequest;
use crate::properties;
use crate::properties::Property;
use crate::properties::UpdatePropertyRequest;
use crate::queries;
use crate::queries::event_segmentation::EventSegmentation;
use crate::queries::property_values::PropertyValues;
use crate::Column;
use crate::Context;
use crate::DataTable;
use crate::EventFilter;
use crate::EventRef;
use crate::ListResponse;
use crate::PropValueOperation;
use crate::PropertyRef;
use crate::ResponseMetadata;
use serde_json::Value;
use crate::dashboards::{CreateDashboardRequest, Dashboard, Panel, Row, UpdateDashboardRequest};
use chrono::Utc;
use chrono::NaiveDateTime;
use chrono::DateTime;
use chrono::Duration;
use axum::async_trait;
use crate::Result;

lazy_static! {
    pub static ref DATE_TIME: DateTime<Utc> =
        DateTime::from_utc(NaiveDateTime::from_timestamp(1000, 0), Utc);
}


pub struct Accounts {}

impl Accounts {
    pub fn account() -> Account {
        Account {
            id: 1,
            created_at: *DATE_TIME,
            created_by: Some(1),
            updated_at: Some(*DATE_TIME),
            updated_by: Some(1),
            email: "email".to_string(),
            first_name: Some("first_name".to_string()),
            last_name: Some("last_name".to_string()),
            role: Some(Role::Admin),
            organizations: Some(vec![(1, OrganizationRole::Admin)]),
            projects: Some(vec![(1, ProjectRole::Admin)]),
            teams: Some(vec![(1, Role::Admin)]),
        }
    }
}

#[async_trait]
impl accounts::Provider for Accounts {
    async fn create(&self, _ctx: Context, _req: CreateAccountRequest) -> Result<Account> {
        Ok(Accounts::account())
    }

    async fn get_by_id(&self, _ctx: Context, _id: u64) -> Result<Account> {
        Ok(Accounts::account())
    }

    async fn list(&self, _ctx: Context) -> Result<ListResponse<Account>> {
        Ok(ListResponse {
            data: vec![Accounts::account()],
            meta: ResponseMetadata { next: None },
        })
    }

    async fn update(
        &self,
        _ctx: Context,
        _account_id: u64,
        _req: UpdateAccountRequest,
    ) -> Result<Account> {
        Ok(Accounts::account())
    }

    async fn delete(&self, _ctx: Context, _id: u64) -> Result<Account> {
        Ok(Accounts::account())
    }
}

pub struct Auth {}

impl Auth {
    pub fn token_response() -> TokensResponse {
        TokensResponse {
            access_token: "access_token".to_string(),
            refresh_token: "refresh_token".to_string(),
        }
    }
}

#[async_trait]
impl auth::Provider for Auth {
    async fn sign_up(&self, _req: SignUpRequest) -> Result<TokensResponse> {
        Ok(Auth::token_response())
    }

    async fn log_in(&self, _req: LogInRequest) -> Result<TokensResponse> {
        Ok(Auth::token_response())
    }

    async fn refresh_token(&self, _refresh_token: &str) -> Result<TokensResponse> {
        Ok(Auth::token_response())
    }
}

pub struct CustomEvents {}

impl CustomEvents {
    pub fn custom_event() -> CustomEvent {
        CustomEvent {
            id: 1,
            created_at: *DATE_TIME,
            updated_at: Some(*DATE_TIME),
            created_by: 1,
            updated_by: Some(1),
            project_id: 1,
            tags: Some(vec!["tag".to_string()]),
            name: "name".to_string(),
            description: Some("description".to_string()),
            status: custom_events::Status::Enabled,
            is_system: true,
            events: vec![custom_events::Event {
                event: EventRef::Custom { event_id: 1 },
                filters: Some(vec![EventFilter::Property {
                    property: PropertyRef::Event {
                        property_name: "prop".to_string(),
                    },
                    operation: PropValueOperation::Eq,
                    value: Some(vec![Value::from(1u8)]),
                }]),
            }],
        }
    }
}

#[async_trait]
impl custom_events::Provider for CustomEvents {
    async fn create(
        &self,
        _ctx: Context,
        _organization_id: u64,
        _project_id: u64,
        _req: CreateCustomEventRequest,
    ) -> Result<CustomEvent> {
        Ok(CustomEvents::custom_event())
    }

    async fn get_by_id(
        &self,
        _ctx: Context,
        _organization_id: u64,
        _project_id: u64,
        _id: u64,
    ) -> Result<CustomEvent> {
        Ok(CustomEvents::custom_event())
    }

    async fn list(
        &self,
        _ctx: Context,
        _organization_id: u64,
        _project_id: u64,
    ) -> Result<ListResponse<CustomEvent>> {
        Ok(ListResponse {
            data: vec![CustomEvents::custom_event()],
            meta: ResponseMetadata { next: None },
        })
    }

    async fn update(
        &self,
        _ctx: Context,
        _organization_id: u64,
        _project_id: u64,
        _event_id: u64,
        _req: UpdateCustomEventRequest,
    ) -> Result<CustomEvent> {
        Ok(CustomEvents::custom_event())
    }

    async fn delete(
        &self,
        _ctx: Context,
        _organization_id: u64,
        _project_id: u64,
        _id: u64,
    ) -> Result<CustomEvent> {
        Ok(CustomEvents::custom_event())
    }
}

pub struct Events {}

impl Events {
    pub fn event() -> Event {
        Event {
            id: 1,
            created_at: *DATE_TIME,
            updated_at: Some(*DATE_TIME),
            created_by: 1,
            updated_by: Some(1),
            project_id: 1,
            tags: Some(vec!["tag".to_string()]),
            name: "name".to_string(),
            display_name: Some("display_name".to_string()),
            description: Some("description".to_string()),
            status: events::Status::Enabled,
            is_system: true,
            properties: Some(vec![1]),
            custom_properties: Some(vec![1]),
        }
    }
}

#[async_trait]
impl events::Provider for Events {
    async fn create(
        &self,
        _ctx: Context,
        _organization_id: u64,
        _project_id: u64,
        _request: CreateEventRequest,
    ) -> Result<Event> {
        Ok(Events::event())
    }

    async fn get_by_id(
        &self,
        _ctx: Context,
        _organization_id: u64,
        _project_id: u64,
        _id: u64,
    ) -> Result<Event> {
        Ok(Events::event())
    }

    async fn get_by_name(
        &self,
        _ctx: Context,
        _organization_id: u64,
        _project_id: u64,
        _name: &str,
    ) -> Result<Event> {
        Ok(Events::event())
    }

    async fn list(
        &self,
        _ctx: Context,
        _organization_id: u64,
        _project_id: u64,
    ) -> Result<ListResponse<Event>> {
        Ok(ListResponse {
            data: vec![Events::event()],
            meta: ResponseMetadata { next: None },
        })
    }

    async fn update(
        &self,
        _ctx: Context,
        _organization_id: u64,
        _project_id: u64,
        _event_id: u64,
        _req: UpdateEventRequest,
    ) -> Result<Event> {
        Ok(Events::event())
    }

    async fn attach_property(
        &self,
        _ctx: Context,
        _organization_id: u64,
        _project_id: u64,
        _event_id: u64,
        _prop_id: u64,
    ) -> Result<Event> {
        Ok(Events::event())
    }

    async fn detach_property(
        &self,
        _ctx: Context,
        _organization_id: u64,
        _project_id: u64,
        _event_id: u64,
        _prop_id: u64,
    ) -> Result<Event> {
        Ok(Events::event())
    }

    async fn delete(
        &self,
        _ctx: Context,
        _organization_id: u64,
        _project_id: u64,
        _id: u64,
    ) -> Result<Event> {
        Ok(Events::event())
    }
}

pub struct Properties {}

impl Properties {
    pub fn property() -> Property {
        Property {
            id: 1,
            created_at: *DATE_TIME,
            updated_at: Some(*DATE_TIME),
            created_by: 1,
            updated_by: Some(1),
            project_id: 1,
            tags: Some(vec!["tag".to_string()]),
            name: "name".to_string(),
            description: Some("description".to_string()),
            display_name: Some("display_name".to_string()),
            typ: DataType::Number,
            status: properties::Status::Enabled,
            is_system: true,
            nullable: true,
            is_array: true,
            is_dictionary: true,
            dictionary_type: Some(DictionaryDataType::UInt8),
        }
    }
}

#[async_trait]
impl properties::Provider for Properties {
    async fn get_by_id(
        &self,
        _ctx: Context,
        _organization_id: u64,
        _project_id: u64,
        _id: u64,
    ) -> Result<Property> {
        Ok(Properties::property())
    }

    async fn get_by_name(
        &self,
        _ctx: Context,
        _organization_id: u64,
        _project_id: u64,
        _name: &str,
    ) -> Result<Property> {
        Ok(Properties::property())
    }

    async fn list(
        &self,
        _ctx: Context,
        _organization_id: u64,
        _project_id: u64,
    ) -> Result<ListResponse<Property>> {
        Ok(ListResponse {
            data: vec![Properties::property()],
            meta: ResponseMetadata { next: None },
        })
    }

    async fn update(
        &self,
        _ctx: Context,
        _organization_id: u64,
        _project_id: u64,
        _property_id: u64,
        _req: UpdatePropertyRequest,
    ) -> Result<Property> {
        Ok(Properties::property())
    }

    async fn delete(
        &self,
        _ctx: Context,
        _organization_id: u64,
        _project_id: u64,
        _id: u64,
    ) -> Result<Property> {
        Ok(Properties::property())
    }
}

pub struct Queries {}

#[async_trait]
impl queries::Provider for Queries {
    async fn event_segmentation(
        &self,
        _ctx: Context,
        _organization_id: u64,
        _project_id: u64,
        _req: EventSegmentation,
    ) -> Result<DataTable> {
        Ok(DataTable::new(vec![Column {
            name: "name".to_string(),
            group: "group".to_string(),
            is_nullable: true,
            data_type: arrow::datatypes::DataType::Null,
            data: vec![Value::from(1u8)],
        }]))
    }

    async fn property_values(
        &self,
        _ctx: Context,
        _organization_id: u64,
        _project_id: u64,
        _req: PropertyValues,
    ) -> Result<queries::property_values::ListResponse> {
        Ok(queries::property_values::ListResponse::new(vec![
            Value::from("value"),
        ]))
    }
}

pub struct Dashboards {}

impl Dashboards {
    pub fn dashboard() -> Dashboard {
        Dashboard {
            id: 1,
            created_at: *DATE_TIME,
            updated_at: Some(*DATE_TIME),
            created_by: 1,
            updated_by: Some(1),
            project_id: 1,
            tags: Some(vec!["tag".to_string()]),
            name: "name".to_string(),
            description: Some("description".to_string()),
            rows: vec![
                Row {
                    panels: vec![Panel {
                        span: 1,
                        typ: dashboards::Type::Report,
                        report_id: 0,
                    }]
                }
            ],
        }
    }
}

#[async_trait]
impl dashboards::Provider for Dashboards {
    async fn create(
        &self,
        _ctx: Context,
        _organization_id: u64,
        _project_id: u64,
        _request: CreateDashboardRequest,
    ) -> Result<Dashboard> {
        Ok(Dashboards::dashboard())
    }

    async fn get_by_id(
        &self,
        _ctx: Context,
        _organization_id: u64,
        _project_id: u64,
        _id: u64,
    ) -> Result<Dashboard> {
        Ok(Dashboards::dashboard())
    }

    async fn list(
        &self,
        _ctx: Context,
        _organization_id: u64,
        _project_id: u64,
    ) -> Result<ListResponse<Dashboard>> {
        Ok(ListResponse {
            data: vec![Dashboards::dashboard()],
            meta: ResponseMetadata { next: None },
        })
    }

    async fn update(
        &self,
        _ctx: Context,
        _organization_id: u64,
        _project_id: u64,
        _event_id: u64,
        _req: UpdateDashboardRequest,
    ) -> Result<Dashboard> {
        Ok(Dashboards::dashboard())
    }

    async fn delete(
        &self,
        _ctx: Context,
        _organization_id: u64,
        _project_id: u64,
        _id: u64,
    ) -> Result<Dashboard> {
        Ok(Dashboards::dashboard())
    }
}