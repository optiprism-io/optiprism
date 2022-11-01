use async_trait::async_trait;
use common::rbac::OrganizationRole;
use common::rbac::ProjectRole;
use common::rbac::Role;
use common::types::DictionaryDataType;
use common::DataType;
use platform::accounts;
use platform::accounts::Account;
use platform::accounts::CreateAccountRequest;
use platform::accounts::UpdateAccountRequest;
use platform::auth;
use platform::auth::LogInRequest;
use platform::auth::SignUpRequest;
use platform::auth::TokensResponse;
use platform::custom_events;
use platform::custom_events::CreateCustomEventRequest;
use platform::custom_events::CustomEvent;
use platform::custom_events::UpdateCustomEventRequest;
use platform::events;
use platform::events::CreateEventRequest;
use platform::events::Event;
use platform::events::UpdateEventRequest;
use platform::properties;
use platform::properties::Property;
use platform::properties::UpdatePropertyRequest;
use platform::queries;
use platform::queries::event_segmentation::EventSegmentation;
use platform::queries::property_values::PropertyValues;
use platform::Column;
use platform::Context;
use platform::DataTable;
use platform::EventFilter;
use platform::EventRef;
use platform::ListResponse;
use platform::PropValueOperation;
use platform::PropertyRef;
use platform::ResponseMetadata;
use serde_json::Value;

use crate::DATE_TIME;

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
    async fn create(&self, _ctx: Context, _req: CreateAccountRequest) -> platform::Result<Account> {
        Ok(Accounts::account())
    }

    async fn get_by_id(&self, _ctx: Context, _id: u64) -> platform::Result<Account> {
        Ok(Accounts::account())
    }

    async fn list(&self, _ctx: Context) -> platform::Result<ListResponse<Account>> {
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
    ) -> platform::Result<Account> {
        Ok(Accounts::account())
    }

    async fn delete(&self, _ctx: Context, _id: u64) -> platform::Result<Account> {
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
    async fn sign_up(&self, _req: SignUpRequest) -> platform::Result<TokensResponse> {
        Ok(Auth::token_response())
    }

    async fn log_in(&self, _req: LogInRequest) -> platform::Result<TokensResponse> {
        Ok(Auth::token_response())
    }

    async fn refresh_token(&self, _refresh_token: &str) -> platform::Result<TokensResponse> {
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
    ) -> platform::Result<CustomEvent> {
        Ok(CustomEvents::custom_event())
    }

    async fn get_by_id(
        &self,
        _ctx: Context,
        _organization_id: u64,
        _project_id: u64,
        _id: u64,
    ) -> platform::Result<CustomEvent> {
        Ok(CustomEvents::custom_event())
    }

    async fn list(
        &self,
        _ctx: Context,
        _organization_id: u64,
        _project_id: u64,
    ) -> platform::Result<ListResponse<CustomEvent>> {
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
    ) -> platform::Result<CustomEvent> {
        Ok(CustomEvents::custom_event())
    }

    async fn delete(
        &self,
        _ctx: Context,
        _organization_id: u64,
        _project_id: u64,
        _id: u64,
    ) -> platform::Result<CustomEvent> {
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
    ) -> platform::Result<Event> {
        Ok(Events::event())
    }

    async fn get_by_id(
        &self,
        _ctx: Context,
        _organization_id: u64,
        _project_id: u64,
        _id: u64,
    ) -> platform::Result<Event> {
        Ok(Events::event())
    }

    async fn get_by_name(
        &self,
        _ctx: Context,
        _organization_id: u64,
        _project_id: u64,
        _name: &str,
    ) -> platform::Result<Event> {
        Ok(Events::event())
    }

    async fn list(
        &self,
        _ctx: Context,
        _organization_id: u64,
        _project_id: u64,
    ) -> platform::Result<ListResponse<Event>> {
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
    ) -> platform::Result<Event> {
        Ok(Events::event())
    }

    async fn attach_property(
        &self,
        _ctx: Context,
        _organization_id: u64,
        _project_id: u64,
        _event_id: u64,
        _prop_id: u64,
    ) -> platform::Result<Event> {
        Ok(Events::event())
    }

    async fn detach_property(
        &self,
        _ctx: Context,
        _organization_id: u64,
        _project_id: u64,
        _event_id: u64,
        _prop_id: u64,
    ) -> platform::Result<Event> {
        Ok(Events::event())
    }

    async fn delete(
        &self,
        _ctx: Context,
        _organization_id: u64,
        _project_id: u64,
        _id: u64,
    ) -> platform::Result<Event> {
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
    ) -> platform::Result<Property> {
        Ok(Properties::property())
    }

    async fn get_by_name(
        &self,
        _ctx: Context,
        _organization_id: u64,
        _project_id: u64,
        _name: &str,
    ) -> platform::Result<Property> {
        Ok(Properties::property())
    }

    async fn list(
        &self,
        _ctx: Context,
        _organization_id: u64,
        _project_id: u64,
    ) -> platform::Result<ListResponse<Property>> {
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
    ) -> platform::Result<Property> {
        Ok(Properties::property())
    }

    async fn delete(
        &self,
        _ctx: Context,
        _organization_id: u64,
        _project_id: u64,
        _id: u64,
    ) -> platform::Result<Property> {
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
    ) -> platform::Result<DataTable> {
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
    ) -> platform::Result<queries::property_values::ListResponse> {
        Ok(queries::property_values::ListResponse::new(vec![
            Value::from("value"),
        ]))
    }
}
