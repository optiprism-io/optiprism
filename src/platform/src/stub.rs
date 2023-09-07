use std::collections::HashMap;

use axum::async_trait;
use chrono::DateTime;
use chrono::NaiveDateTime;
use chrono::Utc;
use common::rbac::OrganizationRole;
use common::rbac::ProjectRole;
use common::rbac::Role;
use lazy_static::lazy_static;
use serde_json::Value;

use crate::accounts;
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
use crate::custom_properties;
use crate::custom_properties::CustomProperty;
use crate::dashboards;
use crate::dashboards::CreateDashboardRequest;
use crate::dashboards::Dashboard;
use crate::dashboards::Panel;
use crate::dashboards::UpdateDashboardRequest;
use crate::datatype::DataType;
use crate::datatype::DictionaryDataType;
use crate::event_records;
use crate::event_records::EventRecord;
use crate::event_records::ListEventRecordsRequest;
use crate::events;
use crate::events::CreateEventRequest;
use crate::events::Event;
use crate::events::UpdateEventRequest;
use crate::group_records;
use crate::group_records::GroupRecord;
use crate::group_records::ListGroupRecordsRequest;
use crate::group_records::UpdateGroupRecordRequest;
use crate::properties;
use crate::properties::Property;
use crate::properties::UpdatePropertyRequest;
use crate::queries;
use crate::queries::event_segmentation;
use crate::queries::event_segmentation::Analysis;
use crate::queries::event_segmentation::ChartType;
use crate::queries::event_segmentation::EventSegmentation;
use crate::queries::event_segmentation::Query;
use crate::queries::property_values::ListPropertyValuesRequest;
use crate::queries::QueryParams;
use crate::queries::QueryTime;
use crate::queries::TimeIntervalUnit;
use crate::reports;
use crate::reports::CreateReportRequest;
use crate::reports::Report;
use crate::reports::UpdateReportRequest;
use crate::Column;
use crate::ColumnType;
use crate::Context;
use crate::DataTable;
use crate::EventFilter;
use crate::EventRef;
use crate::JSONQueryResponse;
use crate::ListResponse;
use crate::PropValueOperation;
use crate::PropertyRef;
use crate::QueryResponse;
use crate::ResponseMetadata;
use crate::Result;

lazy_static! {
    pub static ref DATE_TIME: DateTime<Utc> =
        DateTime::from_utc(NaiveDateTime::from_timestamp_opt(0, 0).unwrap(), Utc);
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
            meta: ResponseMetadata {
                next: Some("next".to_string()),
            },
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
            meta: ResponseMetadata {
                next: Some("next".to_string()),
            },
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
            event_properties: Some(vec![1]),
            user_properties: Some(vec![1]),
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
            meta: ResponseMetadata {
                next: Some("next".to_string()),
            },
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
            events: Some(vec![1]),
            tags: Some(vec!["tag".to_string()]),
            name: "name".to_string(),
            display_name: Some("display_name".to_string()),
            description: Some("description".to_string()),
            data_type: DataType::Number,
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
            meta: ResponseMetadata {
                next: Some("next".to_string()),
            },
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

pub struct CustomProperties {}

impl CustomProperties {
    pub fn property() -> CustomProperty {
        CustomProperty {
            id: 1,
            created_at: *DATE_TIME,
            updated_at: Some(*DATE_TIME),
            created_by: 1,
            updated_by: Some(1),
            project_id: 1,
            tags: Some(vec!["tag".to_string()]),
            name: "name".to_string(),
            description: Some("description".to_string()),
        }
    }
}

#[async_trait]
impl custom_properties::Provider for CustomProperties {
    async fn list(
        &self,
        _ctx: Context,
        _organization_id: u64,
        _project_id: u64,
    ) -> Result<ListResponse<CustomProperty>> {
        Ok(ListResponse {
            data: vec![CustomProperties::property()],
            meta: ResponseMetadata {
                next: Some("next".to_string()),
            },
        })
    }
}

pub struct Queries {}

impl Queries {
    pub fn event_segmentation() -> EventSegmentation {
        EventSegmentation {
            time: QueryTime::From { from: *DATE_TIME },
            group: "group".to_string(),
            interval_unit: TimeIntervalUnit::Second,
            chart_type: ChartType::Line,
            analysis: Analysis::Linear,
            compare: None,
            events: vec![event_segmentation::Event {
                event: EventRef::Regular {
                    event_name: "event".to_string(),
                },
                filters: None,
                breakdowns: None,
                queries: vec![Query::CountEvents],
            }],
            filters: None,
            breakdowns: None,
            segments: None,
        }
    }
}

#[async_trait]
impl queries::Provider for Queries {
    async fn event_segmentation(
        &self,
        _ctx: Context,
        _organization_id: u64,
        _project_id: u64,
        _req: EventSegmentation,
        _query: QueryParams,
    ) -> Result<QueryResponse> {
        let columns = vec![Column {
            typ: ColumnType::Dimension,
            name: "name".to_string(),
            is_nullable: true,
            data_type: DataType::Number,
            data: vec![Value::from(1)],
            step: Some(1),
            compare_values: Some(vec![Value::from(2)]),
        }];

        Ok(QueryResponse::JSON(JSONQueryResponse { columns }))
    }

    async fn property_values(
        &self,
        _ctx: Context,
        _organization_id: u64,
        _project_id: u64,
        _req: ListPropertyValuesRequest,
    ) -> Result<ListResponse<Value>> {
        Ok(ListResponse {
            data: vec![Value::from("value")],
            meta: ResponseMetadata {
                next: Some("next".to_string()),
            },
        })
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
            panels: vec![Panel {
                typ: dashboards::Type::Report,
                report_id: 1,
                x: 1,
                y: 2,
                w: 3,
                h: 4,
            }],
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
            meta: ResponseMetadata {
                next: Some("next".to_string()),
            },
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

pub struct Reports {}

impl Reports {
    pub fn entity() -> Report {
        Report {
            id: 1,
            created_at: *DATE_TIME,
            updated_at: Some(*DATE_TIME),
            created_by: 1,
            updated_by: Some(1),
            project_id: 1,
            tags: Some(vec!["tag".to_string()]),
            name: "name".to_string(),
            description: Some("description".to_string()),
            typ: reports::Type::EventSegmentation,
            query: reports::Query::EventSegmentation(Queries::event_segmentation()),
        }
    }
}

#[async_trait]
impl reports::Provider for Reports {
    async fn create(
        &self,
        _ctx: Context,
        _organization_id: u64,
        _project_id: u64,
        _request: CreateReportRequest,
    ) -> Result<Report> {
        Ok(Reports::entity())
    }

    async fn get_by_id(
        &self,
        _ctx: Context,
        _organization_id: u64,
        _project_id: u64,
        _id: u64,
    ) -> Result<Report> {
        Ok(Reports::entity())
    }

    async fn list(
        &self,
        _ctx: Context,
        _organization_id: u64,
        _project_id: u64,
    ) -> Result<ListResponse<Report>> {
        Ok(ListResponse {
            data: vec![Reports::entity()],
            meta: ResponseMetadata {
                next: Some("next".to_string()),
            },
        })
    }

    async fn update(
        &self,
        _ctx: Context,
        _organization_id: u64,
        _project_id: u64,
        _event_id: u64,
        _req: UpdateReportRequest,
    ) -> Result<Report> {
        Ok(Reports::entity())
    }

    async fn delete(
        &self,
        _ctx: Context,
        _organization_id: u64,
        _project_id: u64,
        _id: u64,
    ) -> Result<Report> {
        Ok(Reports::entity())
    }
}

pub struct EventRecords {}

impl EventRecords {
    pub fn entity() -> EventRecord {
        EventRecord {
            id: 1,
            name: "name".to_string(),
            event_properties: Some(HashMap::from([(
                "key".to_string(),
                Value::String("value".to_string()),
            )])),
            user_properties: Some(HashMap::from([(
                "key".to_string(),
                Value::String("value".to_string()),
            )])),
            matched_custom_events: Some(vec![1]),
        }
    }
}

#[async_trait]
impl event_records::Provider for EventRecords {
    async fn list(
        &self,
        _ctx: Context,
        _organization_id: u64,
        _project_id: u64,
        _request: ListEventRecordsRequest,
    ) -> Result<ListResponse<EventRecord>> {
        Ok(ListResponse {
            data: vec![EventRecords::entity()],
            meta: ResponseMetadata {
                next: Some("next".to_string()),
            },
        })
    }

    async fn get_by_id(
        &self,
        _ctx: Context,
        _organization_id: u64,
        _project_id: u64,
        _id: u64,
    ) -> Result<EventRecord> {
        Ok(EventRecords::entity())
    }
}

pub struct GroupRecords {}

impl GroupRecords {
    pub fn entity() -> GroupRecord {
        GroupRecord {
            id: 1,
            str_id: "1".to_string(),
            group: "group".to_string(),
            properties: Some(HashMap::from([(
                "key".to_string(),
                Value::String("value".to_string()),
            )])),
        }
    }
}

#[async_trait]
impl group_records::Provider for GroupRecords {
    async fn list(
        &self,
        _ctx: Context,
        _organization_id: u64,
        _project_id: u64,
        _request: ListGroupRecordsRequest,
    ) -> Result<ListResponse<GroupRecord>> {
        Ok(ListResponse {
            data: vec![GroupRecords::entity()],
            meta: ResponseMetadata {
                next: Some("next".to_string()),
            },
        })
    }

    async fn get_by_id(
        &self,
        _ctx: Context,
        _organization_id: u64,
        _project_id: u64,
        _id: u64,
    ) -> Result<GroupRecord> {
        Ok(GroupRecords::entity())
    }

    async fn update(
        &self,
        _ctx: Context,
        _organization_id: u64,
        _project_id: u64,
        _id: u64,
        _req: UpdateGroupRecordRequest,
    ) -> Result<GroupRecord> {
        Ok(GroupRecords::entity())
    }
}
