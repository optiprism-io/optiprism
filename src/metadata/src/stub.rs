use async_trait::async_trait;
use chrono::DateTime;
use chrono::NaiveDateTime;
use chrono::Utc;
use common::query::event_segmentation;
use common::query::event_segmentation::Analysis;
use common::query::event_segmentation::ChartType;
use common::query::event_segmentation::EventSegmentation;
use common::query::event_segmentation::NamedQuery;
use common::query::EventFilter;
use common::query::EventRef;
use common::query::PropValueOperation;
use common::query::PropertyRef;
use common::query::QueryTime;
use common::query::TimeIntervalUnit;
use common::rbac::OrganizationRole;
use common::rbac::ProjectRole;
use common::rbac::Role;
use datafusion::arrow::datatypes::DataType;
use datafusion_common::ScalarValue;
use lazy_static::lazy_static;

use crate::accounts;
use crate::accounts::Account;
use crate::accounts::CreateAccountRequest;
use crate::accounts::UpdateAccountRequest;
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
use crate::database;
use crate::database::Column;
use crate::database::Table;
use crate::database::TableRef;
use crate::dictionaries;
use crate::events;
use crate::events::CreateEventRequest;
use crate::events::Event;
use crate::events::UpdateEventRequest;
use crate::metadata::ListResponse;
use crate::metadata::ResponseMetadata;
use crate::organizations;
use crate::organizations::CreateOrganizationRequest;
use crate::organizations::Organization;
use crate::organizations::UpdateOrganizationRequest;
use crate::projects;
use crate::projects::CreateProjectRequest;
use crate::projects::Project;
use crate::projects::SDKLogLevel;
use crate::projects::UpdateProjectRequest;
use crate::projects::SDK;
use crate::properties;
use crate::properties::CreatePropertyRequest;
use crate::properties::Property;
use crate::properties::UpdatePropertyRequest;
use crate::reports;
use crate::reports::CreateReportRequest;
use crate::reports::Query;
use crate::reports::Report;
use crate::reports::Type;
use crate::reports::UpdateReportRequest;
use crate::teams;
use crate::teams::CreateTeamRequest;
use crate::teams::Team;
use crate::teams::UpdateTeamRequest;
use crate::Result;

lazy_static! {
    pub static ref DATE_TIME: DateTime<Utc> = DateTime::from_naive_utc_and_offset(
        NaiveDateTime::from_timestamp_opt(1000, 0).unwrap(),
        Utc
    );
}

pub struct Accounts {}

impl Accounts {}

impl Accounts {
    pub fn account() -> Account {
        Account {
            id: 1,
            created_at: *DATE_TIME,
            created_by: Some(1),
            updated_at: Some(*DATE_TIME),
            updated_by: Some(1),
            password_hash: "password_hash".to_string(),
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
    async fn create(&self, _req: CreateAccountRequest) -> Result<Account> {
        Ok(Accounts::account())
    }

    async fn get_by_id(&self, _id: u64) -> Result<Account> {
        Ok(Accounts::account())
    }

    async fn get_by_email(&self, _email: &str) -> Result<Account> {
        Ok(Accounts::account())
    }

    async fn list(&self) -> Result<ListResponse<Account>> {
        Ok(ListResponse {
            data: vec![Accounts::account()],
            meta: ResponseMetadata { next: None },
        })
    }

    async fn update(&self, _account_id: u64, _req: UpdateAccountRequest) -> Result<Account> {
        Ok(Self::account())
    }

    async fn delete(&self, _id: u64) -> Result<Account> {
        Ok(Self::account())
    }
}

pub struct Dashboards {}

#[async_trait]
impl dashboards::Provider for Dashboards {
    async fn create(
        &self,
        _organization_id: u64,
        _project_id: u64,
        _req: CreateDashboardRequest,
    ) -> Result<Dashboard> {
        Ok(Dashboards::dashboard())
    }

    async fn get_by_id(
        &self,
        _organization_id: u64,
        _project_id: u64,
        _id: u64,
    ) -> Result<Dashboard> {
        Ok(Dashboards::dashboard())
    }

    async fn list(
        &self,
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
        _organization_id: u64,
        _project_id: u64,
        _dashboard_id: u64,
        _req: UpdateDashboardRequest,
    ) -> Result<Dashboard> {
        Ok(Dashboards::dashboard())
    }

    async fn delete(&self, _organization_id: u64, _project_id: u64, _id: u64) -> Result<Dashboard> {
        Ok(Dashboards::dashboard())
    }
}

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

pub struct Reports {}

#[async_trait]
impl reports::Provider for Reports {
    async fn create(
        &self,
        _organization_id: u64,
        _project_id: u64,
        _req: CreateReportRequest,
    ) -> Result<Report> {
        Ok(Reports::report())
    }

    async fn get_by_id(&self, _organization_id: u64, _project_id: u64, _id: u64) -> Result<Report> {
        Ok(Reports::report())
    }

    async fn list(&self, _organization_id: u64, _project_id: u64) -> Result<ListResponse<Report>> {
        Ok(ListResponse {
            data: vec![Reports::report()],
            meta: ResponseMetadata { next: None },
        })
    }

    async fn update(
        &self,
        _organization_id: u64,
        _project_id: u64,
        _report_id: u64,
        _req: UpdateReportRequest,
    ) -> Result<Report> {
        Ok(Reports::report())
    }

    async fn delete(&self, _organization_id: u64, _project_id: u64, _id: u64) -> Result<Report> {
        Ok(Reports::report())
    }
}

impl Reports {
    pub fn report() -> Report {
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
            typ: Type::EventSegmentation,
            query: Query::EventSegmentation(EventSegmentation {
                time: QueryTime::From(*DATE_TIME),
                group: "group".to_string(),
                interval_unit: TimeIntervalUnit::Second,
                chart_type: ChartType::Line,
                analysis: Analysis::Linear,
                compare: None,
                events: vec![event_segmentation::Event {
                    event: EventRef::Regular(1),
                    filters: None,
                    breakdowns: None,
                    queries: vec![NamedQuery {
                        agg: event_segmentation::Query::CountEvents,
                        name: Some("name".to_string()),
                    }],
                }],
                filters: None,
                breakdowns: None,
                segments: None,
            }),
        }
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
                event: EventRef::Custom(1),
                filters: Some(vec![EventFilter::Property {
                    property: PropertyRef::Event("prop".to_string()),
                    operation: PropValueOperation::Eq,
                    value: Some(vec![ScalarValue::Utf8(Some("value".to_string()))]),
                }]),
            }],
        }
    }
}

#[async_trait]
impl custom_events::Provider for CustomEvents {
    async fn create(
        &self,
        _organization_id: u64,
        _project_id: u64,
        _req: CreateCustomEventRequest,
    ) -> Result<CustomEvent> {
        Ok(CustomEvents::custom_event())
    }

    async fn get_by_id(
        &self,
        _organization_id: u64,
        _project_id: u64,
        _id: u64,
    ) -> Result<CustomEvent> {
        Ok(CustomEvents::custom_event())
    }

    async fn get_by_name(
        &self,
        _organization_id: u64,
        _project_id: u64,
        _name: &str,
    ) -> Result<CustomEvent> {
        Ok(CustomEvents::custom_event())
    }

    async fn list(
        &self,
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
        _organization_id: u64,
        _project_id: u64,
        _event_id: u64,
        _req: UpdateCustomEventRequest,
    ) -> Result<CustomEvent> {
        Ok(CustomEvents::custom_event())
    }

    async fn delete(
        &self,
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
        _organization_id: u64,
        _project_id: u64,
        _req: CreateEventRequest,
    ) -> Result<Event> {
        Ok(Events::event())
    }

    async fn get_or_create(
        &self,
        _organization_id: u64,
        _project_id: u64,
        _req: CreateEventRequest,
    ) -> Result<Event> {
        Ok(Events::event())
    }

    async fn get_by_id(&self, _organization_id: u64, _project_id: u64, _id: u64) -> Result<Event> {
        Ok(Events::event())
    }

    async fn get_by_name(
        &self,
        _organization_id: u64,
        _project_id: u64,
        _name: &str,
    ) -> Result<Event> {
        Ok(Events::event())
    }

    async fn list(&self, _organization_id: u64, _project_id: u64) -> Result<ListResponse<Event>> {
        Ok(ListResponse {
            data: vec![Events::event()],
            meta: ResponseMetadata { next: None },
        })
    }

    async fn update(
        &self,
        _organization_id: u64,
        _project_id: u64,
        _event_id: u64,
        _req: UpdateEventRequest,
    ) -> Result<Event> {
        Ok(Events::event())
    }

    async fn attach_property(
        &self,
        _organization_id: u64,
        _project_id: u64,
        _event_id: u64,
        _prop_id: u64,
    ) -> Result<Event> {
        Ok(Events::event())
    }

    async fn detach_property(
        &self,
        _organization_id: u64,
        _project_id: u64,
        _event_id: u64,
        _prop_id: u64,
    ) -> Result<Event> {
        Ok(Events::event())
    }

    async fn delete(&self, _organization_id: u64, _project_id: u64, _id: u64) -> Result<Event> {
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
            typ: DataType::Null,
            status: properties::Status::Enabled,
            is_system: true,
            nullable: true,
            is_array: true,
            is_dictionary: true,
            dictionary_type: Some(DataType::UInt8),
        }
    }
}

#[async_trait]
impl properties::Provider for Properties {
    async fn create(
        &self,
        _organization_id: u64,
        _project_id: u64,
        _req: CreatePropertyRequest,
    ) -> Result<Property> {
        Ok(Properties::property())
    }

    async fn get_or_create(
        &self,
        _organization_id: u64,
        _project_id: u64,
        _req: CreatePropertyRequest,
    ) -> Result<Property> {
        Ok(Properties::property())
    }

    async fn get_by_id(
        &self,
        _organization_id: u64,
        _project_id: u64,
        _id: u64,
    ) -> Result<Property> {
        Ok(Properties::property())
    }

    async fn get_by_name(
        &self,
        _organization_id: u64,
        _project_id: u64,
        _name: &str,
    ) -> Result<Property> {
        Ok(Properties::property())
    }

    async fn list(
        &self,
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
        _organization_id: u64,
        _project_id: u64,
        _property_id: u64,
        _req: UpdatePropertyRequest,
    ) -> Result<Property> {
        Ok(Properties::property())
    }

    async fn delete(&self, _organization_id: u64, _project_id: u64, _id: u64) -> Result<Property> {
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
        _organization_id: u64,
        _project_id: u64,
    ) -> Result<ListResponse<CustomProperty>> {
        Ok(ListResponse {
            data: vec![CustomProperties::property()],
            meta: ResponseMetadata { next: None },
        })
    }
}

pub struct Database {}

#[async_trait]
impl database::Provider for Database {
    async fn create_table(&self, _table: Table) -> Result<()> {
        Ok(())
    }

    async fn get_table(&self, _table_type: TableRef) -> Result<Table> {
        Ok(Table {
            typ: TableRef::Events(1, 1),
            columns: vec![Column::new(
                "col".to_string(),
                DataType::UInt8,
                true,
                Some(DataType::Int16),
            )],
        })
    }

    async fn add_column(&self, _table_type: TableRef, _col: Column) -> Result<()> {
        Ok(())
    }
}
#[derive(Debug)]
pub struct Dictionaries {}

#[async_trait]
impl dictionaries::Provider for Dictionaries {
    async fn get_key_or_create(
        &self,
        _organization_id: u64,
        _project_id: u64,
        _dict: &str,
        _value: &str,
    ) -> Result<u64> {
        Ok(1)
    }

    async fn get_value(
        &self,
        _organization_id: u64,
        _project_id: u64,
        _dict: &str,
        _key: u64,
    ) -> Result<String> {
        Ok("v".to_string())
    }

    async fn get_key(
        &self,
        _organization_id: u64,
        _project_id: u64,
        _dict: &str,
        _value: &str,
    ) -> Result<u64> {
        Ok(1)
    }
}

pub struct Organizations {}

impl Organizations {
    pub fn org() -> Organization {
        Organization {
            id: 1,
            created_at: *DATE_TIME,
            created_by: 1,
            updated_at: Some(*DATE_TIME),
            updated_by: Some(1),
            name: "name".to_string(),
        }
    }
}

#[async_trait]
impl organizations::Provider for Organizations {
    async fn create(&self, _req: CreateOrganizationRequest) -> Result<Organization> {
        Ok(Organizations::org())
    }

    async fn get_by_id(&self, _id: u64) -> Result<Organization> {
        Ok(Organizations::org())
    }

    async fn list(&self) -> Result<ListResponse<Organization>> {
        Ok(ListResponse {
            data: vec![Organizations::org()],
            meta: ResponseMetadata { next: None },
        })
    }

    async fn update(&self, _org_id: u64, _req: UpdateOrganizationRequest) -> Result<Organization> {
        Ok(Organizations::org())
    }

    async fn delete(&self, _id: u64) -> Result<Organization> {
        Ok(Organizations::org())
    }
}

pub struct Projects {}

impl Projects {
    pub fn project() -> Project {
        Project {
            id: 1,
            created_at: *DATE_TIME,
            created_by: 1,
            updated_at: Some(*DATE_TIME),
            updated_by: Some(1),
            organization_id: 1,
            name: "name".to_string(),
            token: "token".to_string(),
            sdk: SDK {
                autotrack_pageviews: false,
                log_level: SDKLogLevel::Debug,
            },
        }
    }
}

#[async_trait]
impl projects::Provider for Projects {
    async fn create(&self, _organization_id: u64, _req: CreateProjectRequest) -> Result<Project> {
        Ok(Projects::project())
    }

    async fn get_by_id(&self, _organization_id: u64, _project_id: u64) -> Result<Project> {
        Ok(Projects::project())
    }

    async fn get_by_token(&self, token: &str) -> Result<Project> {
        Ok(Projects::project())
    }

    async fn list(&self, _organization_id: u64) -> Result<ListResponse<Project>> {
        Ok(ListResponse {
            data: vec![Projects::project()],
            meta: ResponseMetadata { next: None },
        })
    }

    async fn update(
        &self,
        _organization_id: u64,
        _project_id: u64,
        _req: UpdateProjectRequest,
    ) -> Result<Project> {
        Ok(Projects::project())
    }

    async fn delete(&self, _organization_id: u64, _project_id: u64) -> Result<Project> {
        Ok(Projects::project())
    }
}

pub struct Teams {}

impl Teams {
    pub fn team() -> Team {
        Team {
            id: 1,
            created_at: *DATE_TIME,
            created_by: 1,
            updated_at: Some(*DATE_TIME),
            updated_by: Some(1),
            organization_id: 1,
            name: "name".to_string(),
        }
    }
}

#[async_trait]
impl teams::Provider for Teams {
    async fn create(&self, _organization_id: u64, _req: CreateTeamRequest) -> Result<Team> {
        Ok(Teams::team())
    }

    async fn get_by_id(&self, _organization_id: u64, _team_id: u64) -> Result<Team> {
        Ok(Teams::team())
    }

    async fn list(&self, _organization_id: u64) -> Result<ListResponse<Team>> {
        Ok(ListResponse {
            data: vec![Teams::team()],
            meta: ResponseMetadata { next: None },
        })
    }

    async fn update(
        &self,
        _organization_id: u64,
        _team_id: u64,
        _req: UpdateTeamRequest,
    ) -> Result<Team> {
        Ok(Teams::team())
    }

    async fn delete(&self, _organization_id: u64, _team_id: u64) -> Result<Team> {
        Ok(Teams::team())
    }
}
