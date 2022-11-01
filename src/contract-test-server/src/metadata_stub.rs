use async_trait::async_trait;
use common::rbac::OrganizationRole;
use common::rbac::ProjectRole;
use common::rbac::Role;
use common::types::DictionaryDataType;
use common::types::EventFilter;
use common::types::EventRef;
use common::types::PropValueOperation;
use common::types::PropertyRef;
use common::ScalarValue;
use datafusion::arrow::datatypes::DataType;
use metadata::accounts;
use metadata::accounts::Account;
use metadata::accounts::CreateAccountRequest;
use metadata::accounts::UpdateAccountRequest;
use metadata::custom_events;
use metadata::custom_events::CreateCustomEventRequest;
use metadata::custom_events::CustomEvent;
use metadata::custom_events::UpdateCustomEventRequest;
use metadata::database;
use metadata::database::Column;
use metadata::database::Table;
use metadata::database::TableRef;
use metadata::dictionaries;
use metadata::events;
use metadata::events::CreateEventRequest;
use metadata::events::Event;
use metadata::events::UpdateEventRequest;
use metadata::metadata::ListResponse;
use metadata::metadata::ResponseMetadata;
use metadata::organizations;
use metadata::organizations::CreateOrganizationRequest;
use metadata::organizations::Organization;
use metadata::organizations::UpdateOrganizationRequest;
use metadata::projects;
use metadata::projects::CreateProjectRequest;
use metadata::projects::Project;
use metadata::projects::UpdateProjectRequest;
use metadata::properties;
use metadata::properties::CreatePropertyRequest;
use metadata::properties::Property;
use metadata::properties::UpdatePropertyRequest;
use metadata::teams;
use metadata::teams::CreateTeamRequest;
use metadata::teams::Team;
use metadata::teams::UpdateTeamRequest;
use serde_json::Value;

use crate::DATE_TIME;

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
    async fn create(&self, req: CreateAccountRequest) -> metadata::Result<Account> {
        Ok(Accounts::account())
    }

    async fn get_by_id(&self, id: u64) -> metadata::Result<Account> {
        Ok(Accounts::account())
    }

    async fn get_by_email(&self, email: &str) -> metadata::Result<Account> {
        Ok(Accounts::account())
    }

    async fn list(&self) -> metadata::Result<ListResponse<Account>> {
        Ok(ListResponse {
            data: vec![Accounts::account()],
            meta: ResponseMetadata { next: None },
        })
    }

    async fn update(
        &self,
        account_id: u64,
        req: UpdateAccountRequest,
    ) -> metadata::Result<Account> {
        Ok(Self::account())
    }

    async fn delete(&self, id: u64) -> metadata::Result<Account> {
        Ok(Self::account())
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
        organization_id: u64,
        project_id: u64,
        req: CreateCustomEventRequest,
    ) -> metadata::Result<CustomEvent> {
        Ok(CustomEvents::custom_event())
    }

    async fn get_by_id(
        &self,
        organization_id: u64,
        project_id: u64,
        id: u64,
    ) -> metadata::Result<CustomEvent> {
        Ok(CustomEvents::custom_event())
    }

    async fn get_by_name(
        &self,
        organization_id: u64,
        project_id: u64,
        name: &str,
    ) -> metadata::Result<CustomEvent> {
        Ok(CustomEvents::custom_event())
    }

    async fn list(
        &self,
        organization_id: u64,
        project_id: u64,
    ) -> metadata::Result<ListResponse<CustomEvent>> {
        Ok(ListResponse {
            data: vec![CustomEvents::custom_event()],
            meta: ResponseMetadata { next: None },
        })
    }

    async fn update(
        &self,
        organization_id: u64,
        project_id: u64,
        event_id: u64,
        req: UpdateCustomEventRequest,
    ) -> metadata::Result<CustomEvent> {
        Ok(CustomEvents::custom_event())
    }

    async fn delete(
        &self,
        organization_id: u64,
        project_id: u64,
        id: u64,
    ) -> metadata::Result<CustomEvent> {
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
        organization_id: u64,
        project_id: u64,
        req: CreateEventRequest,
    ) -> metadata::Result<Event> {
        Ok(Events::event())
    }

    async fn get_or_create(
        &self,
        organization_id: u64,
        project_id: u64,
        req: CreateEventRequest,
    ) -> metadata::Result<Event> {
        Ok(Events::event())
    }

    async fn get_by_id(
        &self,
        organization_id: u64,
        project_id: u64,
        id: u64,
    ) -> metadata::Result<Event> {
        Ok(Events::event())
    }

    async fn get_by_name(
        &self,
        organization_id: u64,
        project_id: u64,
        name: &str,
    ) -> metadata::Result<Event> {
        Ok(Events::event())
    }

    async fn list(
        &self,
        organization_id: u64,
        project_id: u64,
    ) -> metadata::Result<ListResponse<Event>> {
        Ok(ListResponse {
            data: vec![Events::event()],
            meta: ResponseMetadata { next: None },
        })
    }

    async fn update(
        &self,
        organization_id: u64,
        project_id: u64,
        event_id: u64,
        req: UpdateEventRequest,
    ) -> metadata::Result<Event> {
        Ok(Events::event())
    }

    async fn attach_property(
        &self,
        organization_id: u64,
        project_id: u64,
        event_id: u64,
        prop_id: u64,
    ) -> metadata::Result<Event> {
        Ok(Events::event())
    }

    async fn detach_property(
        &self,
        organization_id: u64,
        project_id: u64,
        event_id: u64,
        prop_id: u64,
    ) -> metadata::Result<Event> {
        Ok(Events::event())
    }

    async fn delete(
        &self,
        organization_id: u64,
        project_id: u64,
        id: u64,
    ) -> metadata::Result<Event> {
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
        organization_id: u64,
        project_id: u64,
        req: CreatePropertyRequest,
    ) -> metadata::Result<Property> {
        Ok(Properties::property())
    }

    async fn get_or_create(
        &self,
        organization_id: u64,
        project_id: u64,
        req: CreatePropertyRequest,
    ) -> metadata::Result<Property> {
        Ok(Properties::property())
    }

    async fn get_by_id(
        &self,
        organization_id: u64,
        project_id: u64,
        id: u64,
    ) -> metadata::Result<Property> {
        Ok(Properties::property())
    }

    async fn get_by_name(
        &self,
        organization_id: u64,
        project_id: u64,
        name: &str,
    ) -> metadata::Result<Property> {
        Ok(Properties::property())
    }

    async fn list(
        &self,
        organization_id: u64,
        project_id: u64,
    ) -> metadata::Result<ListResponse<Property>> {
        Ok(ListResponse {
            data: vec![Properties::property()],
            meta: ResponseMetadata { next: None },
        })
    }

    async fn update(
        &self,
        organization_id: u64,
        project_id: u64,
        property_id: u64,
        req: UpdatePropertyRequest,
    ) -> metadata::Result<Property> {
        Ok(Properties::property())
    }

    async fn delete(
        &self,
        organization_id: u64,
        project_id: u64,
        id: u64,
    ) -> metadata::Result<Property> {
        Ok(Properties::property())
    }
}

pub struct Database {}
#[async_trait]
impl database::Provider for Database {
    async fn create_table(&self, table: Table) -> metadata::Result<()> {
        Ok(())
    }

    async fn get_table(&self, table_type: TableRef) -> metadata::Result<Table> {
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

    async fn add_column(&self, table_type: TableRef, col: Column) -> metadata::Result<()> {
        Ok(())
    }
}

pub struct Dictionaries {}
#[async_trait]
impl dictionaries::Provider for Dictionaries {
    async fn get_key_or_create(
        &self,
        organization_id: u64,
        project_id: u64,
        dict: &str,
        value: &str,
    ) -> metadata::Result<u64> {
        Ok(1)
    }

    async fn get_value(
        &self,
        organization_id: u64,
        project_id: u64,
        dict: &str,
        key: u64,
    ) -> metadata::Result<String> {
        Ok("v".to_string())
    }

    async fn get_key(
        &self,
        _organization_id: u64,
        _project_id: u64,
        _dict: &str,
        value: &str,
    ) -> metadata::Result<u64> {
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
    async fn create(&self, req: CreateOrganizationRequest) -> metadata::Result<Organization> {
        Ok(Organizations::org())
    }

    async fn get_by_id(&self, id: u64) -> metadata::Result<Organization> {
        Ok(Organizations::org())
    }

    async fn list(&self) -> metadata::Result<ListResponse<Organization>> {
        Ok(ListResponse {
            data: vec![Organizations::org()],
            meta: ResponseMetadata { next: None },
        })
    }

    async fn update(
        &self,
        org_id: u64,
        req: UpdateOrganizationRequest,
    ) -> metadata::Result<Organization> {
        Ok(Organizations::org())
    }

    async fn delete(&self, id: u64) -> metadata::Result<Organization> {
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
        }
    }
}
#[async_trait]
impl projects::Provider for Projects {
    async fn create(
        &self,
        organization_id: u64,
        req: CreateProjectRequest,
    ) -> metadata::Result<Project> {
        Ok(Projects::project())
    }

    async fn get_by_id(&self, organization_id: u64, project_id: u64) -> metadata::Result<Project> {
        Ok(Projects::project())
    }

    async fn list(&self, organization_id: u64) -> metadata::Result<ListResponse<Project>> {
        Ok(ListResponse {
            data: vec![Projects::project()],
            meta: ResponseMetadata { next: None },
        })
    }

    async fn update(
        &self,
        organization_id: u64,
        project_id: u64,
        req: UpdateProjectRequest,
    ) -> metadata::Result<Project> {
        Ok(Projects::project())
    }

    async fn delete(&self, organization_id: u64, project_id: u64) -> metadata::Result<Project> {
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
    async fn create(&self, organization_id: u64, req: CreateTeamRequest) -> metadata::Result<Team> {
        Ok(Teams::team())
    }

    async fn get_by_id(&self, organization_id: u64, team_id: u64) -> metadata::Result<Team> {
        Ok(Teams::team())
    }

    async fn list(&self, organization_id: u64) -> metadata::Result<ListResponse<Team>> {
        Ok(ListResponse {
            data: vec![Teams::team()],
            meta: ResponseMetadata { next: None },
        })
    }

    async fn update(
        &self,
        organization_id: u64,
        team_id: u64,
        req: UpdateTeamRequest,
    ) -> metadata::Result<Team> {
        Ok(Teams::team())
    }

    async fn delete(&self, organization_id: u64, team_id: u64) -> metadata::Result<Team> {
        Ok(Teams::team())
    }
}
