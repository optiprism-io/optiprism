use std::env::temp_dir;
use std::sync::Arc;

use common::event_segmentation::Analysis;
use common::event_segmentation::ChartType;
use common::event_segmentation::EventSegmentationRequest;
use common::group_col;
use common::query::EventRef;
use common::query::QueryTime;
use common::query::TimeIntervalUnit;
use metadata::accounts::CreateAccountRequest;
use metadata::custom_events::CreateCustomEventRequest;
use metadata::custom_events::Event;
use metadata::dashboards::CreateDashboardRequest;
use metadata::events::CreateEventRequest;
use metadata::organizations::CreateOrganizationRequest;
use metadata::projects::CreateProjectRequest;
use metadata::properties;
use metadata::properties::CreatePropertyRequest;
use metadata::reports;
use metadata::reports::CreateReportRequest;
use metadata::MetadataProvider;
use storage::db;
use storage::table;
use uuid::Uuid;

#[test]
fn test_list() {
    let mut path = temp_dir();
    path.push(format!("{}.db", Uuid::new_v4()));

    let db = Arc::new(metadata::rocksdb::new(path).unwrap());
    let opts = db::Options {};
    let mut path = temp_dir();
    path.push(format!("{}.db", Uuid::new_v4()));
    let opti_db = storage::db::OptiDBImpl::open(path, opts).unwrap();
    opti_db
        .create_table("events".to_string(), table::Options::test(false))
        .unwrap();
    opti_db
        .create_table(group_col(0), table::Options::test(false))
        .unwrap();
    let md = Box::new(MetadataProvider::try_new(db, Arc::new(opti_db)).unwrap());
    let event1 = md
        .events
        .create(1, CreateEventRequest {
            created_by: 0,
            tags: None,
            name: "e1".to_string(),
            display_name: None,
            description: None,
            status: Default::default(),
            is_system: false,
            event_properties: None,
            user_properties: None,
            custom_properties: None,
        })
        .unwrap();
    let event2 = md
        .events
        .create(2, CreateEventRequest {
            created_by: 0,
            tags: None,
            name: "e2".to_string(),
            display_name: None,
            description: None,
            status: Default::default(),
            is_system: false,
            event_properties: None,
            user_properties: None,
            custom_properties: None,
        })
        .unwrap();

    md.projects
        .create(CreateProjectRequest {
            created_by: 0,
            organization_id: 1,
            name: "p11sdfsdfsdfsdfsdfsdfsdfsdfsdddfsdfsdfsdfsdfdsfdsfsdf".to_string(),
            description: None,
            tags: None,
            token: "t1".to_string(),
            session_duration_seconds: 0,
        })
        .unwrap();
    md.projects
        .create(CreateProjectRequest {
            created_by: 0,
            organization_id: 2,
            name: "p2".to_string(),
            description: None,
            tags: None,
            token: "t2".to_string(),
            session_duration_seconds: 0,
        })
        .unwrap();

    md.event_properties
        .create(1, CreatePropertyRequest {
            created_by: 0,
            tags: None,
            name: "e1".to_string(),
            description: None,
            display_name: None,
            typ: properties::Type::Event,
            data_type: Default::default(),
            status: Default::default(),
            hidden: false,
            is_system: false,
            nullable: false,
            is_array: false,
            is_dictionary: false,
            dictionary_type: None,
        })
        .unwrap();

    md.event_properties
        .create(1, CreatePropertyRequest {
            created_by: 0,
            tags: None,
            name: "e2".to_string(),
            description: None,
            display_name: None,
            typ: properties::Type::Event,
            data_type: Default::default(),
            status: Default::default(),
            hidden: false,
            is_system: false,
            nullable: false,
            is_array: false,
            is_dictionary: false,
            dictionary_type: None,
        })
        .unwrap();

    md.group_properties[0]
        .create(1, CreatePropertyRequest {
            created_by: 0,
            tags: None,
            name: "g1".to_string(),
            description: None,
            display_name: None,
            typ: properties::Type::Group(0),
            data_type: Default::default(),
            status: Default::default(),
            hidden: false,
            is_system: false,
            nullable: false,
            is_array: false,
            is_dictionary: false,
            dictionary_type: None,
        })
        .unwrap();

    md.accounts
        .create(CreateAccountRequest {
            created_by: 0,
            password_hash: "".to_string(),
            email: "e1".to_string(),
            name: None,
            force_update_password: false,
            force_update_email: false,
            role: None,
            organizations: None,
            projects: None,
            teams: None,
        })
        .unwrap();

    md.accounts
        .create(CreateAccountRequest {
            created_by: 0,
            password_hash: "".to_string(),
            email: "e2".to_string(),
            name: None,
            force_update_password: false,
            force_update_email: false,
            role: None,
            organizations: None,
            projects: None,
            teams: None,
        })
        .unwrap();

    md.organizations
        .create(CreateOrganizationRequest {
            created_by: 1,
            name: "org1".to_string(),
        })
        .unwrap();
    md.organizations
        .create(CreateOrganizationRequest {
            created_by: 2,
            name: "org2".to_string(),
        })
        .unwrap();

    md.groups
        .get_or_create_group(1, "1".to_string(), "1".to_string())
        .unwrap();
    md.groups
        .get_or_create_group(1, "2".to_string(), "1".to_string())
        .unwrap();

    md.dashboards
        .create(1, CreateDashboardRequest {
            created_by: 0,
            tags: None,
            name: "d1".to_string(),
            description: None,
            panels: vec![],
        })
        .unwrap();

    md.dashboards
        .create(1, CreateDashboardRequest {
            created_by: 0,
            tags: None,
            name: "d2".to_string(),
            description: None,
            panels: vec![],
        })
        .unwrap();

    md.reports
        .create(1, CreateReportRequest {
            created_by: 0,
            tags: None,
            name: "r1".to_string(),
            description: None,
            typ: reports::Type::EventSegmentation,
            query: reports::Query::EventSegmentation(EventSegmentationRequest {
                time: QueryTime::Last {
                    last: 0,
                    unit: TimeIntervalUnit::Hour,
                },
                group_id: 0,
                interval_unit: TimeIntervalUnit::Hour,
                chart_type: ChartType::Line,
                analysis: Analysis::Linear,
                compare: None,
                events: vec![],
                filters: None,
                breakdowns: None,
                segments: None,
            }),
        })
        .unwrap();

    md.reports
        .create(1, CreateReportRequest {
            created_by: 0,
            tags: None,
            name: "r233".to_string(),
            description: None,
            typ: reports::Type::EventSegmentation,
            query: reports::Query::EventSegmentation(EventSegmentationRequest {
                time: QueryTime::Last {
                    last: 0,
                    unit: TimeIntervalUnit::Hour,
                },
                group_id: 0,
                interval_unit: TimeIntervalUnit::Hour,
                chart_type: ChartType::Line,
                analysis: Analysis::Linear,
                compare: None,
                events: vec![],
                filters: None,
                breakdowns: None,
                segments: None,
            }),
        })
        .unwrap();

    md.custom_events
        .create(1, CreateCustomEventRequest {
            created_by: 0,
            tags: None,
            name: "1".to_string(),
            description: None,
            status: Default::default(),
            is_system: false,
            events: vec![Event {
                event: EventRef::Regular(1),
                filters: None,
            }],
        })
        .unwrap();
    md.custom_events
        .create(1, CreateCustomEventRequest {
            created_by: 0,
            tags: None,
            name: "2".to_string(),
            description: None,
            status: Default::default(),
            is_system: false,
            events: vec![Event {
                event: EventRef::Regular(1),
                filters: None,
            }],
        })
        .unwrap();

    let l = md.events.list(1).unwrap();
    assert_eq!(l.len(), 1);
    assert_eq!(l.data[0].id, event1.id);
    let l = md.events.list(2).unwrap();
    assert_eq!(l.len(), 1);
    assert_eq!(l.data[0].id, event2.id);

    let l = md.projects.list().unwrap();
    assert_eq!(l.len(), 2);

    let l = md.event_properties.list(1).unwrap();
    assert_eq!(l.len(), 2);

    let l = md.group_properties[0].clone().list(1).unwrap();
    assert_eq!(l.len(), 1);

    let l = md.accounts.list().unwrap();
    assert_eq!(l.len(), 2);

    let l = md.organizations.list().unwrap();
    assert_eq!(l.len(), 2);

    let l = md.groups.list_groups(1).unwrap();
    assert_eq!(l.len(), 2);

    let l = md.dashboards.list(1).unwrap();
    assert_eq!(l.len(), 2);

    let l = md.reports.list(1).unwrap();
    assert_eq!(l.len(), 2);

    let l = md.custom_events.list(1).unwrap();
    assert_eq!(l.len(), 2);
}
