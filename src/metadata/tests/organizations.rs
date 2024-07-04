use std::env::temp_dir;
use std::sync::Arc;

use common::types::OptionalProperty;
use metadata::error::Result;
use metadata::events::CreateEventRequest;
use metadata::events::Events;
use metadata::events::Status;
use metadata::events::UpdateEventRequest;
use uuid::Uuid;
use common::rbac::OrganizationRole;
use metadata::accounts::CreateAccountRequest;
use metadata::organizations::{CreateOrganizationRequest, Organizations};

#[test]
fn test_organizations() {
    let mut path = temp_dir();
    path.push(format!("{}.db", Uuid::new_v4()));

    let db = Arc::new(metadata::rocksdb::new(path).unwrap());
    let accs = Arc::new(metadata::accounts::Accounts::new(db.clone()));
    let orgs = Organizations::new(db.clone(), accs.clone());

    let req = CreateAccountRequest {
        created_by: 1,
        password_hash: "".to_string(),
        email: "e@mail.com".to_string(),
        name: None,
        force_update_password: false,
        force_update_email: false,
        role: None,
        organizations: None,
        projects: None,
        teams: None,
    };

    let acc = accs.create(req).unwrap();
    let req = CreateOrganizationRequest { created_by: 1, name: "org1".to_string() };
    let org = orgs.create(req).unwrap();

    orgs.add_member(org.id, acc.id, OrganizationRole::Owner).unwrap();

    let org = orgs.get_by_id(org.id).unwrap();
    let acc = accs.get_by_id(org.id).unwrap();

    assert_eq!(org.members, vec![acc.id]);
    assert_eq!(acc.organizations, Some(vec![(org.id, OrganizationRole::Owner)]));
}