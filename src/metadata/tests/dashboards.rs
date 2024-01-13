use std::env::temp_dir;
use std::sync::Arc;

use metadata::dashboards::CreateDashboardRequest;
use metadata::dashboards::Dashboards;
use metadata::dashboards::Provider;
use metadata::dashboards::UpdateDashboardRequest;
use metadata::error::Result;
use uuid::Uuid;

#[test]
fn test_dashboards() -> Result<()> {
    let mut path = temp_dir();
    path.push(format!("{}.db", Uuid::new_v4()));

    let db = Arc::new(metadata::rocksdb::new(path).unwrap());
    let dashboards: Box<dyn Provider> = Box::new(Dashboards::new(db.clone()));
    let create_dash_req = CreateDashboardRequest {
        created_by: 0,
        tags: None,
        name: "test".to_string(),
        description: None,
        panels: vec![],
    };

    let update_dash_req = UpdateDashboardRequest {
        updated_by: 0,
        tags: Default::default(),
        name: Default::default(),
        description: Default::default(),
        panels: Default::default(),
    };

    // try to get, delete, update unexisting event
    assert!(dashboards.get_by_id(1, 1, 1).is_err());
    assert!(dashboards.delete(1, 1, 1).is_err());
    // assert_eq!(events.list_events()?, vec![]);
    // create two events

    let mut create_dash1 = create_dash_req.clone();
    create_dash1.name = "dash1".to_string();
    let res = dashboards.create(1, 1, create_dash1.clone())?;
    assert_eq!(res.id, 1);
    let mut create_dash2 = create_dash_req.clone();
    create_dash2.name = "dash2".to_string();
    let res = dashboards.create(1, 1, create_dash2.clone())?;
    assert_eq!(res.id, 2);

    // check existence by id
    assert_eq!(dashboards.get_by_id(1, 1, 1)?.id, 1);
    assert_eq!(dashboards.get_by_id(1, 1, 2)?.id, 2);

    let mut update_dash1 = update_dash_req.clone();

    update_dash1.name.insert("d2".to_string());
    assert_eq!(
        dashboards.update(1, 1, 1, update_dash1.clone())?.name,
        "d2".to_string()
    );

    assert_eq!(dashboards.list(1, 1)?.data[0].id, 1);

    // delete events
    assert_eq!(dashboards.delete(1, 1, 1)?.id, 1);
    assert_eq!(dashboards.delete(1, 1, 2)?.id, 2);

    // events should gone now
    assert!(dashboards.get_by_id(1, 1, 1).is_err());
    assert!(dashboards.get_by_id(1, 1, 2).is_err());
    Ok(())
}
