use metadata::store::Store;
use metadata::{
    organizations::{CreateOrganizationRequest, ListRequest},
    Metadata, Result,
};
use std::{env::temp_dir, sync::Arc};
use uuid::Uuid;

#[tokio::test]
async fn test_organizations() -> Result<()> {
    let mut path = temp_dir();
    path.push(format!("{}.db", Uuid::new_v4()));

    let store = Arc::new(Store::new(path));
    let metadata = Metadata::try_new(store.clone())?;

    let _ = metadata
        .organizations
        .create(CreateOrganizationRequest {
            created_by: 0,
            name: "organization1".into(),
        })
        .await
        .unwrap();
    let _ = metadata
        .organizations
        .create(CreateOrganizationRequest {
            created_by: 0,
            name: "organization2".into(),
        })
        .await
        .unwrap();

    let list = metadata
        .organizations
        .list()
        .await
        .unwrap();

    dbg!(list);

    // assert_eq!(md.organizations.list_organizations().await?[0].id, 1);
    // assert_eq!(md.organizations.list_organizations().await?[1].id, 2);

    Ok(())
}
