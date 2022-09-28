use serde::{Deserialize, Serialize};
use lazy_static::lazy_static;

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub enum Permission {
    All,
    ManageAccounts,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub enum Role {
    Admin,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub enum OrganizationPermission {
    ManageProjects,
    DeleteOrganization,
    All,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub enum OrganizationRole {
    Owner,
    Admin,
    Member,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub enum ProjectPermission {
    ManageProject,
    DeleteProject,
    InviteMembers,
    ManageMembers,
    ExploreReports,
    ManageReports,
    All,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub enum ProjectRole {
    Owner,
    Admin,
    Member,
    Reader,
}

lazy_static! {
    pub static ref PERMISSIONS:Vec<(Role,Vec<Permission>)> = vec![
        (Role::Admin,vec![
            Permission::All
        ]),
    ];

    pub static ref ORGANIZATION_PERMISSIONS:Vec<(OrganizationRole,Vec<OrganizationPermission>)> = vec![
        (OrganizationRole::Owner,vec![
            OrganizationPermission::All
        ]),
        (OrganizationRole::Admin,vec![
            OrganizationPermission::ManageProjects
        ])
    ];

        pub static ref PROJECT_PERMISSIONS:Vec<(ProjectRole,Vec<ProjectPermission>)> = vec![
        (ProjectRole::Owner,vec![
            ProjectPermission::All
        ]),
        (ProjectRole::Admin,vec![
            ProjectPermission::ManageProject,
            ProjectPermission::InviteMembers,
            ProjectPermission::ManageMembers,
            ProjectPermission::ExploreReports,
            ProjectPermission::ManageReports
        ]),
        (ProjectRole::Member,vec![
            ProjectPermission::ExploreReports,
            ProjectPermission::ManageReports
        ]),
        (ProjectRole::Reader,vec![
            ProjectPermission::ExploreReports,
        ])
    ];
}