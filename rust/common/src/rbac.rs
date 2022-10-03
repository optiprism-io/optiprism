use serde::{Deserialize, Serialize};
use lazy_static::lazy_static;

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub enum Permission {
    All,
    ManageAccounts,
    ViewAccounts,
    DeleteAccounts,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub enum Role {
    Admin,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub enum OrganizationPermission {
    ManageProjects,
    DeleteOrganization,
    All,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub enum OrganizationRole {
    Owner,
    Admin,
    Member,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub enum ProjectPermission {
    ManageProject,
    DeleteProject,
    ManageSchema,
    DeleteSchema,
    ViewSchema,
    InviteMembers,
    ManageMembers,
    ExploreReports,
    ManageReports,
    All,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
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
            ProjectPermission::ManageReports,
            ProjectPermission::ViewSchema,
            ProjectPermission::ManageSchema,
        ]),
        (ProjectRole::Member,vec![
            ProjectPermission::ExploreReports,
            ProjectPermission::ManageReports,
            ProjectPermission::ViewSchema,
            ProjectPermission::ManageSchema,
        ]),
        (ProjectRole::Reader,vec![
            ProjectPermission::ExploreReports,
            ProjectPermission::ViewSchema,
        ])
    ];
}