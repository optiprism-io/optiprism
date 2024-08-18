use lazy_static::lazy_static;
use serde::Deserialize;
use serde::Serialize;

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub enum Permission {
    All,
    ManageAccounts,
    ViewAccounts,
    ManageOrganizations,
    ViewOrganizations,
    ManageProjects,
    ManageServer,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub enum Role {
    Admin = 1,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub enum OrganizationPermission {
    ViewOrganization = 1,
    ManageOrganization = 2,
    ManageProjects = 3,
    ExploreProjects = 4,
    DeleteOrganization = 5,
    All,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub enum OrganizationRole {
    Owner = 1,
    Admin = 2,
    Member = 3,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub enum ProjectPermission {
    ManageProject = 1,
    DeleteProject = 2,
    ViewProject = 3,
    ManageSchema = 4,
    DeleteSchema = 5,
    ViewSchema = 6,
    InviteMembers = 7,
    ManageMembers = 8,
    ExploreReports = 9,
    ManageReports = 10,
    All = 11,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub enum ProjectRole {
    Owner,
    Admin,
    Member,
    Reader,
}

lazy_static! {
    pub static ref PERMISSIONS: Vec<(Role, Vec<Permission>)> =
        vec![(Role::Admin, vec![Permission::All]),];
    pub static ref ORGANIZATION_PERMISSIONS: Vec<(OrganizationRole, Vec<OrganizationPermission>)> = vec![
        (OrganizationRole::Owner, vec![OrganizationPermission::All]),
        (OrganizationRole::Admin, vec![
            OrganizationPermission::ManageProjects
        ])
    ];
    pub static ref PROJECT_PERMISSIONS: Vec<(ProjectRole, Vec<ProjectPermission>)> = vec![
        (ProjectRole::Owner, vec![ProjectPermission::All]),
        (ProjectRole::Admin, vec![
            ProjectPermission::ManageProject,
            ProjectPermission::InviteMembers,
            ProjectPermission::ManageMembers,
            ProjectPermission::ExploreReports,
            ProjectPermission::ManageReports,
            ProjectPermission::ViewSchema,
            ProjectPermission::ManageSchema,
        ]),
        (ProjectRole::Member, vec![
            ProjectPermission::ExploreReports,
            ProjectPermission::ManageReports,
            ProjectPermission::ViewSchema,
            ProjectPermission::ManageSchema,
        ]),
        (ProjectRole::Reader, vec![
            ProjectPermission::ExploreReports,
            ProjectPermission::ViewSchema,
        ])
    ];
}
