use std::collections::HashMap;
use std::sync::Arc;

use lazy_static::lazy_static;
use metadata::MetadataProvider;
use serde::Deserialize;
use serde::Serialize;

use crate::PlatformError;
use crate::Result;

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub enum Permission {
    All,
    ManageAccounts,
    ViewAccounts,
    ManageOrganizations,
    ViewOrganizations,
    ManageProjects,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub enum Role {
    Admin,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub enum OrganizationPermission {
    ManageProjects,
    ExploreProjects,
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

pub struct RBAC {
    accounts: HashMap<u64, Account>,
    md: Arc<MetadataProvider>,
}

struct Account {
    id: u64,
    role: Option<Role>,
    organizations: Option<Vec<(u64, OrganizationRole)>>,
    projects: Option<Vec<(u64, ProjectRole)>>,
    teams: Option<Vec<(u64, Role)>>,
}

impl RBAC {
    pub fn new(md: Arc<MetadataProvider>) -> Self {
        Self {
            accounts: HashMap::new(),
            md,
        }
    }

    pub fn add_account(&mut self, account: Account) {
        self.accounts.insert(account.id, account);
    }

    pub fn get_account(&self, account_id: u64) -> Result<&Account> {
        self.accounts
            .get(&account_id)
            .ok_or_else(|| PlatformError::Forbidden("forbidden".to_string()))
    }

    pub fn check_permission(&self, account_id: u64, permission: Permission) -> Result<()> {
        let acc = self.get_account(account_id)?;
        if let Some(role) = &acc.role {
            for (root_role, role_permissions) in PERMISSIONS.iter() {
                if *root_role != *role {
                    continue;
                }
                if role_permissions.contains(&Permission::All) {
                    return Ok(());
                }
                if role_permissions.contains(&permission) {
                    return Ok(());
                }
            }
        }

        Err(PlatformError::Forbidden("forbidden".to_string()))
    }

    pub fn check_project_permission(
        &self,
        account_id: u64,
        project_id: u64,
        permission: ProjectPermission,
    ) -> Result<()> {
        let acc = self.get_account(account_id)?;
        if self
            .check_permission(acc.id, Permission::ManageProjects)
            .is_ok()
        {
            return Ok(());
        }
        let organization_id = self.md.projects.get_by_id(project_id)?.organization_id;

        if let Ok(role) = self.get_organization_role(acc, organization_id) {
            match role {
                OrganizationRole::Owner => return Ok(()),
                OrganizationRole::Admin => return Ok(()),
                OrganizationRole::Member => {}
            }
        }

        let role = self.get_project_role(acc, project_id)?;

        for (proj_role, role_permission) in PROJECT_PERMISSIONS.iter() {
            if *proj_role != role {
                continue;
            }

            if role_permission.contains(&ProjectPermission::All) {
                return Ok(());
            }
            if role_permission.contains(&permission) {
                return Ok(());
            }
        }

        Err(PlatformError::Forbidden("forbidden".to_string()))
    }

    pub fn check_organization_permission(
        &self,
        account_id: u64,
        organization_id: u64,
        permission: OrganizationPermission,
    ) -> Result<()> {
        let acc = self.get_account(account_id)?;
        if self
            .check_permission(account_id, Permission::ManageOrganizations)
            .is_ok()
        {
            return Ok(());
        }
        let role = self.get_organization_role(acc, organization_id)?;
        for (org_role, role_permission) in ORGANIZATION_PERMISSIONS.iter() {
            if *org_role != role {
                continue;
            }

            if role_permission.contains(&OrganizationPermission::All) {
                return Ok(());
            }
            if role_permission.contains(&permission) {
                return Ok(());
            }
        }

        Err(PlatformError::Forbidden("forbidden".to_string()))
    }

    fn get_organization_role(
        &self,
        acc: &Account,
        organization_id: u64,
    ) -> Result<OrganizationRole> {
        if let Some(organizations) = &acc.organizations {
            for (org_id, role) in organizations.iter() {
                if *org_id == organization_id {
                    return Ok(role.to_owned());
                }
            }
        }

        Err(PlatformError::Forbidden("forbidden".to_string()))
    }

    fn get_project_role(&self, acc: &Account, project_id: u64) -> Result<ProjectRole> {
        if let Some(projects) = &acc.projects {
            for (proj_id, role) in projects.iter() {
                if *proj_id == project_id {
                    return Ok(role.to_owned());
                }
            }
        }

        Err(PlatformError::Forbidden("forbidden".to_string()))
    }
}

#[cfg(test)]
mod tests {
    use std::env::temp_dir;
    use std::sync::Arc;

    use metadata::MetadataProvider;
    use storage::db::OptiDBImpl;
    use storage::db::Options;
    use uuid::Uuid;

    use crate::rbac::RBAC;

    #[test]
    fn test_rbac() {
        let mut path = temp_dir();
        path.push(format!("{}", Uuid::new_v4()));
        let rocks = Arc::new(metadata::rocksdb::new(path.join("md")).unwrap());
        let db = Arc::new(OptiDBImpl::open(path.join("store"), Options {}).unwrap());
        let md = Arc::new(MetadataProvider::try_new(rocks, db.clone()).unwrap());

        let rbac = RBAC::new(md);
    }
}
