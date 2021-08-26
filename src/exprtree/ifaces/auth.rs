use crate::exprtree::error::{Error, Result};

enum Role {
    Admin,
    Owner,
    Reader,
}

enum Permission {
    List,
}

struct User {
    id: u64,
    username: String,
    password_hash: String,
}

struct UserPermissions {
    user_id: u64,
    roles: Vec<Role>,
    permissions: Vec<Permission>,
}

trait Auth {
    fn obtain_user_token(&self, username: String, password_hash: String) -> Result<()>;
}

trait UserProvider {
    fn create_user(&mut self, user: &User) -> Result<User>;
    fn update_user(&mut self, user: &User) -> Result<User>;
    fn delete_user(&mut self, id: u64) -> Result<()>;
    fn get_user_by_id(&self, id: u64) -> Result<User>;
    fn list_users(&self) -> Result<Vec<User>>;
}

struct Team {
    id: u64,
    name: String,
    users: Vec<User>,
}

trait TeamProvider {
    fn create_team(&mut self, team: &Team) -> Result<Team>;
    fn update_team(&mut self, team: &Team) -> Result<Team>;
    fn delete_team(&mut self, id: u64) -> Result<()>;
    fn get_team_by_id(&self, id: u64) -> Result<Team>;
    fn list_teams(&self) -> Result<Vec<Team>>;
}

struct Project {
    id: u64,
    name: String,
    description: String,
    users: Vec<UserPermissions>,
}

trait ProjectProvider {
    fn create_project(&mut self, project: &Project) -> Result<Project>;
    fn update_project(&mut self, project: &Project) -> Result<Project>;
    fn delete_project(&mut self, id: u64) -> Result<()>;
    fn get_project_by_id(&self, id: u64) -> Result<Project>;
    fn list_projects(&self) -> Result<Vec<Project>>;
}

struct Organization {
    users: Vec<UserPermissions>,
    projects: Vec<u64>,
    teams: Vec<u64>,
}

trait OrganizationProvider {
    fn create_organization(&mut self, organization: &Organization) -> Result<Organization>;
    fn update_organization(&mut self, organization: &Organization) -> Result<Organization>;
    fn delete_organization(&mut self, id: u64) -> Result<()>;
    fn get_organization_by_id(&self, id: u64) -> Result<Organization>;
    fn list_organizations(&self) -> Result<Vec<Organization>>;
}
