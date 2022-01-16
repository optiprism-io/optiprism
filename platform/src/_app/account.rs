use super::{
    auth::{make_password_hash, make_salt},
    context::Context,
    entity_utils::List,
    error::{Result, ERR_ACCOUNT_CREATE_CONFLICT, ERR_ACCOUNT_NOT_FOUND, ERR_TODO},
    rbac::{Permission, Role, Scope},
    sequence::Sequence,
};
use bincode::{deserialize, serialize};
use chrono::{DateTime, Utc};
use parking_lot::Mutex;
use rocksdb::{ColumnFamily, IteratorMode, DB};
use serde::{Deserialize, Serialize};
use std::{collections::HashMap, convert::TryInto, rc::Rc, sync::Arc};

pub const PRIMARY_CF: &str = "account";
pub const SECONDARY_CF: &str = "account_sec";
const SEQUENCE_KEY: &'static [u8] = b"_id";

#[derive(Serialize, Deserialize)]
pub struct Account {
    pub id: u64,
    pub created_at: DateTime<Utc>,
    pub updated_at: Option<DateTime<Utc>>,
    pub admin: bool,
    pub salt: String,
    pub password: String,
    pub organization_id: u64,
    pub email: String,
    pub roles: Option<HashMap<Scope, Role>>,
    pub permissions: Option<HashMap<Scope, Vec<Permission>>>,
    pub first_name: Option<String>,
    pub middle_name: Option<String>,
    pub last_name: Option<String>,
    // TODO: add account fields
}

#[derive(Deserialize)]
pub struct CreateRequest {
    pub admin: bool,
    pub password: String,
    pub organization_id: u64,
    pub email: String,
    pub roles: Option<HashMap<Scope, Role>>,
    pub permissions: Option<HashMap<Scope, Vec<Permission>>>,
    pub first_name: Option<String>,
    pub middle_name: Option<String>,
    pub last_name: Option<String>,
    // TODO: add create account fields
}

#[derive(Deserialize)]
pub struct UpdateRequest {
    pub id: u64,
    pub first_name: Option<String>,
    pub middle_name: Option<String>,
    pub last_name: Option<String>,
    // TODO: add update account fields
}

pub struct Provider {
    db: Arc<DB>,
    primary_cf: Arc<ColumnFamily>,
    secondary_cf: Arc<ColumnFamily>,
    sequence: Sequence,
    create_guard: Mutex<()>,
}

unsafe impl Send for Provider {}
unsafe impl Sync for Provider {}

impl Provider {
    pub fn new(db: Arc<DB>) -> Result<Self> {
        let bcf = match db.cf_handle(PRIMARY_CF) {
            Some(bcf) => bcf,
            None => return Err(ERR_TODO.into()),
        };
        let primary_cf: Arc<ColumnFamily> = unsafe { std::mem::transmute(bcf) };
        let bcf = match db.cf_handle(SECONDARY_CF) {
            Some(bcf) => bcf,
            None => return Err(ERR_TODO.into()),
        };
        let secondary_cf: Arc<ColumnFamily> = unsafe { std::mem::transmute(bcf) };
        let sequence = Sequence::new(db.clone(), secondary_cf.clone(), SEQUENCE_KEY);
        Ok(Self {
            db,
            primary_cf,
            secondary_cf,
            sequence,
            create_guard: Mutex::new(()),
        })
    }

    pub fn create(&self, ctx: Rc<Context>, request: CreateRequest) -> Result<Account> {
        if !ctx.is_permitted(request.organization_id, 0, Permission::AccountCreate) {
            return Err(ERR_TODO.into());
        }
        let id = self.sequence.next()?;
        let salt = make_salt();
        let password = make_password_hash(&request.password, &salt);
        let acc = Account {
            id,
            created_at: Utc::now(),
            updated_at: None,
            admin: request.admin,
            salt,
            password,
            organization_id: request.organization_id,
            email: request.email,
            roles: request.roles,
            permissions: request.permissions,
            first_name: request.first_name,
            middle_name: request.middle_name,
            last_name: request.last_name,
        };
        {
            let _guard = self.create_guard.lock();
            match self
                .db
                .get_cf(self.secondary_cf.as_ref(), acc.email.as_bytes())
            {
                Ok(value) => {
                    if value.is_some() {
                        return Err(ERR_ACCOUNT_CREATE_CONFLICT.into());
                    }
                }
                Err(_) => return Err(ERR_TODO.into()),
            }
            let result = self.db.put_cf(
                self.primary_cf.as_ref(),
                id.to_le_bytes().as_ref(),
                match serialize(&acc) {
                    Ok(value) => value,
                    Err(_) => return Err(ERR_TODO.into()),
                },
            );
            if let Err(_) = result {
                return Err(ERR_TODO.into());
            }
            let result = self.db.put_cf(
                self.secondary_cf.as_ref(),
                acc.email.as_bytes(),
                id.to_le_bytes(),
            );
            if let Err(_) = result {
                return Err(ERR_TODO.into());
            }
        }
        Ok(acc)
    }

    pub fn get_by_id(&self, ctx: Rc<Context>, id: u64) -> Result<Account> {
        let acc: Account = match self
            .db
            .get_cf(self.primary_cf.as_ref(), id.to_le_bytes().as_ref())
        {
            Ok(value) => match value {
                Some(value) => match deserialize(&value) {
                    Ok(value) => value,
                    Err(_) => return Err(ERR_TODO.into()),
                },
                None => return Err(ERR_ACCOUNT_NOT_FOUND.into()),
            },
            Err(_) => return Err(ERR_TODO.into()),
        };
        if acc.id != ctx.account_id
            && !ctx.is_permitted(acc.organization_id, 0, Permission::AccountGetById)
        {
            return Err(ERR_TODO.into());
        }
        Ok(acc)
    }

    pub fn get_by_email(&self, ctx: Rc<Context>, email: String) -> Result<Account> {
        let value = match self.db.get_cf(self.secondary_cf.as_ref(), email.as_bytes()) {
            Ok(value) => match value {
                Some(value) => value,
                None => return Err(ERR_ACCOUNT_NOT_FOUND.into()),
            },
            Err(_) => return Err(ERR_TODO.into()),
        };
        let id = u64::from_le_bytes(match value.try_into() {
            Ok(value) => value,
            Err(_) => return Err(ERR_TODO.into()),
        });
        self.get_by_id(ctx, id)
    }

    pub fn list(&self, ctx: Rc<Context>) -> Result<List<Account>> {
        // TODO: add filter
        let mut list = List {
            data: Vec::new(),
            total: 0,
        };
        for (_, value) in self
            .db
            .iterator_cf(self.primary_cf.as_ref(), IteratorMode::Start)
        {
            list.total += 1;
            list.data.push(match deserialize(&value) {
                Ok(value) => value,
                Err(_) => return Err(ERR_TODO.into()),
            });
        }
        Ok(list)
    }

    pub fn update(&self, ctx: Rc<Context>, request: UpdateRequest) -> Result<Account> {
        let mut acc = self.get_by_id(ctx, request.id)?;
        let mut updated = false;
        if let Some(value) = &request.first_name {
            updated = true;
            acc.first_name = if value.is_empty() {
                None
            } else {
                Some(value.to_string())
            };
        }
        if let Some(value) = &request.middle_name {
            updated = true;
            acc.middle_name = if value.is_empty() {
                None
            } else {
                Some(value.to_string())
            };
        }
        if let Some(value) = &request.last_name {
            updated = true;
            acc.last_name = if value.is_empty() {
                None
            } else {
                Some(value.to_string())
            };
        }
        if updated {
            let result = self.db.put_cf(
                self.primary_cf.as_ref(),
                acc.id.to_le_bytes().as_ref(),
                match serialize(&acc) {
                    Ok(value) => value,
                    Err(_) => return Err(ERR_TODO.into()),
                },
            );
            if let Err(_) = result {
                return Err(ERR_TODO.into());
            }
        }
        Ok(acc)
    }

    pub fn delete(&self, ctx: Rc<Context>, id: u64) -> Result<()> {
        let result = self
            .db
            .delete_cf(self.primary_cf.as_ref(), id.to_le_bytes().as_ref());
        if let Err(_) = result {
            return Err(ERR_TODO.into());
        }
        Ok(())
    }
}
