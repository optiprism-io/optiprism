use std::sync::Arc;

use common::group_col;
use common::types::GROUP_COLUMN_CREATED_AT;
use common::types::GROUP_COLUMN_ID;
use common::types::GROUP_COLUMN_PROJECT_ID;
use common::types::GROUP_COLUMN_VERSION;
use metadata::MetadataProvider;
use storage::db::OptiDBImpl;
use storage::NamedValue;
use storage::Value;

use crate::error::Result;
use crate::Destination;
use crate::Identify;
use crate::RequestContext;

pub struct Local {
    db: Arc<OptiDBImpl>,
    md: Arc<MetadataProvider>,
}

impl Local {
    pub fn new(db: Arc<OptiDBImpl>, md: Arc<MetadataProvider>) -> Self {
        Self { db, md }
    }
}

impl Destination<Identify> for Local {
    fn send(&self, ctx: &RequestContext, req: Identify) -> Result<()> {
        let mut prop_values = vec![];

        prop_values.push(NamedValue::new(
            GROUP_COLUMN_PROJECT_ID.to_string(),
            Value::Int64(Some(ctx.project_id.unwrap() as i64)),
        ));
        prop_values.push(NamedValue::new(
            GROUP_COLUMN_ID.to_string(),
            Value::Int64(Some(req.resolved_group.clone().unwrap().id as i64)),
        ));
        let version = self
            .md
            .groups
            .next_record_sequence(ctx.project_id.unwrap(), req.group_id)?;

        prop_values.push(NamedValue::new(
            GROUP_COLUMN_VERSION.to_string(),
            Value::Int64(Some(version as i64)),
        ));

        prop_values.push(NamedValue::new(
            GROUP_COLUMN_CREATED_AT.to_string(),
            Value::Int64(Some(req.timestamp.timestamp())),
        ));

        for value in &req.resolved_group.unwrap().values {
            let prop = self.md.group_properties[req.group_id as usize]
                .get_by_id(ctx.project_id.unwrap(), value.property_id)?;
            let value = match value.value.clone() {
                metadata::groups::Value::Null => Value::Null,
                metadata::groups::Value::Int8(v) => Value::Int8(v),
                metadata::groups::Value::Int16(v) => Value::Int16(v),
                metadata::groups::Value::Int32(v) => Value::Int32(v),
                metadata::groups::Value::Int64(v) => Value::Int64(v),
                metadata::groups::Value::Boolean(v) => Value::Boolean(v),
                metadata::groups::Value::Timestamp(v) => Value::Timestamp(v),
                metadata::groups::Value::Decimal(v) => Value::Decimal(v),
                metadata::groups::Value::String(v) => Value::String(v),
                metadata::groups::Value::ListInt8(v) => Value::ListInt8(v),
                metadata::groups::Value::ListInt16(v) => Value::ListInt16(v),
                metadata::groups::Value::ListInt32(v) => Value::ListInt32(v),
                metadata::groups::Value::ListInt64(v) => Value::ListInt64(v),
                metadata::groups::Value::ListBoolean(v) => Value::ListBoolean(v),
                metadata::groups::Value::ListTimestamp(v) => Value::ListTimestamp(v),
                metadata::groups::Value::ListDecimal(v) => Value::ListDecimal(v),
                metadata::groups::Value::ListString(v) => Value::ListString(v),
            };
            prop_values.push(NamedValue::new(prop.column_name(), value));
        }

        self.db
            .insert(group_col(req.group_id as usize).as_str(), prop_values)?;
        Ok(())
    }
}
