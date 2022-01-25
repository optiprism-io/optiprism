use std::any::Any;
use crate::error::Error;
use crate::store::store::{make_data_key, make_id_seq_key, make_index_key, Store};
use crate::Result;

use crate::events::types::{CreateEventRequest, IndexValues, UpdateEventRequest};
use crate::events::{Event, Status};
use crate::store::index;
use bincode::{deserialize, serialize};
use chrono::Utc;
use std::sync::Arc;
use arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use datafusion::datasource::{TableProvider, TableType};
use datafusion::datasource::datasource::TableProviderFilterPushDown;
use datafusion::logical_plan::Expr;
use datafusion::physical_plan::ExecutionPlan;
use crate::events::memory::RocksDBTable;

const NAMESPACE: &[u8] = b"events";
const IDX_NAME: &[u8] = b"name";
const IDX_DISPLAY_NAME: &[u8] = b"display_name";

fn index_keys(values: Box<&dyn IndexValues>) -> Vec<Option<Vec<u8>>> {
    if let Status::Disabled = values.status() {
        return vec![None, None];
    }
    if let Some(display_name) = values.display_name() {
        vec![
            Some(make_index_key(NAMESPACE, values.project_id(), IDX_NAME, values.name()).to_vec()),
            Some(
                make_index_key(
                    NAMESPACE,
                    values.project_id(),
                    IDX_DISPLAY_NAME,
                    display_name,
                )
                    .to_vec(),
            ),
        ]
    } else {
        vec![
            Some(make_index_key(NAMESPACE, values.project_id(), IDX_NAME, values.name()).to_vec()),
            None,
        ]
    }
}

pub struct Provider {
    store: Arc<Store>,
    idx: index::hash_map::HashMap,
}

impl Provider {
    pub fn new(kv: Arc<Store>) -> Self {
        Provider {
            store: kv.clone(),
            idx: index::hash_map::HashMap::new(kv),
        }
    }

    pub async fn create(&mut self, req: CreateEventRequest) -> Result<Event> {
        let idx_keys = index_keys(Box::new(&req));
        self.idx.check_insert_constraints(idx_keys.as_ref()).await?;

        let created_at = Utc::now();
        let id = self
            .store
            .next_seq(make_id_seq_key(NAMESPACE, req.project_id))
            .await?;

        let event = req.into_event(id, created_at);
        let data = serialize(&event)?;
        self.store
            .put(
                make_data_key(NAMESPACE, event.project_id, event.id),
                &data,
            )
            .await?;

        self.idx
            .insert(idx_keys.as_ref(), &data)
            .await?;
        Ok(event)
    }

    pub async fn get_by_id(&self, project_id: u64, id: u64) -> Result<Event> {
        match self
            .store
            .get(make_data_key(NAMESPACE, project_id, id))
            .await?
        {
            None => Err(Error::EventDoesNotExist),
            Some(value) => Ok(deserialize(&value)?),
        }
    }

    pub async fn get_by_name(&self, project_id: u64, name: &str) -> Result<Event> {
        let data = self
            .idx
            .get(make_index_key(NAMESPACE, project_id, IDX_NAME, name))
            .await?;

        Ok(deserialize(&data)?)
    }

    pub async fn list(&self) -> Result<Vec<Event>> {
        let list = self
            .store
            .list_prefix(b"/events/ent") // TODO doesn't work
            .await?
            .iter()
            .map(|v| deserialize(v.1.as_ref()))
            .collect::<bincode::Result<_>>()?;

        Ok(list)
    }

    pub async fn update(&mut self, req: UpdateEventRequest) -> Result<Event> {
        let idx_keys = index_keys(Box::new(&req));
        let prev_event = self.get_by_id(req.project_id, req.id).await?;
        let idx_prev_keys = index_keys(Box::new(&prev_event));
        self.idx
            .check_update_constraints(idx_keys.as_ref(), idx_prev_keys.as_ref())
            .await?;

        let updated_at = Utc::now(); // TODO add updated_by
        let event = req.into_event(prev_event, updated_at, None);
        let data = serialize(&event)?;
        self.store
            .put(
                make_data_key(NAMESPACE, event.project_id, event.id),
                &data,
            )
            .await?;

        self.idx
            .update(
                idx_keys.as_ref(),
                idx_prev_keys.as_ref(),
                &data,
            )
            .await?;
        Ok(event)
    }

    pub async fn attach_property(
        &mut self,
        project_id: u64,
        event_id: u64,
        prop_id: u64,
    ) -> Result<()> {
        let mut event = self.get_by_id(project_id, event_id).await?;
        event.properties = match event.properties {
            None => Some(vec![prop_id]),
            Some(props) => match props.iter().find(|x| prop_id == **x) {
                None => Some([props, vec![prop_id]].concat()),
                Some(_) => return Err(Error::EventAlreadyHasGlobalProperty),
            },
        };

        self.store
            .put(
                make_data_key(NAMESPACE, event.project_id, event.id),
                serialize(&event)?,
            )
            .await?;
        Ok(())
    }

    pub async fn detach_property(
        &mut self,
        project_id: u64,
        event_id: u64,
        prop_id: u64,
    ) -> Result<()> {
        let mut event = self.get_by_id(project_id, event_id).await?;
        event.properties = match event.properties {
            None => return Err(Error::EventDoesntHaveGlobalProperty),
            Some(props) => match props.iter().find(|x| prop_id == **x) {
                None => return Err(Error::EventDoesntHaveGlobalProperty),
                Some(_) => Some(props.into_iter().filter(|x| prop_id != *x).collect()),
            },
        };

        self.store
            .put(
                make_data_key(NAMESPACE, event.project_id, event.id),
                serialize(&event)?,
            )
            .await?;
        Ok(())
    }

    pub async fn delete(&mut self, project_id: u64, id: u64) -> Result<Event> {
        let event = self.get_by_id(project_id, id).await?;
        self.store
            .delete(make_data_key(NAMESPACE, project_id, id))
            .await?;

        self.idx
            .delete(index_keys(Box::new(&event)).as_ref())
            .await?;
        Ok(event)
    }
}

impl TableProvider for Provider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        let fields = vec![
            Field::new("id", DataType::UInt64, false),
            Field::new("created_at", DataType::Timestamp(TimeUnit::Second, None), false),
            Field::new("updated_at", DataType::Timestamp(TimeUnit::Second, None), true),
            Field::new("created_by", DataType::UInt64, false),
            Field::new("updated_by", DataType::UInt64, true),
            Field::new("project_id", DataType::UInt64, false),
            Field::new("tags", DataType::List(Box::new(Field::new("tag", DataType::Utf8, false))), true),
            Field::new("name", DataType::Utf8, false),
            Field::new("display_name", DataType::Utf8, true),
            Field::new("description", DataType::Utf8, true),
            Field::new("status", DataType::Utf8, false),
            Field::new("scope", DataType::Utf8, false),
            Field::new("properties", DataType::List(Box::new(Field::new("property_id", DataType::UInt64, false))), true),
            Field::new("custom_properties", DataType::List(Box::new(Field::new("property_id", DataType::UInt64, false))), true),
        ];

        Arc::new(Schema::new(fields))
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(&self,
                  projection: &Option<Vec<usize>>,
                  _batch_size: usize,
                  _filters: &[Expr],
                  limit: Option<usize>,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        self.store.list_prefix()
    }

    fn supports_filter_pushdown(&self, _filter: &Expr) -> datafusion::error::Result<TableProviderFilterPushDown> {
        Ok(TableProviderFilterPushDown::Unsupported)
    }
}
