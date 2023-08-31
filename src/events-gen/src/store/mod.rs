use std::collections::HashMap;
use std::io;
use std::sync::Arc;

use arrow::record_batch::RecordBatch;
use chrono::DateTime;
use chrono::Utc;
use enum_iterator::all;
use metadata::MetadataProvider;
use rand::thread_rng;
use tracing::info;

use crate::error::Result;
use crate::generator;
use crate::generator::Generator;
use crate::profiles::ProfileProvider;
use crate::store::events::Event;
use crate::store::products::ProductProvider;
use crate::store::scenario::Scenario;
use crate::store::schema::create_entities;

pub mod actions;
mod batch_builder;
mod coefficients;
pub mod events;
mod intention;
pub mod products;
pub mod scenario;
pub mod schema;
mod transitions;
