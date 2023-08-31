use std::collections::HashMap;
use std::io;
use std::sync::Arc;

use arrow::record_batch::RecordBatch;
use chrono::DateTime;
use chrono::Utc;
use enum_iterator::all;
use events_gen::generator;
use events_gen::generator::Generator;
use events_gen::profiles::ProfileProvider;
use metadata::MetadataProvider;
use rand::thread_rng;
use tracing::info;

use crate::error::Result;
use crate::generator;
use crate::generator::Generator;
use crate::profiles::ProfileProvider;
use crate::store_dictionary::events::Event;
use crate::store_dictionary::products::ProductProvider;
use crate::store_dictionary::scenario::Scenario;
use crate::store_dictionary::schema::create_entities;

pub mod actions;
mod batch_builder;
mod coefficients;
pub mod events;
mod intention;
pub mod products;
pub mod scenario;
pub mod schema;
mod transitions;
