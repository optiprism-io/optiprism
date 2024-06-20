use std::sync::Arc;

use arrow::array::ArrayRef;
use metadata::MetadataProvider;
use serde::Deserialize;
use serde::Serialize;
use serde_json::Value;

use crate::{array_ref_to_json_values, validate_event_filter_property};
use crate::json_value_to_scalar;
use crate::EventRef;
use crate::ListResponse;
use crate::PlatformError;
use crate::PropValueOperation;
use crate::PropertyRef;
use crate::ResponseMetadata;
use crate::Result;


