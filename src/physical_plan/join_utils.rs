use arrow::datatypes::{Schema, Field};
use crate::physical_plan::join_segment::JoinOn;
use std::collections::HashSet;

/// Creates a schema for a join operation.
/// The fields from the left side are first
pub fn build_join_schema(
    left: &Schema,
    right: &Schema,
    on: &JoinOn,
) -> Schema {
    let fields: Vec<Field> = {
        // remove right-side join keys if they have the same names as the left-side
        let duplicate_keys = &on
            .iter()
            .filter(|(l, r)| l == r)
            .map(|on| on.1.to_string())
            .collect::<HashSet<_>>();


        let left_fields = left.fields().iter();

        let right_fields = right
            .fields()
            .iter()
            .filter(|f| !(&on.0 == &on.1 && &on.0 == f.name()));

        // left then right
        left_fields.chain(right_fields).cloned().collect()
    };
    Schema::new(fields)
}