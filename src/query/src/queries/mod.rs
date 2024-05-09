use std::collections::HashMap;
use std::sync::Arc;

use common::query::EventFilter;
use common::query::PropValueOperation;
use common::query::PropertyRef;
use datafusion_common::Column;
use metadata::dictionaries::SingleDictionaryProvider;
use metadata::MetadataProvider;

use crate::Context;

pub mod event_records_search;
pub mod event_segmentation;
pub mod funnel;
pub mod group_records_search;
pub mod property_values;

pub fn decode_filter_single_dictionary(
    ctx: &Context,
    metadata: &Arc<MetadataProvider>,
    cols_hash: &mut HashMap<String, ()>,
    decode_cols: &mut Vec<(Column, Arc<SingleDictionaryProvider>)>,
    filter: &EventFilter,
) -> crate::Result<()> {
    match filter {
        EventFilter::Property {
            property,
            operation,
            value: _,
        } => match operation {
            PropValueOperation::Like
            | PropValueOperation::NotLike
            | PropValueOperation::Regex
            | PropValueOperation::NotRegex => {
                let prop = match property {
                    PropertyRef::System(prop_ref) => metadata
                        .system_properties
                        .get_by_name(ctx.project_id, prop_ref.as_str())?,
                    PropertyRef::SystemGroup(prop_ref) => metadata
                        .system_group_properties
                        .get_by_name(ctx.project_id, prop_ref.as_str())?,
                    PropertyRef::Group(prop_ref, group) => metadata.group_properties[*group]
                        .get_by_name(ctx.project_id, prop_ref.as_str())?,
                    PropertyRef::Event(prop_ref) => metadata
                        .event_properties
                        .get_by_name(ctx.project_id, prop_ref.as_str())?,
                    PropertyRef::Custom(_) => unreachable!(),
                };

                if !prop.is_dictionary {
                    return Ok(());
                }
                let col_name = prop.column_name();
                let dict = SingleDictionaryProvider::new(
                    ctx.project_id,
                    col_name.clone(),
                    metadata.dictionaries.clone(),
                );
                let col = Column::from_name(col_name);
                decode_cols.push((col, Arc::new(dict)));

                if cols_hash.contains_key(prop.column_name().as_str()) {
                    return Ok(());
                }
                cols_hash.insert(prop.column_name(), ());
            }
            _ => {}
        },
    }

    Ok(())
}
