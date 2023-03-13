use parquet2::metadata::SchemaDescriptor;
use parquet2::schema::types::ParquetType;
use crate::error::{Result, StoreError};

pub fn try_merge_fields(left: &ParquetType, right: &ParquetType) -> Result<ParquetType> {
    let merged = match (left, right) {
        (ParquetType::PrimitiveType(l), ParquetType::PrimitiveType(r))if l == r =>
            ParquetType::PrimitiveType(l.to_owned()),
        (ParquetType::GroupType {
            fields,
            field_info,
            logical_type,
            converted_type
        }, ParquetType::GroupType {
            fields: other_fields,
            field_info: other_field_info,
            logical_type: other_logical_type,
            converted_type: other_converted_type,
        }) => {
            if field_info != other_field_info || logical_type != other_logical_type || converted_type != other_converted_type {
                return Err(StoreError::InvalidParameter("Group types are not equal".to_string()));
            }

            let fields = fields
                .iter()
                .map(|f| {
                    let other_field = other_fields.iter().find(|other_f| other_f.name() == f.name());
                    match other_field {
                        None => Ok(f.to_owned()),
                        Some(other_field) => try_merge_fields(f, other_field)
                    }
                })
                .collect::<Result<Vec<ParquetType>>>()?;

            ParquetType::GroupType {
                field_info: field_info.to_owned(),
                logical_type: *logical_type,
                converted_type: *converted_type,
                fields,
            }
        }
        _ => return Err(StoreError::InvalidParameter("Primitive types are not equal".to_string()))
    };

    Ok(merged)
}

pub fn try_merge_schemas(schemas: Vec<&SchemaDescriptor>, name: String) -> Result<SchemaDescriptor> {
    let fields: Result<Vec<ParquetType>> = schemas
        .into_iter()
        .map(|sd| sd.fields())
        .try_fold(Vec::<ParquetType>::new(), |mut merged, unmerged| {
            for field in unmerged.into_iter() {
                let merged_field = merged.iter_mut().find(|merged_field| merged_field.name() == field.name());
                match merged_field {
                    None => merged.push(field.to_owned()),
                    Some(merged_field) => *merged_field = try_merge_fields(merged_field, field)?
                }
            }

            Ok(merged)
        });

    Ok(SchemaDescriptor::new(name, fields?))
}

#[cfg(test)]
mod tests {
    use parquet2::schema::io_message::from_message;
    use parquet2::schema::Repetition;
    use parquet2::schema::types::{FieldInfo, GroupConvertedType, GroupLogicalType, ParquetType, PhysicalType, PrimitiveLogicalType, PrimitiveType};
    use crate::parquet::schema::try_merge_fields;

    #[test]
    fn test_merge_primitive_fields() -> anyhow::Result<()> {
        let l = ParquetType::PrimitiveType(PrimitiveType {
            field_info: FieldInfo {
                name: "a".to_string(),
                repetition: Repetition::Optional,
                id: None,
            },
            converted_type: None,
            logical_type: None,
            physical_type: PhysicalType::Int32,
        });

        let r = ParquetType::PrimitiveType(PrimitiveType {
            field_info: FieldInfo {
                name: "a".to_string(),
                repetition: Repetition::Optional,
                id: None,
            },
            converted_type: None,
            logical_type: None,
            physical_type: PhysicalType::Int32,
        });

        let merged = try_merge_fields(&l, &r)?;

        println!("{:?}", merged);
        // assert_eq!(merged, l);

        Ok(())
    }

    #[test]
    fn test_merge_group_fields() -> anyhow::Result<()> {
        let l = ParquetType::GroupType {
            field_info: FieldInfo {
                name: "a".to_string(),
                repetition: Repetition::Required,
                id: None,
            },
            logical_type: Some(GroupLogicalType::List),
            converted_type: Some(GroupConvertedType::List),
            fields: vec![
                ParquetType::PrimitiveType(PrimitiveType {
                    field_info: FieldInfo {
                        name: "a".to_string(),
                        repetition: Repetition::Optional,
                        id: None,
                    },
                    converted_type: None,
                    logical_type: None,
                    physical_type: PhysicalType::Int32,
                })
            ],
        };


        let r = ParquetType::GroupType {
            field_info: FieldInfo {
                name: "a".to_string(),
                repetition: Repetition::Required,
                id: None,
            },
            logical_type: Some(GroupLogicalType::List),
            converted_type: Some(GroupConvertedType::List),
            fields: vec![
                ParquetType::PrimitiveType(PrimitiveType {
                    field_info: FieldInfo {
                        name: "b".to_string(),
                        repetition: Repetition::Optional,
                        id: None,
                    },
                    converted_type: None,
                    logical_type: None,
                    physical_type: PhysicalType::Int64,
                }),
                ParquetType::GroupType {
                    field_info: FieldInfo {
                        name: "a".to_string(),
                        repetition: Repetition::Required,
                        id: None,
                    },
                    logical_type: Some(GroupLogicalType::List),
                    converted_type: Some(GroupConvertedType::List),
                    fields: vec![
                        ParquetType::PrimitiveType(PrimitiveType {
                            field_info: FieldInfo {
                                name: "a".to_string(),
                                repetition: Repetition::Optional,
                                id: None,
                            },
                            converted_type: None,
                            logical_type: None,
                            physical_type: PhysicalType::Int32,
                        })
                    ],
                }
            ],
        };

        let merged = try_merge_fields(&l, &r)?;

        println!("{:?}", merged);
        // assert_eq!(merged, l);

        Ok(())
    }
}
