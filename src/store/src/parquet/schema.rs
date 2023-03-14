use tracing::error;
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
        }) if field_info == other_field_info && logical_type == other_logical_type && converted_type == other_converted_type => {
            let mut fields = fields.to_owned();
            for other_field in other_fields.iter() {
                match fields.iter_mut().find(|f| f.name() == other_field.name()) {
                    None => fields.push(other_field.to_owned()),
                    Some(merged_field) => *merged_field = try_merge_fields(merged_field, other_field)?
                }
            }

            ParquetType::GroupType {
                field_info: field_info.to_owned(),
                logical_type: *logical_type,
                converted_type: *converted_type,
                fields,
            }
        }
        _ => {
            error!("Types {:#?} and {:#?} are not equal", left, right);
            return Err(StoreError::InvalidParameter(format!("Types are not equal")));
        }
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
    use crate::parquet::schema::{try_merge_fields, try_merge_schemas};
    use test_log::test;
    use parquet2::metadata::SchemaDescriptor;

    fn left_schema() -> ParquetType {
        ParquetType::GroupType {
            field_info: FieldInfo {
                name: "1".to_string(),
                repetition: Repetition::Required,
                id: None,
            },
            logical_type: Some(GroupLogicalType::List),
            converted_type: Some(GroupConvertedType::List),
            fields: vec![
                ParquetType::PrimitiveType(PrimitiveType {
                    field_info: FieldInfo {
                        name: "1.1".to_string(),
                        repetition: Repetition::Optional,
                        id: None,
                    },
                    converted_type: None,
                    logical_type: None,
                    physical_type: PhysicalType::Int32,
                }),
                ParquetType::GroupType {
                    field_info: FieldInfo {
                        name: "1.2".to_string(),
                        repetition: Repetition::Required,
                        id: None,
                    },
                    logical_type: Some(GroupLogicalType::List),
                    converted_type: Some(GroupConvertedType::List),
                    fields: vec![
                        ParquetType::PrimitiveType(PrimitiveType {
                            field_info: FieldInfo {
                                name: "1.2.1".to_string(),
                                repetition: Repetition::Optional,
                                id: None,
                            },
                            converted_type: None,
                            logical_type: None,
                            physical_type: PhysicalType::Int32,
                        })
                    ],
                },
            ],
        }
    }

    fn right_schema() -> ParquetType {
        ParquetType::GroupType {
            field_info: FieldInfo {
                name: "1".to_string(),
                repetition: Repetition::Required,
                id: None,
            },
            logical_type: Some(GroupLogicalType::List),
            converted_type: Some(GroupConvertedType::List),
            fields: vec![
                ParquetType::PrimitiveType(PrimitiveType {
                    field_info: FieldInfo {
                        name: "1.3".to_string(),
                        repetition: Repetition::Optional,
                        id: None,
                    },
                    converted_type: None,
                    logical_type: None,
                    physical_type: PhysicalType::Int32,
                }),
                ParquetType::GroupType {
                    field_info: FieldInfo {
                        name: "1.2".to_string(),
                        repetition: Repetition::Required,
                        id: None,
                    },
                    logical_type: Some(GroupLogicalType::List),
                    converted_type: Some(GroupConvertedType::List),
                    fields: vec![
                        ParquetType::PrimitiveType(PrimitiveType {
                            field_info: FieldInfo {
                                name: "1.2.2".to_string(),
                                repetition: Repetition::Optional,
                                id: None,
                            },
                            converted_type: None,
                            logical_type: None,
                            physical_type: PhysicalType::Int32,
                        }),
                        ParquetType::PrimitiveType(PrimitiveType {
                            field_info: FieldInfo {
                                name: "1.2.3".to_string(),
                                repetition: Repetition::Optional,
                                id: None,
                            },
                            converted_type: None,
                            logical_type: None,
                            physical_type: PhysicalType::Int32,
                        }),
                    ],
                },
            ],
        }
    }

    fn expected_schema() -> ParquetType {
        ParquetType::GroupType {
            field_info: FieldInfo {
                name: "1".to_string(),
                repetition: Repetition::Required,
                id: None,
            },
            logical_type: Some(GroupLogicalType::List),
            converted_type: Some(GroupConvertedType::List),
            fields: vec![
                ParquetType::PrimitiveType(PrimitiveType {
                    field_info: FieldInfo {
                        name: "1.1".to_string(),
                        repetition: Repetition::Optional,
                        id: None,
                    },
                    converted_type: None,
                    logical_type: None,
                    physical_type: PhysicalType::Int32,
                }),
                ParquetType::GroupType {
                    field_info: FieldInfo {
                        name: "1.2".to_string(),
                        repetition: Repetition::Required,
                        id: None,
                    },
                    logical_type: Some(GroupLogicalType::List),
                    converted_type: Some(GroupConvertedType::List),
                    fields: vec![
                        ParquetType::PrimitiveType(PrimitiveType {
                            field_info: FieldInfo {
                                name: "1.2.1".to_string(),
                                repetition: Repetition::Optional,
                                id: None,
                            },
                            converted_type: None,
                            logical_type: None,
                            physical_type: PhysicalType::Int32,
                        }),
                        ParquetType::PrimitiveType(PrimitiveType {
                            field_info: FieldInfo {
                                name: "1.2.2".to_string(),
                                repetition: Repetition::Optional,
                                id: None,
                            },
                            converted_type: None,
                            logical_type: None,
                            physical_type: PhysicalType::Int32,
                        }),
                        ParquetType::PrimitiveType(PrimitiveType {
                            field_info: FieldInfo {
                                name: "1.2.3".to_string(),
                                repetition: Repetition::Optional,
                                id: None,
                            },
                            converted_type: None,
                            logical_type: None,
                            physical_type: PhysicalType::Int32,
                        }),
                    ],
                },
                ParquetType::PrimitiveType(PrimitiveType {
                    field_info: FieldInfo {
                        name: "1.3".to_string(),
                        repetition: Repetition::Optional,
                        id: None,
                    },
                    converted_type: None,
                    logical_type: None,
                    physical_type: PhysicalType::Int32,
                }),
            ],
        }
    }

    #[test]
    fn test_merge_primitive_fields_mismatch() -> anyhow::Result<()> {
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
                repetition: Repetition::Required,
                id: None,
            },
            converted_type: None,
            logical_type: None,
            physical_type: PhysicalType::Int32,
        });

        assert!(try_merge_fields(&l, &r).is_err());

        Ok(())
    }

    #[test]
    fn test_merge_group_types_mismatch() -> anyhow::Result<()> {
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
            logical_type: Some(GroupLogicalType::Map),
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

        assert!(try_merge_fields(&l, &r).is_err());

        Ok(())
    }

    #[test]
    fn test_merge_fields() -> anyhow::Result<()> {
        let merged = try_merge_fields(&left_schema(), &right_schema())?;
        assert_eq!(merged, expected_schema());

        Ok(())
    }

    #[test]
    fn test_merge_schemas() -> anyhow::Result<()> {
        let l = SchemaDescriptor::new("a".to_string(), vec![left_schema()]);
        let r = SchemaDescriptor::new("b".to_string(), vec![right_schema()]);
        let exp = SchemaDescriptor::new("m".to_string(), vec![expected_schema()]);
        let merged = try_merge_schemas(vec![&l, &r], "m".to_string())?;
        assert_eq!(merged.name(), "m");
        assert_eq!(merged.fields(), exp.fields());
        assert_eq!(merged.columns(), exp.columns());

        Ok(())
    }
}
