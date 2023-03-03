use std::cmp::Ordering;
use std::fs::File;
use std::io::{Read, Write};
use std::iter::Peekable;
use std::path::{Path, PathBuf};
use std::task::Wake;
use arrow2::array::{Array, MutablePrimitiveArray};
use arrow2::chunk::Chunk;
use arrow2::datatypes::{DataType, Schema, TimeUnit};
use arrow2::io::parquet::read::{column_iter_to_arrays, fallible_streaming_iterator, Pages, ParquetError};
use arrow2::io::parquet::read::deserialize::page_iter_to_arrays;
use parquet2::encoding::Encoding;
use parquet2::metadata::SchemaDescriptor;
use parquet2::page::{CompressedDataPage, CompressedPage, DataPage, DictPage, Page, split_buffer};
use parquet2::read::{decompress, get_page_iterator};
use parquet2::schema::Repetition;
use parquet2::schema::types::{FieldInfo, ParquetType, PhysicalType, PrimitiveConvertedType, PrimitiveLogicalType, PrimitiveType};
use parquet2::write::{DynIter, DynStreamingIterator, FileSeqWriter, FileWriter, Version, WriteOptions};
use fallible_streaming_iterator::FallibleStreamingIterator;
use arrow2::io::parquet::read;
use parquet2::error::Error;
use store::error::Result;
use store::parquet::SequentialWriter;
use crate::init::init;

fn from_physical_type(t: &PhysicalType) -> DataType {
    match t {
        PhysicalType::Boolean => DataType::Boolean,
        PhysicalType::Int32 => DataType::Int32,
        PhysicalType::Int64 => DataType::Int64,
        PhysicalType::Float => DataType::Float32,
        PhysicalType::Double => DataType::Float64,
        PhysicalType::ByteArray => DataType::Utf8,
        PhysicalType::FixedLenByteArray(l) => DataType::FixedSizeBinary(*l),
        PhysicalType::Int96 => DataType::Timestamp(TimeUnit::Nanosecond, None),
    }
}

fn data_page_to_array(page: CompressedDataPage, buf: &mut Vec<u8>) -> Result<Box<dyn Array>> {
    let stats = page.statistics().unwrap()?;

    let num_rows = page.num_values() + stats.null_count().or_else(|| Some(0)).unwrap() as usize;
    let physical_type = stats.physical_type();
    let primitive_type = PrimitiveType::from_physical("f".to_string(), physical_type.to_owned());
    let data_type = from_physical_type(physical_type);
    let decompressed_page = decompress(CompressedPage::Data(page), buf)?;
    let iter = fallible_streaming_iterator::convert(std::iter::once(Ok(&decompressed_page)));
    let mut r = page_iter_to_arrays(iter, &primitive_type, data_type, None, num_rows)?;
    Ok(r.next().unwrap()?)
}

fn main() -> anyhow::Result<()> {
    println!("init");
    init()?;
    println!("\n\nwrite");
    let pt = PrimitiveType {
        field_info: FieldInfo {
            name: "0".to_string(),
            repetition: Repetition::Optional,
            id: None,
        },
        logical_type: Some(PrimitiveLogicalType::String),
        converted_type: Some(PrimitiveConvertedType::Uint8),
        physical_type: PhysicalType::ByteArray,
    };
    // TODO fix infinite execution with this schema, figure out how to define schema correctly
    let _schema = SchemaDescriptor::new(
        "schema".to_string(),
        vec![
            ParquetType::PrimitiveType(pt),
            ParquetType::from_physical(
                "1".to_string(),
                PhysicalType::Int64,
            ),
            ParquetType::from_physical(
                "2".to_string(),
                PhysicalType::Int64,
            ),
        ],
    );

    let schema = {
        let mut reader = File::open("/tmp/optiprism/0.parquet")?;
        // we can read its metadata:
        let metadata = parquet2::read::read_metadata(&mut reader)?;

        metadata.schema().to_owned()
    };
    let mut file = std::fs::File::create(format!("/tmp/optiprism/merged.parquet"))?;

    let mut seq_writer = FileSeqWriter::new(
        file,
        schema.clone(),
        WriteOptions {
            write_statistics: true,
            version: Version::V1,
        },
        None,
    );

    for file_id in 0..=1 {
        let mut reader = File::open(format!("/tmp/optiprism/{file_id}.parquet"))?;
        let metadata = parquet2::read::read_metadata(&mut reader)?;

        let mut arr_buf = vec![];
        for row_group in metadata.row_groups {
            for column in row_group.columns().iter() {
                let pages = parquet2::read::get_page_iterator(
                    column,
                    &mut reader,
                    None,
                    vec![],
                    1024 * 1024,
                )?;

                for maybe_page in pages {
                    let p = maybe_page?;
                    if let CompressedPage::Data(p) = p {
                        let arr = data_page_to_array(p, &mut arr_buf)?;
                        println!("{:?}", arr);
                    }

                    // seq_writer.write_page(&p)?;
                }
                seq_writer.end_column()?;
            }

            seq_writer.end_row_group()?;
        }
    }
    seq_writer.end(None)?;

    /*{
        let mut reader = File::open("/tmp/optiprism/merged.parquet")?;
        // we can read its metadata:
        let metadata = arrow2::io::parquet::read::read_metadata(&mut reader)?;
        // we can read the statistics of all parquet's row groups (here for each field)
        /*for field in &schema.fields {
            let statistics = arrow2::io::parquet::read::statistics::deserialize(field, &metadata.row_groups)?;
            println!("{statistics:#?}");
        }*/

        // we can then read the row groups into chunks
        let chunks = arrow2::io::parquet::read::FileReader::new(reader, metadata.row_groups, schema, Some(1024 * 8 * 8), None, None);
        for maybe_chunk in chunks {
            let chunk = maybe_chunk?;
            println!("{:?}", chunk);
        }
    }*/
    Ok(())
}

mod init {
    use arrow2::array::{Array, BooleanArray, Int64Array, Utf8Array};
    use arrow2::chunk::Chunk;
    use arrow2::datatypes::{Field, Schema};
    use arrow2::io::parquet::write::{FileWriter, RowGroupIterator, WriteOptions};
    use parquet2::compression::CompressionOptions;
    use parquet2::encoding::Encoding;
    use parquet2::write::Version;

    pub fn init() -> anyhow::Result<()> {
        let data: Vec<Vec<Vec<Box<dyn Array>>>> = vec![
            vec![ // file1
                  vec![ // row group 1
                        Box::new(Utf8Array::<i64>::from_slice(["a", "a"])),
                        Box::new(Int64Array::from_slice([1, 2])),
                        Box::new(Int64Array::from_slice([1, 1])),
                  ],
                  vec![ // row group 2
                        Box::new(Utf8Array::<i64>::from_slice(["a", "c", "c", "d"])),
                        Box::new(Int64Array::from_slice([3, 1, 2, 2])),
                        Box::new(Int64Array::from_slice([1, 1, 1, 1])),
                  ],
            ],
            vec![ // file2
                  vec![ // row group 1
                        Box::new(Utf8Array::<i64>::from_slice(["a", "c"])),
                        Box::new(Int64Array::from_slice([1, 2])),
                        Box::new(Int64Array::from_slice([1, 1])),
                  ],
                  vec![ // row group 2
                        Box::new(Utf8Array::<i64>::from_slice(["c"])),
                        Box::new(Int64Array::from_slice([3])),
                        Box::new(Int64Array::from_slice([1])),
                  ],
                  vec![ // row group 3
                        Box::new(Utf8Array::<i64>::from_slice(["d", "e"])),
                        Box::new(Int64Array::from_slice([1, 1])),
                        Box::new(Int64Array::from_slice([1, 1])),
                  ],
            ],
            /*vec![ // file3
                  vec![ // row group 1
                        Box::new(Utf8Array::<i64>::from_slice(["a", "e"])),
                        Box::new(Int64Array::from_slice([2, 3])),
                        Box::new(Int64Array::from_slice([1, 1])),
                        Box::new(BooleanArray::from_slice([true, true])),
                  ],
            ],
            vec![ // file4
                  vec![ // row group 1
                        Box::new(Utf8Array::<i64>::from_slice(["f", "g"])),
                        Box::new(Int64Array::from_slice(&[1, 2])),
                        Box::new(BooleanArray::from_slice([true, true])),
                  ],
            ],*/
        ];

        // declare a schema with fields
        for (file_id, row_groups) in data.into_iter().enumerate() {
            println!("{file_id}");
            let fields: Vec<Field> = row_groups[0].iter().enumerate().map(|(idx, v)| Field::new(format!("{idx}"), v.data_type().to_owned(), false)).collect();
            let fields_len = fields.len();
            let schema = Schema::from(fields);
            let mut file = std::fs::File::create(format!("/tmp/optiprism/{file_id}.parquet"))?;

            let options = WriteOptions {
                write_statistics: true,
                compression: CompressionOptions::Snappy,
                version: Version::V1,
                data_pagesize_limit: None,
            };
            let mut writer = FileWriter::try_new(file, schema.clone(), options.clone())?;

            let chunks: Vec<std::result::Result<Chunk<Box<dyn Array>>, _>> = row_groups.into_iter().map(|arrs| Ok(Chunk::new(arrs))).collect();

            let row_groups = RowGroupIterator::try_new(
                chunks.into_iter(),
                &schema,
                options,
                vec![vec![Encoding::Plain]; fields_len],
            )?;

            for group in row_groups {
                writer.write(group?)?;
            }
            let _ = writer.end(None)?;
        }

        Ok(())
    }
}