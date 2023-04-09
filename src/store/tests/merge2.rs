use std::fs::File;
use std::io;
use std::io::BufRead;
use std::io::BufReader;
use std::io::Cursor;
use std::path::Path;

use arrow2::array::{Array, BinaryArray, FixedSizeBinaryArray};
use arrow2::array::BooleanArray;
use arrow2::array::Float64Array;
use arrow2::array::Int32Array;
use arrow2::array::Int64Array;
use arrow2::array::Int8Array;
use arrow2::array::ListArray;
use arrow2::array::MutableBooleanArray;
use arrow2::array::MutableListArray;
use arrow2::array::MutablePrimitiveArray;
use arrow2::array::MutableUtf8Array;
use arrow2::array::PrimitiveArray;
use arrow2::array::TryExtend;
use arrow2::array::Utf8Array;
use arrow2::buffer::Buffer;
use arrow2::chunk::Chunk;
use arrow2::datatypes::DataType;
use arrow2::datatypes::Field;
use arrow2::datatypes::Schema;
use arrow2::io::csv::read::deserialize_batch;
use arrow2::io::csv::read::deserialize_column;
use arrow2::io::csv::read::infer;
use arrow2::io::csv::read::infer_schema;
use arrow2::io::csv::read::read_rows;
use arrow2::io::csv::read::ByteRecord;
use arrow2::io::csv::read::ReaderBuilder;
use arrow2::io::parquet::read;
use arrow2::io::parquet::write::to_parquet_schema;
use arrow2::io::parquet::write::to_parquet_type;
use arrow2::io::print;
use arrow2::offset::Offset;
use arrow2::types::NativeType;
use parquet2::compression::CompressionOptions;
use parquet2::page::CompressedPage;
use parquet2::read::{get_page_iterator, read_metadata};
use parquet2::schema::types::ParquetType;
use parquet2::write::FileSeqWriter;
use parquet2::write::Version;
use parquet2::write::WriteOptions;
use store::error::Result;
use store::parquet_new::merger::FileMerger;
use store::parquet_new::parquet::CompressedPageIterator;
use store::test_util::{create_list_primitive_array, create_parquet_from_arrays, gen_arrays_for_parquet, gen_binary_data_array, gen_binary_data_list_array, gen_boolean_data_array, gen_boolean_data_list_array, gen_fixed_size_binary_data_array, gen_idx_primitive_array, gen_primitive_data_array, gen_primitive_data_list_array, gen_secondary_idx_primitive_array, gen_utf8_data_array, gen_utf8_data_list_array};
use store::test_util::parse_markdown_table;


#[test]
fn test() {
    let fields = vec![
        Field::new("idx1", DataType::Int64, false),
        Field::new("idx2", DataType::Int32, false),
        Field::new("d1", DataType::Int64, true),
        Field::new("d2", DataType::Float64, true),
        Field::new("d3", DataType::Utf8, true),
        Field::new("d4", DataType::LargeUtf8, true),
        Field::new("d5", DataType::Binary, true),
        Field::new("d6", DataType::LargeBinary, true),
        Field::new("d7", DataType::Boolean, true),
        Field::new("dl1", DataType::List(Box::new(Field::new("f", DataType::Int64, false))), true),
        Field::new("dl2", DataType::List(Box::new(Field::new("f", DataType::Float64, false))), true),
        Field::new("dl3", DataType::List(Box::new(Field::new("f", DataType::Utf8, false))), true),
        Field::new("dl4", DataType::List(Box::new(Field::new("f", DataType::LargeUtf8, false))), true),
        Field::new("dl5", DataType::List(Box::new(Field::new("f", DataType::Binary, false))), true),
        Field::new("dl6", DataType::List(Box::new(Field::new("f", DataType::LargeBinary, false))), true),
        Field::new("dl7", DataType::List(Box::new(Field::new("f", DataType::Boolean, false))), true),
    ];

    let arrs = gen_arrays_for_parquet(&fields, 2, 10, None);
    let chunk = Chunk::new(arrs);
    let names = fields.iter().map(|f| f.name.to_string()).collect::<Vec<_>>();
    println!("{}", print::write(&[chunk], &names));
}