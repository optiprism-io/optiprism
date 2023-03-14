use std::io::Write;
use arrow::array::{ArrayRef, Int32Array};
use arrow::record_batch::RecordBatch;
use arrow_array::ListArray;
use arrow_array::types::Int32Type;
use parquet::basic::{Compression, Repetition, Type};
use parquet::file::reader::{FilePageIterator, FileReader, SerializedFileReader};
use parquet::file::writer::{SerializedFileWriter, SerializedPageWriter};
use parquet::schema::types;

fn main() {
    use arrow_array::{Int32Array, ArrayRef};
    use arrow_array::RecordBatch;
    use parquet::arrow::arrow_writer::ArrowWriter;
    use parquet::file::properties::WriterProperties;
    use std::fs::File;
    use std::sync::Arc;
    let ids = Int32Array::from(vec![1, 2, 3, 4]);
    let vals = Int32Array::from(vec![5, 6, 7, 8]);
    let list_data = vec![
        Some(vec![]),
        None,
        Some(vec![Some(3), None, Some(5), Some(19)]),
        Some(vec![Some(6), Some(7)]),
    ];
    let list_array = ListArray::from_iter_primitive::<Int32Type, _, _>(list_data);
    let batch = RecordBatch::try_from_iter(vec![
        ("id", Arc::new(ids) as ArrayRef),
        ("val", Arc::new(vals) as ArrayRef),
        ("list", Arc::new(list_array) as ArrayRef),
    ]).unwrap();

    let file = File::create("data.parquet").unwrap();

    // Default writer properties
    let props = WriterProperties::builder().build();

    let mut writer = ArrowWriter::try_new(file, batch.schema(), Some(props)).unwrap();

    writer.write(&batch).expect("Writing batch");

    // writer must be closed to write footer
    writer.close().unwrap();

    let file = File::open("data.parquet").unwrap();
    let file_reader: Arc<dyn FileReader> =
        Arc::new(SerializedFileReader::new(file).unwrap());

    let i = FilePageIterator::new(0, file_reader).unwrap();

    for p in i {
        let p = p.unwrap();
        let pp = p.get_next_page().unwrap().unwrap();
        println!("{:?}", pp.statistics().unwrap().physical_type())
    }
    /*
        let schema = Arc::new(
            types::Type::group_type_builder("schema")
                .with_fields(&mut vec![Arc::new(
                    types::Type::primitive_type_builder("col1", Type::INT32)
                        .with_repetition(Repetition::REQUIRED)
                        .build()
                        .unwrap(),
                )])
                .build()
                .unwrap(),
        );
        let props = Arc::new(
            WriterProperties::builder()
                .set_compression(Compression::SNAPPY)
                .build(),
        );

        let mut file = File::create("data2.parquet").unwrap();

        let mut file_writer =
            SerializedFileWriter::new(&mut file, schema, props).unwrap();
        let mut rows: i64 = 0;
        )
        for (idx, subset) in data.iter().enumerate() {
            let mut row_group_writer = file_writer.next_row_group().unwrap();
            if let Some(mut writer) = row_group_writer.next_column().unwrap() {
                writer.type
                rows += writer
                    .typed::<D>()
                    .write_batch(&subset[..], None, None)
                    .unwrap() as i64;
                writer.close().unwrap();
            }
            let last_group = row_group_writer.close().unwrap();
            let flushed = file_writer.flushed_row_groups();
            assert_eq!(flushed.len(), idx + 1);
            assert_eq!(flushed[idx].as_ref(), last_group.as_ref());
        }
        let file_metadata = file_writer.close().unwrap();*/
}