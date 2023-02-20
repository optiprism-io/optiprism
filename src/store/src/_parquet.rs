use std::any::Any;
use std::fs::File;
use std::io::Bytes;
use std::path::PathBuf;
use bytes::BytesMut;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::data_type::Int32Type;
use parquet::file::properties::{WriterProperties, WriterPropertiesBuilder, WriterPropertiesPtr};
use parquet::file::reader::RowGroupReader;
use parquet::file::serialized_reader::{FileReader, SerializedFileReader};
use parquet::file::statistics::Statistics;
use parquet::file::writer::{SerializedRowGroupWriter, TrackedWrite};
use parquet::format::RowGroup;
use parquet::schema::types::SchemaDescPtr;
use crate::error::StoreError;
use crate::Result;

pub struct Parquet {}

impl Parquet {
    pub fn open(path: PathBuf) -> Result<Self> {
        file = File::open(path)?;
        let reader = SerializedFileReader::new(file)?;
        reader.
        let parquet_metadata = reader.metadata();
        parquet_metadata.row_group(1).column(1).statistics().unwrap()
        assert_eq!(parquet_metadata.num_row_groups(), 1);

        let row_group_reader = reader.get_row_group(0).unwrap();
        assert_eq!(row_group_reader.num_columns(), 1);
    }
}

pub struct RowGroupIter<'a> {
    rdr: &'a dyn FileReader,
    current_row_group: usize,
    num_row_groups: usize,
}

impl<'a> RowGroupIter<'a> {
    pub fn new(rdr: &'a dyn FileReader) -> Self {
        Self {
            rdr,
            current_row_group: 0,
            num_row_groups: rdr.metadata().num_row_groups(),
        }
    }
}

impl<'a> Iterator for RowGroupIter<'a> {
    type Item = Result<Box<dyn RowGroupReader>>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.current_row_group >= self.num_row_groups {
            return None;
        }

        let res = match self.rdr.get_row_group(self.current_row_group) {
            Ok(row_group) => {
                self.current_row_group += 1;
                Ok(row_group)
            }
            Err(err) => Err(err.into())
        };

        Some(res)
    }
}

pub struct MergingRowGroupIter<'a> {
    iters: Vec<RowGroupIter<'a>>,
    schema: SchemaDescPtr,
    write_properties: WriterPropertiesPtr,
    buf: BytesMut,
}

impl<'a> MergingRowGroupIter<'a> {
    pub fn new(iters: Vec<RowGroupIter>, schema: SchemaDescPtr, write_properties: WriterPropertiesPtr) -> Self {
        Self { iters, schema, write_properties, buf: BytesMut::with_capacity(1024) }
    }
}

impl<'a> Iterator for MergingRowGroupIter<'a> {
    type Item = Result<Box<dyn RowGroupReader>>;

    fn next(&mut self) -> Option<Self::Item> {
        self.buf.clear();
        let mut tracked_buf = TrackedWrite::new(&mut self.buf);

        let mut writer = SerializedRowGroupWriter::new(
            self.schema.clone(),
            self.write_properties.clone(),
            &mut tracked_buf,
            None,
        );

        let mut cw = writer.next_column().unwrap().unwrap();
        cw.typed::<Int32Type>().write_batch();

        writer.close().unwrap();
        None
    }
}

pub fn merge(iters: &mut [RowGroupIter], out: PathBuf) -> Result<()> {
    for iter in iters.iter_mut() {
        let a = iter.next().unwrap()?;
        a.metadata().schema_descr().column(1).name()
        a.metadata().column(1).statistics().unwrap().sor
        match a.metadata().column(1).statistics().unwrap() {
            Statistics::Int32(v) => {}
            Statistics::Int64(_) => {}
            Statistics::Int96(_) => {}
            Statistics::Float(_) => {}
            Statistics::Double(_) => {}
            Statistics::ByteArray(_) => {}
            Statistics::FixedLenByteArray(_) => {}
        }
        let i = a.get_column_reader(1)?;
        i.val
        for i in a.get_column_reader(1) {
            i.
        }
    }
    for path in files.iter() {
        if let Ok(file) = File::open(&path) {
            let reader = SerializedFileReader::new(file)?;

            let parquet_metadata = reader.metadata();
            assert_eq!(parquet_metadata.num_row_groups(), 1);

            let row_group_reader = reader.get_row_group(0)?;
            assert_eq!(row_group_reader.num_columns(), 1);
        }
    }

    Ok(())
}