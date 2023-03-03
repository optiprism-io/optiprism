use std::io::Write;
use std::mem;
use parquet_format_safe::{ColumnChunk, KeyValue, RowGroup};
use parquet_format_safe::thrift::protocol::TCompactOutputProtocol;
use crate::error::Error;
use crate::metadata::{ColumnChunkMetaData, ColumnDescriptor, SchemaDescriptor, ThriftFileMetaData};
use crate::write::page::{is_data_page, PageWriteSpec, write_page};
use crate::write::{ColumnOffsetsMetadata, State, WriteOptions};
use crate::write::file::{end_file, start_file};
use crate::error::Result;
use crate::page::CompressedPage;
use crate::write::column_chunk::build_column_chunk;
use crate::write::indexes::{write_column_index, write_offset_index};

struct RowGroupState {
    columns: Vec<ColumnChunk>,
    cur_col: usize,
    cur_col_page_specs: Vec<PageWriteSpec>,
    page_specs: Vec<Vec<PageWriteSpec>>,
}

impl RowGroupState {
    pub fn new() -> Self {
        Self {
            columns: vec![],
            cur_col: 0,
            cur_col_page_specs: vec![],
            page_specs: vec![],
        }
    }
}

/// An interface to write a parquet file.
/// Use `start` to write the header, `write` to write a row group,
/// and `end` to write the footer.
pub struct FileSeqWriter<W: Write> {
    writer: W,
    schema: SchemaDescriptor,
    options: WriteOptions,
    created_by: Option<String>,
    offset: u64,
    row_group_state: RowGroupState,
    row_groups: Vec<RowGroup>,
    page_specs: Vec<Vec<Vec<PageWriteSpec>>>,
    /// Used to store the current state for writing the file
    state: State,
    // when the file is written, metadata becomes available
    metadata: Option<ThriftFileMetaData>,
}

impl<W: Write> FileSeqWriter<W> {
    /// Returns a new [`FileSeqWriter`].
    pub fn new(
        writer: W,
        schema: SchemaDescriptor,
        options: WriteOptions,
        created_by: Option<String>,
    ) -> Self {
        Self {
            writer,
            schema,
            options,
            created_by,
            offset: 0,
            row_group_state: RowGroupState::new(),
            row_groups: vec![],
            page_specs: vec![],
            state: State::Initialised,
            metadata: None,
        }
    }

    fn current_column_descriptor(&self) -> &ColumnDescriptor {
        &self.schema.columns()[self.row_group_state.cur_col]
    }
    pub fn current_column_page_specs(&self) -> &[PageWriteSpec] {
        &self.row_group_state.page_specs[self.row_group_state.cur_col]
    }

    fn start(&mut self) -> Result<()> {
        if self.offset == 0 {
            self.offset = start_file(&mut self.writer)? as u64;
            self.state = State::Started;
            Ok(())
        } else {
            Err(Error::InvalidParameter(
                "Start cannot be called twice".to_string(),
            ))
        }
    }

    pub fn end_column(&mut self) -> Result<()> {
        let state = &mut self.row_group_state;
        let specs = mem::take(&mut state.cur_col_page_specs);
        let descriptor = &self.schema.columns()[state.cur_col];
        let column_chunk = build_column_chunk(&specs, descriptor)?;

        // write metadata
        let mut protocol = TCompactOutputProtocol::new(&mut self.writer);
        let size = column_chunk
            .meta_data
            .as_ref()
            .unwrap()
            .write_to_out_protocol(&mut protocol)? as u64;
        ;
        self.offset += size;

        state.columns.push(column_chunk);
        state.page_specs.push(specs);
        if state.cur_col + 1 < self.schema.columns().len() {
            state.cur_col += 1;
        }

        Ok(())
    }

    pub fn end_row_group(&mut self) -> Result<()> {
        let mut state = RowGroupState::new();
        mem::swap(&mut state, &mut self.row_group_state);
        let columns = &state.columns;
        // compute row group stats
        let file_offset = columns
            .get(0)
            .map(|column_chunk| {
                ColumnOffsetsMetadata::from_column_chunk(column_chunk).calc_row_group_file_offset()
            })
            .unwrap_or(None);

        let total_byte_size = columns
            .iter()
            .map(|c| c.meta_data.as_ref().unwrap().total_uncompressed_size)
            .sum();
        let total_compressed_size = columns
            .iter()
            .map(|c| c.meta_data.as_ref().unwrap().total_compressed_size)
            .sum();

        // TODO fix this this
        let num_rows = state
            .page_specs[0]
            .iter()
            .filter(|x| is_data_page(x))
            .fold(0, |acc, x| acc + x.num_values);
        let row_group = RowGroup {
            columns: state.columns,
            total_byte_size,
            num_rows: num_rows as i64,
            sorting_columns: None,
            file_offset,
            total_compressed_size: Some(total_compressed_size),
            ordinal: Some(self.row_groups.len() as i16),
        };

        self.page_specs.push(state.page_specs);
        self.row_groups.push(row_group);
        Ok(())
    }

    pub fn write_page(&mut self, compressed_page: &CompressedPage) -> Result<()> {
        if self.offset == 0 {
            self.start()?;
        }

        let mut spec = write_page(&mut self.writer, self.offset, compressed_page)?;
        self.offset += spec.bytes_written;
        // todo fix
        spec.num_rows = Some(spec.num_values);
        self.row_group_state.cur_col_page_specs.push(spec);
        Ok(())
    }

    pub fn end(&mut self, key_value_metadata: Option<Vec<KeyValue>>) -> Result<u64> {
        if self.offset == 0 {
            self.start()?;
        }

        if self.state != State::Started {
            return Err(Error::InvalidParameter(
                "End cannot be called twice".to_string(),
            ));
        }
        // compute file stats
        let num_rows = self.row_groups.iter().map(|group| group.num_rows).sum();

        if self.options.write_statistics {
            // write column indexes (require page statistics)
            self.row_groups
                .iter_mut()
                .zip(self.page_specs.iter())
                .try_for_each(|(group, pages)| {
                    group.columns.iter_mut().zip(pages.iter()).try_for_each(
                        |(column, pages)| {
                            let offset = self.offset;
                            column.column_index_offset = Some(offset as i64);
                            self.offset += write_column_index(&mut self.writer, pages)?;
                            let length = self.offset - offset;
                            column.column_index_length = Some(length as i32);
                            Result::Ok(())
                        },
                    )?;
                    Result::Ok(())
                })?;
        };

        // write offset index
        self.row_groups
            .iter_mut()
            .zip(self.page_specs.iter())
            .try_for_each(|(group, pages)| {
                group
                    .columns
                    .iter_mut()
                    .zip(pages.iter())
                    .try_for_each(|(column, pages)| {
                        let offset = self.offset;
                        column.offset_index_offset = Some(offset as i64);
                        self.offset += write_offset_index(&mut self.writer, pages)?;
                        column.offset_index_length = Some((self.offset - offset) as i32);
                        Result::Ok(())
                    })?;
                Result::Ok(())
            })?;

        let metadata = ThriftFileMetaData::new(
            self.options.version.into(),
            self.schema.clone().into_thrift(),
            num_rows,
            self.row_groups.clone(),
            key_value_metadata,
            self.created_by.clone(),
            None,
            None,
            None,
        );

        let len = end_file(&mut self.writer, &metadata)?;
        self.state = State::Finished;
        self.metadata = Some(metadata);
        Ok(self.offset + len)
    }
}
