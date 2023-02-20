use arrow2::io::parquet::read::{column_iter_to_arrays, infer_schema};
use parquet2::encoding::Encoding;
use parquet2::page::{DataPage, DictPage, Page, split_buffer};
use parquet2::read::{get_page_iterator, read_metadata};
use parquet2::schema::types::PhysicalType;
use crate::Result;

fn a() {
    let mut reader = std::fs::File::open(path)?;
    let metadata = read_metadata(&mut reader)?;

    let row_group = 0;
    let column = 0;
    let columns = metadata.row_groups[row_group].columns();
    let schema = infer_schema(&metadata)?;
    let column_metadata = &columns[column];
    let type_ = column_metadata
        .descriptor()
        .descriptor
        .primitive_type
        .clone();
    let field = schema.fields[0].clone();
    let pages = get_page_iterator(column_metadata, &mut reader, None, vec![], 1024 * 1024)?;
    let mut iter = column_iter_to_arrays(vec![pages], vec![&type_], field, None).unwrap();

    let mut decompress_buffer = vec![];
    for maybe_page in pages {
        let page = maybe_page?;
        let page = parquet2::read::decompress(page, &mut decompress_buffer)?;



        match page {
            Page::Data(page) => {
                let _array = deserialize(&page)?;
            }
            _ => unimplemented!(),
        }
    }
}

fn deserialize(page: &DataPage) -> Result<()> {
    // split the data buffer in repetition levels, definition levels and values
    let (_rep_levels, _def_levels, values_buffer) = split_buffer(page)?;
    // decode and deserialize.
    match (
        page.descriptor.primitive_type.physical_type,
        page.encoding(),
    ) {
        (PhysicalType::Int32, Encoding::Plain, None) => {


        }
    }

    Ok(())
}