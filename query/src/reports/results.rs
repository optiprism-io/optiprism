use arrow::array::ArrayRef;
use arrow::datatypes::{DataType, SchemaRef};

pub struct Column {
    pub name: String,
    pub group: String,
    pub is_nullable: bool,
    pub data_type: DataType,
    pub data: ArrayRef,
}

impl Column {
    pub fn name(&self) -> &str {
        self.name.as_str()
    }

    pub fn group(&self) -> &str {
        self.group.as_str()
    }

    pub fn is_nullable(&self) -> bool {
        self.is_nullable
    }

    pub fn data_type(&self) -> &DataType {
        &self.data_type
    }

    pub fn data(&self) -> &ArrayRef {
        &self.data
    }
}

pub struct DataTable {
    pub schema: SchemaRef,
    pub columns: Vec<Column>,
}

impl DataTable {
    pub fn new(schema: SchemaRef, columns: Vec<Column>) -> Self {
        Self { schema, columns }
    }
}
