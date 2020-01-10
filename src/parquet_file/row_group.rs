use super::column::ParquetColumn;
use parquet::file::metadata::RowGroupMetaDataPtr;
use parquet::file::reader::RowGroupReader;

pub struct RowGroup {
    pub metadata: RowGroupMetaDataPtr,
    pub columns: Vec<ParquetColumn>,
}

impl RowGroup {
    pub fn new(metadata: RowGroupMetaDataPtr, reader: &Box<dyn RowGroupReader>) -> Self {
        let num_columns = metadata.num_columns() as usize;
        let columns = (0..num_columns)
            .into_iter()
            .map(|column_index| {
                ParquetColumn::read(
                    column_index,
                    metadata.column(column_index).num_values() as usize,
                    reader,
                )
                .unwrap()
            })
            .collect();

        Self {
            metadata: metadata,
            columns: columns,
        }
    }
}
