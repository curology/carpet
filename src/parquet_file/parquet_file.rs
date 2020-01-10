use parquet::file::metadata::ParquetMetaDataPtr;
use parquet::file::reader::{FileReader, SerializedFileReader};

use std::fs::File;
use std::io::Error;
use std::path::Path;

use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;
use parquet::file::writer::{FileWriter, SerializedFileWriter};
use std::rc::Rc;

use super::RowGroup;

pub struct ParquetFile {
    pub row_groups: Vec<RowGroup>,
    pub metadata: ParquetMetaDataPtr,
}

impl ParquetFile {
    pub fn write_to(&self, file_path: &Path) -> Result<(), Error> {
        let schema = Rc::new(self.metadata.file_metadata().schema().clone());
        let props = Rc::new(
            WriterProperties::builder()
                .set_compression(Compression::SNAPPY)
                .build(),
        );
        let file = File::create(&file_path).unwrap();

        let mut writer = SerializedFileWriter::new(file, schema, props).unwrap();
        for group in self.row_groups.iter() {
            let mut row_group_writer = writer.next_row_group().unwrap();
            for column in group.columns.iter() {
                column.write(&mut row_group_writer).unwrap();
            }
            writer.close_row_group(row_group_writer).unwrap();
        }
        writer.close().unwrap();
        Ok(())
    }

    pub fn read(file_path: &Path) -> Self {
        let file = File::open(file_path).unwrap();
        let reader = SerializedFileReader::new(file).unwrap();
        let meta = reader.metadata();
        let row_groups = (0..meta.num_row_groups())
            .into_iter()
            .map(|i| {
                let row_group_metadata = meta.row_group(i);
                let row_group_reader = reader.get_row_group(i).unwrap();
                RowGroup::new(row_group_metadata, &row_group_reader)
            })
            .collect();

        Self {
            row_groups: row_groups,
            metadata: meta,
        }
    }
}
