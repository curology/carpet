use parquet::column::reader::ColumnReader;
use parquet::column::writer::ColumnWriter;
use parquet::file::properties::WriterProperties;
use parquet::file::reader::{FileReader, SerializedFileReader};
use parquet::file::writer::{FileWriter, RowGroupWriter, SerializedFileWriter};

// use parquet::record::Row;
// use parquet::record::RowAccessor;

// use parquet::column::reader::get_typed_column_reader;
use parquet::data_type::{ByteArrayType, Int32Type, Int64Type};

use std::fs;
use std::fs::File;
use std::path::Path;
use std::rc::Rc;

use parquet::basic::Type;

use parquet::schema::printer::{print_file_metadata, print_parquet_metadata, print_schema};

use std::str;

trait RowVec {}

fn main() {
    let file = File::open(&Path::new(
        "***REMOVED***",
    ))
    .unwrap();
    let reader = SerializedFileReader::new(file).unwrap();
    let mut iter = reader.get_row_iter(None).unwrap();
    let metadata = reader.metadata();
    let row_group_count = metadata.num_row_groups();

    // print_metadata(&mut std::io::stdout(), &metadata);
    println!("{}", row_group_count);

    let out_path = Path::new("***REMOVED***");
    let schema = Rc::new(metadata.file_metadata().schema().clone());
    let props = Rc::new(WriterProperties::builder().build());
    let file = fs::File::create(&out_path).unwrap();

    let mut writer = SerializedFileWriter::new(file, schema, props).unwrap();
    let mut i = 0;

    let mut string_types: Vec<Vec<String>> = vec![];
    let mut int_types: Vec<Vec<i32>> = vec![];
    let mut indexes: Vec<usize> = vec![];

    let row_group_metadata = metadata.row_group(0);
    for ptr in row_group_metadata.columns().into_iter() {
        if ptr.column_type() == Type::INT32 {
            int_types.push(vec![]);
            indexes.push(int_types.len() - 1);
        }

        if ptr.column_type() == Type::BYTE_ARRAY {
            string_types.push(vec![]);
            indexes.push(string_types.len() - 1);
        }
    }
    for i in 0..row_group_count {
        // Group meta
        let row_group_metadata = metadata.row_group(i);
        // Mutable values for row
        let row_group_reader = reader.get_row_group(i).unwrap();
        let mut row_group_writer = writer.next_row_group().unwrap();
        let num_rows = row_group_metadata.num_rows() as usize;

        println!("Row Groups: {}", row_group_count);
        println!("Rows: {}", num_rows);
        println!("Columns: {}", row_group_metadata.num_columns());

        for j in 0..row_group_metadata.num_columns() {
            let mut string_values: Vec<parquet::data_type::ByteArray> =
                vec![Default::default(); num_rows];
            let mut int_values: Vec<i32> = vec![Default::default(); num_rows];
            let mut def_levels = vec![Default::default(); num_rows];
            let mut rep_levels = vec![Default::default(); num_rows];
            let mut column_reader = row_group_reader.get_column_reader(j).unwrap();
            let batch_size = 20;
            match column_reader {
                ColumnReader::ByteArrayColumnReader(ref mut typed_reader) => {
                    while let Ok((read, _)) = typed_reader.read_batch(
                        batch_size, // batch size
                        Some(&mut def_levels),
                        Some(&mut rep_levels),
                        &mut string_values,
                    ) {
                        if read < batch_size {
                            println!("{}", read);
                            break;
                        }
                    }
                }
                ColumnReader::Int32ColumnReader(ref mut typed_reader) => {
                    while let Ok((read, _)) = typed_reader.read_batch(
                        batch_size, // batch size
                        Some(&mut def_levels),
                        Some(&mut rep_levels),
                        &mut int_values,
                    ) {
                        if read < batch_size {
                            break;
                        }
                    }
                }
                _ => {}
            }

            if let Some(mut col_writer) = row_group_writer.next_column().unwrap() {
                match col_writer {
                    ColumnWriter::ByteArrayColumnWriter(ref mut typed_writer) => {
                        typed_writer
                            .write_batch(&string_values, Some(&def_levels), Some(&rep_levels))
                            .unwrap();
                    }
                    ColumnWriter::Int32ColumnWriter(ref mut typed_writer) => {
                        typed_writer
                            .write_batch(&int_values, Some(&def_levels), Some(&rep_levels))
                            .unwrap();
                    }
                    _ => {}
                }
                row_group_writer.close_column(col_writer).unwrap();
            }
        }
        // println!("{:?}", values.len());
        // for v in values {
        //     println!("{:?}", str::from_utf8(v.data()));
        // }
        writer.close_row_group(row_group_writer).unwrap();
    }
    writer.close().unwrap();

    let wrote_file = File::open(&Path::new(
        "***REMOVED***",
    ))
    .unwrap();
    let wrote_reader = SerializedFileReader::new(wrote_file).unwrap();
    let mut iter = wrote_reader.get_row_iter(None).unwrap();
    let meta = wrote_reader.metadata();
    let rgcount = meta.num_row_groups();
    let nr = meta.row_group(0).num_rows() as usize;

    println!("Row Groups: {}", rgcount);
    println!("Rows: {}", nr);
    // println!("Columns: {}", row_group_metadata.num_columns());
    let mut i = 200;
    while let Some(record) = iter.next() {
        println!("entries: {}", record);
        i -= 1;
        if i == 0 {
            break;
        }
    }
}
