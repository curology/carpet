use parquet::column::reader::ColumnReader;
use parquet::column::writer::ColumnWriter;
use parquet::file::properties::WriterProperties;
use parquet::file::reader::{FileReader, SerializedFileReader};
use parquet::file::writer::{FileWriter, SerializedFileWriter};

use parquet::data_type::ByteArray;

use std::fs;
use std::fs::File;
use std::path::Path;
use std::rc::Rc;

use parquet::schema::printer::print_parquet_metadata;

use std::io::Error;

struct ParquetColumn {
    int_values: Vec<i32>,
    byte_values: Vec<ByteArray>,
    def_levels: Vec<i16>,
    rep_levels: Vec<i16>,
}

fn read_column(
    column_index: usize,
    num_rows: usize,
    row_group_reader: &Box<dyn parquet::file::reader::RowGroupReader>,
) -> Result<ParquetColumn, Error> {
    let mut def_levels = vec![Default::default(); num_rows];
    let mut rep_levels = vec![Default::default(); num_rows];
    let batch_size = num_rows;
    let mut column_reader = row_group_reader.get_column_reader(column_index).unwrap();
    match column_reader {
        ColumnReader::ByteArrayColumnReader(ref mut typed_reader) => {
            let mut values = vec![Default::default(); num_rows];
            while let Ok((read, _)) = typed_reader.read_batch(
                batch_size, // batch size
                Some(&mut def_levels),
                Some(&mut rep_levels),
                &mut values,
            ) {
                if read < batch_size {
                    return Ok(ParquetColumn {
                        byte_values: values,
                        int_values: vec![],
                        def_levels: def_levels,
                        rep_levels: rep_levels,
                    });
                }
            }
        }
        ColumnReader::Int32ColumnReader(ref mut typed_reader) => {
            let mut values = vec![Default::default(); num_rows];
            while let Ok((read, _)) = typed_reader.read_batch(
                batch_size, // batch size
                Some(&mut def_levels),
                Some(&mut rep_levels),
                &mut values,
            ) {
                if read < batch_size {
                    return Ok(ParquetColumn {
                        byte_values: vec![],
                        int_values: values,
                        def_levels: def_levels,
                        rep_levels: rep_levels,
                    });
                }
            }
        }
        _ => unimplemented!(),
    }
    Ok(ParquetColumn {
        byte_values: vec![],
        int_values: vec![],
        def_levels: def_levels,
        rep_levels: rep_levels,
    })
}

fn main() {
    let file = File::open(&Path::new(
        "***REMOVED***",
    ))
    .unwrap();
    let reader = SerializedFileReader::new(file).unwrap();
    let metadata = reader.metadata();
    let row_group_count = metadata.num_row_groups();

    print_parquet_metadata(&mut std::io::stdout(), &metadata);
    println!("{}", row_group_count);

    let out_path = Path::new("***REMOVED***");
    let schema = Rc::new(metadata.file_metadata().schema().clone());
    let props = Rc::new(WriterProperties::builder().build());
    let file = fs::File::create(&out_path).unwrap();

    let mut writer = SerializedFileWriter::new(file, schema, props).unwrap();

    for i in 0..row_group_count {
        // Group meta
        let row_group_metadata = metadata.row_group(i);
        let columns_data = row_group_metadata.columns();
        // Mutable values for row
        let row_group_reader = reader.get_row_group(i).unwrap();
        let mut row_group_writer = writer.next_row_group().unwrap();

        for j in 0..row_group_metadata.num_columns() {
            let column =
                read_column(j, columns_data[j].num_values() as usize, &row_group_reader).unwrap();

            println!("Processing column: {}", j);
            println!("Column Values: {:?}", columns_data[j].num_values());
            // Column writer
            if let Some(mut col_writer) = row_group_writer.next_column().unwrap() {
                match col_writer {
                    ColumnWriter::ByteArrayColumnWriter(ref mut typed_writer) => {
                        let batch = &column.byte_values;
                        let sm_write = typed_writer
                            .write_batch(batch, Some(&column.def_levels), Some(&column.rep_levels))
                            .unwrap();
                        println!("Wrote: {}", sm_write)
                    }
                    ColumnWriter::Int32ColumnWriter(ref mut typed_writer) => {
                        let batch = &column.int_values;
                        typed_writer
                            .write_batch(batch, Some(&column.def_levels), Some(&column.rep_levels))
                            .unwrap();
                    }
                    _ => {}
                }
                println!("Closing column");
                row_group_writer.close_column(col_writer).unwrap();
            }
        }
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
