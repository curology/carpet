use std::path::Path;

mod parquet_file;
use parquet_file::ParquetFile;

fn main() {
    let email_to_remove = String::from("bkassidy98@gmail.com");
    let email_to_replace = String::from("ghost@curology.com");

    let file_paths = vec![Path::new("***REMOVED***")];

    for file_path in file_paths {
        let mut file = ParquetFile::read(file_path);
        for group in file.row_groups.iter_mut() {
            for ref mut column in group.columns.iter_mut() {
                column.remove_value(email_to_remove.clone(), email_to_replace.clone());
            }
        }
        file.write_to(Path::new(
            "***REMOVED***",
        ))
        .unwrap();
    }

    // let wrote_file = File::open(&Path::new(
    //     "***REMOVED***",
    // ))
    // .unwrap();
    // let wrote_reader = SerializedFileReader::new(wrote_file).unwrap();
    // let mut iter = wrote_reader.get_row_iter(None).unwrap();
    // let meta = wrote_reader.metadata();
    // let rgcount = meta.num_row_groups();
    // let nr = meta.row_group(0).num_rows() as usize;

    // println!("Row Groups: {}", rgcount);
    // println!("Rows: {}", nr);
    // // println!("Columns: {}", row_group_metadata.num_columns());
    // let mut i = 200;
    // while let Some(record) = iter.next() {
    //     println!("entries: {}", record);
    //     i -= 1;
    //     if i == 0 {
    //         break;
    //     }
    // }
}
