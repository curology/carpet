use std::env;
use std::path::Path;

mod parquet_file;
use parquet_file::ParquetFile;

fn main() {
    let args: Vec<String> = env::args().collect();
    let email_to_remove = args[1].clone();
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
}
