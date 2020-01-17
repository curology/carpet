use std::env;
use std::ffi::OsString;
use std::fs::{read_dir, DirEntry};
use std::path::Path;

mod parquet_file;
use parquet_file::ParquetFile;

fn main() {
    // Intake arguments.
    let args: Vec<String> = env::args().collect();
    let data_folder = args[1].clone();
    let email_to_remove = args[2].clone();
    let email_to_replace = String::from("ghost@curology.com");

    // Open up folder and get file paths.
    let extension = OsString::from("parquet");
    let dir_files = read_dir(data_folder)
        .unwrap()
        .filter_map(Result::ok)
        .filter(|dir_path| dir_path.path().extension() == Some(extension.as_os_str()))
        .collect::<Vec<DirEntry>>();

    // Open each file and remove the offending strings.
    for file_path in dir_files {
        println!("{:?}", file_path);
        let mut file = ParquetFile::read(file_path.path().as_ref());
        for group in file.row_groups.iter_mut() {
            for ref mut column in group.columns.iter_mut() {
                column.remove_value(email_to_remove.clone(), email_to_replace.clone());
            }
        }
        file.write_to(Path::new("out.parquet")).unwrap();
    }
}
