use std::env;
use std::ffi::OsString;
use std::fs::{copy, read_dir, remove_file, DirEntry};

mod carpet_parquet;
use carpet_parquet::ParquetFile;

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
        let mut file = ParquetFile::read(file_path.path().as_ref());
        for group in file.row_groups.iter_mut() {
            let columns = &mut group.columns.iter_mut();
            for column in columns {
                column.remove_value(email_to_remove.clone(), email_to_replace.clone());
            }
        }
        // Make a backup before writing.
        let mut backup_path = file_path.path();
        backup_path.set_extension("parquet.backup");
        copy(file_path.path(), backup_path.clone()).unwrap();

        // Write the final file, if we fail then restore the backup.
        match file.write_to(file_path.path().as_ref()) {
            Ok(_) => {
                remove_file(backup_path).unwrap();
            }
            Err(err) => {
                copy(&backup_path, file_path.path()).unwrap();
                remove_file(&backup_path).unwrap();
                panic!("Error while writing file: {:?}", err)
            }
        };
    }
}
