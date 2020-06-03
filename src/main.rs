use std::env;
use std::ffi::OsString;
use std::fs::{copy, read_dir, remove_file, DirEntry};

mod carpet_parquet;
use carpet_parquet::ParquetFile;

fn main() {
    // Intake arguments.
    let args: Vec<String> = env::args().collect();
    let data_folder = args[1].clone();
    let emails_to_remove: Vec<String> = args[2].clone().split(',').map(String::from).collect();
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
                column.remove_values(emails_to_remove.clone(), email_to_replace.clone());
            }
        }

        if file.is_dirty() {
            // Make a backup before writing.
            let mut backup_path = file_path.path();
            backup_path.set_extension("parquet.backup");
            copy(file_path.path(), backup_path.clone()).unwrap();
            // Write the final file, if we fail then restore the backup.
            match file.write_to(file_path.path().as_ref()) {
                Ok(_) => {}
                Err(err) => {
                    copy(&backup_path, file_path.path()).unwrap();
                    remove_file(&backup_path).unwrap();
                    panic!("Error while writing file: {:?}", err)
                }
            };
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use assert_cmd::Command;
    use tempfile::tempdir;

    #[test]
    fn test_carpet_runs() {
        // Setup
        let dir = tempdir().unwrap();
        let file_path = dir.path().join("record.parquet");
        copy("test/data/record.parquet", file_path).unwrap();

        // Test
        let mut cmd = Command::cargo_bin(env!("CARGO_PKG_NAME")).unwrap();
        let assert = cmd.arg(dir.path()).arg("foo@bar.com").assert();
        assert.success();

        // Assert that a backup is created when there are records replaced
        let backup_path = dir.path().join("record.parquet.backup");
        assert_eq!(true, backup_path.exists());

        // Cleanup
        dir.close().unwrap();
    }
}
