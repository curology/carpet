use parquet::column::reader::ColumnReader;
use parquet::column::writer::ColumnWriter;
use parquet::data_type::ByteArray;
use parquet::memory::BufferPtr;
use std::io::Error;

pub struct ParquetColumn {
    int_values: Vec<i32>,
    byte_values: Vec<ByteArray>,
    def_levels: Vec<i16>,
    rep_levels: Vec<i16>,
    pub is_dirty: bool,
}

fn find_subsequence(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    haystack
        .windows(needle.len())
        .position(|window| window == needle)
}

impl ParquetColumn {
    pub fn read(
        column_index: usize,
        num_values: usize,
        row_group_reader: &dyn parquet::file::reader::RowGroupReader,
    ) -> Result<ParquetColumn, Error> {
        let mut def_levels = vec![Default::default(); num_values];
        let mut rep_levels = vec![Default::default(); num_values];
        let batch_size = num_values;
        let mut column_reader = row_group_reader.get_column_reader(column_index).unwrap();
        match column_reader {
            ColumnReader::ByteArrayColumnReader(ref mut typed_reader) => {
                let mut values = vec![Default::default(); num_values];
                while let Ok((read, _)) = typed_reader.read_batch(
                    batch_size, // batch size
                    Some(&mut def_levels),
                    Some(&mut rep_levels),
                    &mut values,
                ) {
                    if read < batch_size {
                        return Ok(ParquetColumn::from_bytes(values, def_levels, rep_levels));
                    }
                }
            }
            ColumnReader::Int32ColumnReader(ref mut typed_reader) => {
                let mut values = vec![Default::default(); num_values];
                while let Ok((read, _)) = typed_reader.read_batch(
                    batch_size, // batch size
                    Some(&mut def_levels),
                    Some(&mut rep_levels),
                    &mut values,
                ) {
                    if read < batch_size {
                        return Ok(ParquetColumn::from_int(values, def_levels, rep_levels));
                    }
                }
            }
            _ => unimplemented!(),
        }
        Ok(ParquetColumn::from_bytes(vec![], def_levels, rep_levels))
    }
    fn from_bytes(values: Vec<ByteArray>, def_levels: Vec<i16>, rep_levels: Vec<i16>) -> Self {
        Self {
            int_values: vec![],
            byte_values: values,
            def_levels,
            rep_levels,
            is_dirty: false,
        }
    }
    fn from_int(values: Vec<i32>, def_levels: Vec<i16>, rep_levels: Vec<i16>) -> Self {
        Self {
            int_values: values,
            byte_values: vec![],
            def_levels,
            rep_levels,
            is_dirty: false,
        }
    }

    pub fn write(
        &self,
        writer: &mut dyn parquet::file::writer::RowGroupWriter,
    ) -> Result<(), Error> {
        if let Some(mut col_writer) = writer.next_column().unwrap() {
            match col_writer {
                ColumnWriter::ByteArrayColumnWriter(ref mut typed_writer) => {
                    typed_writer
                        .write_batch(
                            &self.byte_values,
                            Some(self.def_levels.as_slice()),
                            Some(self.rep_levels.as_slice()),
                        )
                        .unwrap();
                }
                ColumnWriter::Int32ColumnWriter(ref mut typed_writer) => {
                    typed_writer
                        .write_batch(
                            &self.int_values,
                            Some(self.def_levels.as_slice()),
                            Some(self.rep_levels.as_slice()),
                        )
                        .unwrap();
                }
                _ => {}
            }
            writer.close_column(col_writer).unwrap();
        }
        Ok(())
    }

    pub fn remove_values(&mut self, search_values: Vec<String>, replace: String) {
        let mut matches: Vec<Vec<usize>> = vec![];
        // Find which values match and store their indexes.
        for (data_index, data_value) in self.byte_values.iter().enumerate() {
            if let Ok(utf8_value) = data_value.as_utf8() {
                let byte_value = utf8_value.as_bytes();
                for (email_index, search) in search_values.iter().enumerate() {
                    // Short circuit if the search term is longer than the value.
                    if search.len() > utf8_value.len() {
                        continue;
                    }

                    // Search the value for any matches.
                    let found_index = find_subsequence(byte_value, search.as_bytes());
                    if found_index != None {
                        matches.push(vec![data_index, email_index]);
                    }
                }
            }
        }

        // Go through the matches and replace the search string with the replacement.
        for email_match in matches {
            let val_index = email_match[0];
            let email_index = email_match[1];
            // Construct the new value.
            let data = String::from(self.byte_values[val_index].as_utf8().unwrap())
                .replace(&search_values[email_index], replace.as_str());
            // Overwrite the old value with the updated one.
            self.byte_values[val_index].set_data(BufferPtr::new(data.as_bytes().to_vec()));
            self.is_dirty = true;
        }
    }
}
