use parquet::column::reader::ColumnReader;
use parquet::column::writer::ColumnWriter;
use parquet::data_type::ByteArray;
use parquet::data_type::Int96;
use parquet::memory::BufferPtr;
use std::io::Error;

#[derive(Default)]
pub struct ParquetColumn {
    int_values: Vec<i32>,
    int64_values: Vec<i64>,
    int96_values: Vec<Int96>,
    byte_values: Vec<ByteArray>,
    float_values: Vec<f32>,
    double_values: Vec<f64>,
    bool_values: Vec<bool>,
    def_levels: Vec<i16>,
    rep_levels: Vec<i16>,
    pub is_dirty: bool,
}

fn find_subsequence(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    haystack
        .windows(needle.len())
        .position(|window| window == needle)
}

macro_rules! gen_converter {
    ($name:ident, $field:ident, $native_type:ty) => {
        fn $name(values: Vec<$native_type>, def_levels: Vec<i16>, rep_levels: Vec<i16>) -> Self {
            Self {
                $field: values,
                def_levels,
                rep_levels,
                is_dirty: false,
                ..Default::default()
            }
        }
    };
}

macro_rules! gen_reader {
    ($i:ident, $tr:ident, $bs:ident, $def:ident, $rep:ident) => {
        let mut values = vec![Default::default(); $bs];
        while let Ok((read, _)) = $tr.read_batch(
            $bs, // batch size
            Some(&mut $def),
            Some(&mut $rep),
            &mut values,
        ) {
            if read < $bs {
                return Ok(ParquetColumn::$i(values, $def, $rep));
            }
        }
    };
}

macro_rules! gen_writer {
    ($self:ident, $writer:ident, $values:ident) => {
        $writer
            .write_batch(
                &$self.$values,
                Some($self.def_levels.as_slice()),
                Some($self.rep_levels.as_slice()),
            )
            .unwrap();
    };
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
                gen_reader!(from_bytes, typed_reader, batch_size, def_levels, rep_levels);
            }
            ColumnReader::Int32ColumnReader(ref mut typed_reader) => {
                gen_reader!(from_int32, typed_reader, batch_size, def_levels, rep_levels);
            }
            ColumnReader::Int96ColumnReader(ref mut typed_reader) => {
                gen_reader!(from_int96, typed_reader, batch_size, def_levels, rep_levels);
            }
            ColumnReader::BoolColumnReader(ref mut typed_reader) => {
                gen_reader!(from_bool, typed_reader, batch_size, def_levels, rep_levels);
            }
            ColumnReader::FloatColumnReader(ref mut typed_reader) => {
                gen_reader!(from_float, typed_reader, batch_size, def_levels, rep_levels);
            }
            ColumnReader::DoubleColumnReader(ref mut typed_reader) => {
                gen_reader!(
                    from_double,
                    typed_reader,
                    batch_size,
                    def_levels,
                    rep_levels
                );
            }
            ColumnReader::Int64ColumnReader(ref mut typed_reader) => {
                gen_reader!(from_int64, typed_reader, batch_size, def_levels, rep_levels);
            }
            _ => unimplemented!(),
        }
        Ok(ParquetColumn::from_bytes(vec![], def_levels, rep_levels))
    }

    gen_converter!(from_bool, bool_values, bool);
    gen_converter!(from_bytes, byte_values, ByteArray);
    gen_converter!(from_int32, int_values, i32);
    gen_converter!(from_int64, int64_values, i64);
    gen_converter!(from_int96, int96_values, Int96);
    gen_converter!(from_float, float_values, f32);
    gen_converter!(from_double, double_values, f64);

    pub fn write(
        &self,
        writer: &mut dyn parquet::file::writer::RowGroupWriter,
    ) -> Result<(), Error> {
        if let Some(mut col_writer) = writer.next_column().unwrap() {
            match col_writer {
                ColumnWriter::ByteArrayColumnWriter(ref mut typed_writer) => {
                    gen_writer!(self, typed_writer, byte_values);
                }
                ColumnWriter::Int32ColumnWriter(ref mut typed_writer) => {
                    gen_writer!(self, typed_writer, int_values);
                }
                ColumnWriter::Int64ColumnWriter(ref mut typed_writer) => {
                    gen_writer!(self, typed_writer, int64_values);
                }
                ColumnWriter::Int96ColumnWriter(ref mut typed_writer) => {
                    gen_writer!(self, typed_writer, int96_values);
                }
                ColumnWriter::BoolColumnWriter(ref mut typed_writer) => {
                    gen_writer!(self, typed_writer, bool_values);
                }
                ColumnWriter::FloatColumnWriter(ref mut typed_writer) => {
                    gen_writer!(self, typed_writer, float_values);
                }
                ColumnWriter::DoubleColumnWriter(ref mut typed_writer) => {
                    gen_writer!(self, typed_writer, double_values);
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
