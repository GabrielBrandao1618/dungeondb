use std::{
    io::{BufReader, Cursor, Read, Seek, SeekFrom},
    str::Chars,
};

use itertools::Itertools;

use super::entry::Entry;

const FOOTER_OFFSET_LENGTH: usize = 2;
const FOOTER_RESTART_COUNT_LENGTH: usize = 4;

#[derive(Debug)]
enum BlockBuilderError {
    Decode,
}

pub struct BlockBuilder {
    entries: Vec<Entry>,
    restart_offsets: Vec<usize>,
    restart_count: usize,
}

impl BlockBuilder {
    pub fn new() -> Self {
        Self {
            entries: Vec::new(),
            restart_offsets: Vec::new(),
            restart_count: 0,
        }
    }
    /// Layout: `[entries][restart offsets][restart count section]`
    pub fn encode(&self) -> String {
        let entries_data_section = self.entries.iter().map(|entry| entry.encode()).join("");
        let restart_count_section = format!(
            "{val:0length$x}",
            val = self.restart_count,
            length = FOOTER_RESTART_COUNT_LENGTH
        );
        let restart_offsets_section = self
            .restart_offsets
            .iter()
            .map(|offset| {
                format!(
                    "{offset:0length$x}",
                    offset = offset,
                    length = FOOTER_OFFSET_LENGTH
                )
            })
            .join("");

        format!("{entries_data_section}{restart_offsets_section}{restart_count_section}")
    }
    pub fn decode<R: Read + Seek>(data: R, len: usize) -> Result<Self, BlockBuilderError> {
        let mut r = BufReader::new(data);
        r.seek(std::io::SeekFrom::End(FOOTER_RESTART_COUNT_LENGTH as i64))
            .map_err(|_| BlockBuilderError::Decode)?;

        let mut restart_offsets_count_buff = vec![0; FOOTER_RESTART_COUNT_LENGTH];
        r.read_exact(&mut restart_offsets_count_buff)
            .map_err(|_| BlockBuilderError::Decode)?;

        let restart_offsets_count_bytes = restart_offsets_count_buff.into_iter().join("");
        let restart_offsets_count = usize::from_str_radix(&restart_offsets_count_bytes, 16)
            .map_err(|_| BlockBuilderError::Decode)?;

        let restart_offsets_section_len = restart_offsets_count * FOOTER_OFFSET_LENGTH;

        let restart_offsets_section_offset_end =
            restart_offsets_section_len + FOOTER_RESTART_COUNT_LENGTH;

        r.seek(SeekFrom::End(restart_offsets_section_offset_end as i64))
            .map_err(|_| BlockBuilderError::Decode)?;

        let mut restart_offsets_section_buf = vec![0; restart_offsets_section_len];
        r.read_exact(&mut restart_offsets_section_buf)
            .map_err(|_| BlockBuilderError::Decode)?;

        let restart_offsets_section_bytes_chunks = restart_offsets_section_buf
            .into_iter()
            .chunks(FOOTER_OFFSET_LENGTH);
        let restart_offsets_section_bytes_iter = restart_offsets_section_bytes_chunks.into_iter();

        let mut restart_offsets = Vec::<usize>::new();

        for mut offset in restart_offsets_section_bytes_iter {
            let offset_bytes = offset.join("");
            let parsed_offset =
                usize::from_str_radix(&offset_bytes, 16).map_err(|_| BlockBuilderError::Decode)?;
            restart_offsets.push(parsed_offset);
        }

        Ok(Self {
            restart_count: restart_offsets_count,
            restart_offsets,
            entries: vec![],
        })
    }
}
