use std::io::{BufReader, Read, Seek, SeekFrom};

use itertools::Itertools;

const FOOTER_OFFSET_LENGTH: usize = 2;
const FOOTER_RESTART_COUNT_LENGTH: usize = 4;

#[derive(Debug)]
pub enum BlockDecoderError {
    Decode,
}

pub struct BlockDecoder {
    restart_offsets: Vec<usize>,
    restart_count: usize,
}

impl BlockDecoder {
    pub fn decode<R: Read + Seek>(data: R, len: usize) -> Result<Self, BlockDecoderError> {
        let mut r = BufReader::new(data);
        r.seek(std::io::SeekFrom::Start(
            (len - FOOTER_RESTART_COUNT_LENGTH) as u64,
        ))
        .map_err(|_| BlockDecoderError::Decode)?;

        let mut restart_offsets_count_buff = vec![0; FOOTER_RESTART_COUNT_LENGTH];
        r.read_exact(&mut restart_offsets_count_buff)
            .map_err(|_| BlockDecoderError::Decode)?;

        let restart_offsets_count_bytes = restart_offsets_count_buff
            .into_iter()
            .map(|byte| byte - 48)
            .join("");
        let restart_offsets_count = usize::from_str_radix(&restart_offsets_count_bytes, 16)
            .map_err(|_| BlockDecoderError::Decode)?;

        let restart_offsets_section_len = restart_offsets_count * FOOTER_OFFSET_LENGTH;

        let restart_offsets_section_offset_end =
            restart_offsets_section_len + FOOTER_RESTART_COUNT_LENGTH;

        r.seek(SeekFrom::Start(
            (len - restart_offsets_section_offset_end) as u64,
        ))
        .map_err(|_| BlockDecoderError::Decode)?;

        let mut restart_offsets_section_buf = vec![0; restart_offsets_section_len];
        r.read_exact(&mut restart_offsets_section_buf)
            .map_err(|_| BlockDecoderError::Decode)?;

        let restart_offsets_section_bytes_chunks = restart_offsets_section_buf
            .into_iter()
            .chunks(FOOTER_OFFSET_LENGTH);
        let restart_offsets_section_bytes_iter = restart_offsets_section_bytes_chunks.into_iter();

        let mut restart_offsets = Vec::<usize>::new();

        for mut offset in restart_offsets_section_bytes_iter {
            let offset_bytes = offset.join("");
            let parsed_offset =
                usize::from_str_radix(&offset_bytes, 16).map_err(|_| BlockDecoderError::Decode)?;
            restart_offsets.push(parsed_offset);
        }

        Ok(Self {
            restart_count: restart_offsets_count,
            restart_offsets,
        })
    }
    pub fn restart_offsets(&self) -> &Vec<usize> {
        &self.restart_offsets
    }
    pub fn restart_offsets_count(&self) -> usize {
        self.restart_count
    }
}
