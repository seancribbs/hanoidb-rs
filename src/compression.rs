use std::io::{Read, Write};

use crate::error::*;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum Compression {
    None = 0,
    Snappy = 1,
    Gzip = 2,
    Lz4 = 3,
}

impl TryFrom<u8> for Compression {
    type Error = Error;

    fn try_from(value: u8) -> Result<Compression> {
        match value {
            0 => Ok(Compression::None),
            1 => Ok(Compression::Snappy),
            2 => Ok(Compression::Gzip),
            3 => Ok(Compression::Lz4),
            _ => Err(Error::InvalidCompression(value)),
        }
    }
}

impl Compression {
    pub fn compress(&self, input: Vec<u8>) -> Result<Vec<u8>> {
        match self {
            Compression::None => Ok(input),
            Compression::Snappy => Ok(snap::raw::Encoder::new().compress_vec(&input)?),
            Compression::Gzip => {
                let mut writer = flate2::write::GzEncoder::new(
                    Vec::with_capacity(input.len()),
                    Default::default(),
                );
                writer.write_all(&input)?;
                Ok(writer.finish()?)
            }
            Compression::Lz4 => {
                let capacity = lz4_flex::block::get_maximum_output_size(input.len());
                let mut writer = lz4_flex::frame::FrameEncoder::new(Vec::with_capacity(capacity));
                writer.write_all(&input)?;
                Ok(writer.finish()?)
            }
        }
    }

    pub fn reader<'a>(&self, r: impl Read + 'a) -> Box<dyn Read + 'a> {
        match self {
            Compression::None => Box::new(r),
            Compression::Snappy => Box::new(snap::read::FrameDecoder::new(r)),
            Compression::Gzip => Box::new(flate2::read::GzDecoder::new(r)),
            Compression::Lz4 => Box::new(lz4_flex::frame::FrameDecoder::new(r)),
        }
    }
}
