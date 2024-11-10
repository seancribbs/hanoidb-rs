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
