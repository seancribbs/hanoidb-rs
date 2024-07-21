use std::fs::File;
use std::io::{Read, Seek, SeekFrom};
use std::path::Path;
const TAG_KV_DATA: u8 = 0x80;
const TAG_DELETED: u8 = 0x81;
const TAG_POSLEN32: u8 = 0x82;
#[allow(dead_code)]
const TAG_TRANSACT: u8 = 0x83;
const TAG_KV_DATA2: u8 = 0x84;
const TAG_DELETED2: u8 = 0x85;
const TAG_END: u8 = 0xFF;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("invalid tree format")]
    InvalidTreeFormat(Vec<u8>),

    #[error("corrupted file: {0}")]
    CorruptedFile(&'static str),

    #[error("invalid compression type: {0}")]
    InvalidCompression(u8),

    #[error("incorrect block length, expected {0}, got {1}")]
    IncorrectBlockLength(u32, u32),

    #[error("expected PosLen entry")]
    PosLenEntryRequired,

    #[error("invalid entry tag {0}")]
    InvalidEntryTag(u8),

    #[error("internal buffer conversion error: {0}")]
    SliceConversion(#[from] std::array::TryFromSliceError),

    #[error("{0}")]
    Io(#[from] std::io::Error),
}

pub type Result<T> = std::result::Result<T, Error>;

pub struct Tree {
    file: File,
    len: u64,
}

impl Tree {
    pub fn from_file(path: impl AsRef<Path>) -> Result<Self> {
        let mut file = File::open(path)?;
        let len = file.metadata()?.len();
        let mut magic: Vec<u8> = vec![0; 4];
        file.read_exact(&mut magic)?;
        if magic == "HAN2".as_bytes() {
            Ok(Self { file, len })
        } else {
            Err(Error::InvalidTreeFormat(magic))
        }
    }

    pub fn root_block(&self) -> Result<Block<'_>> {
        let trailer = self.trailer()?;
        let start = trailer.root_pos;
        Block::from_start(&self.file, start)
    }

    pub fn block_from_poslen_entry(&self, entry: &Entry) -> Result<Block<'_>> {
        let Entry::PosLen {
            blockpos, blocklen, ..
        } = entry
        else {
            return Err(Error::PosLenEntryRequired);
        };
        Block::from_start_length(&self.file, *blockpos, *blocklen)
    }

    pub fn trailer(&self) -> Result<Trailer> {
        let mut file = &self.file;
        file.seek(SeekFrom::End(-12))?; // bloom_len: 4, root_pos: 8
        let mut buffer = vec![0; 12];
        file.read_exact(&mut buffer)?;
        let root_pos = u64::from_be_bytes(buffer[4..].try_into()?);
        let bloom_len = u32::from_be_bytes(buffer[0..4].try_into()?);
        let bloom_start = bloom_len as i64 + 12;
        file.seek(SeekFrom::End(-bloom_start - 4))?;
        let mut padding = vec![0; 4];
        file.read_exact(&mut padding)?;
        if padding[..] != [0, 0, 0, 0] {
            return Err(Error::CorruptedFile("missing trailer padding"));
        }
        let mut bloom = vec![0; bloom_len as usize];
        file.read_exact(&mut bloom)?;
        if root_pos >= self.len {
            return Err(Error::CorruptedFile(
                "root block position outside bounds of file",
            ));
        }
        Ok(Trailer {
            bloom,
            bloom_len,
            root_pos,
        })
    }
}

#[derive(Debug)]
pub struct Trailer {
    bloom: Vec<u8>,
    bloom_len: u32,
    root_pos: u64,
}

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

#[derive(Debug)]
pub struct Block<'a> {
    pub start: u64,
    pub blocklen: u32,
    pub level: u16,
    pub compression: Compression,
    file: &'a File,
}

impl<'a> Block<'a> {
    fn from_start(mut file: &'a File, start: u64) -> Result<Self> {
        file.seek(SeekFrom::Start(start))?;
        let mut header = vec![0; 7];
        file.read_exact(&mut header)?;
        let blocklen = u32::from_be_bytes(header[0..4].try_into()?);
        let level = u16::from_be_bytes(header[4..6].try_into()?);
        let compression: Compression = header[6].try_into()?;

        Ok(Self {
            start,
            blocklen,
            level,
            compression,
            file,
        })
    }

    fn from_start_length(file: &'a File, start: u64, length: u32) -> Result<Self> {
        let block = Self::from_start(file, start)?;
        let expected_length = length - 4;
        if block.blocklen == expected_length {
            Ok(block)
        } else {
            Err(Error::IncorrectBlockLength(expected_length, block.blocklen))
        }
    }

    pub fn entries(&self) -> EntryIterator<'a> {
        EntryIterator {
            file: self.file,
            start: self.start + 7, // Skip the header part of the block (7 bytes)
            end: self.start + (self.blocklen as u64) - 1, // entries always end in TAG_END
        }
    }
}

#[derive(Debug, Clone)]
#[allow(dead_code)]
pub enum Entry {
    KeyVal {
        key: Vec<u8>,
        value: Vec<u8>,
        timestamp: Option<u32>,
    },
    Deleted {
        key: Vec<u8>,
        timestamp: Option<u32>,
    },
    PosLen {
        blockpos: u64,
        blocklen: u32,
        key: Vec<u8>,
    },
}

impl Entry {
    fn read(mut file: &File) -> Result<(Self, u64)> {
        let mut header = vec![0; 9];
        file.read_exact(&mut header)?;
        if header[0] != TAG_END {
            return Err(Error::CorruptedFile("First byte of entry wasn't TAG_END"));
        }
        let length = u32::from_be_bytes(header[1..5].try_into()?);
        let orig_crc = u32::from_be_bytes(header[5..9].try_into()?);
        let mut entry_data = vec![0; length as usize];
        file.read_exact(&mut entry_data)?;
        let crc = crc32fast::hash(&entry_data);
        if crc != orig_crc {
            return Err(Error::CorruptedFile("Entry had incorrect CRC32"));
        }

        let entry = match entry_data[0] {
            TAG_KV_DATA => {
                let keylen = u32::from_be_bytes(entry_data[1..5].try_into()?);
                let mut key = entry_data.split_off(5);
                let value = key.split_off(keylen as usize);
                Self::KeyVal {
                    key,
                    value,
                    timestamp: None,
                }
            }
            TAG_KV_DATA2 => {
                let timestamp = u32::from_be_bytes(entry_data[1..5].try_into()?);
                let keylen = u32::from_be_bytes(entry_data[5..9].try_into()?);
                let mut key = entry_data.split_off(9);
                let value = key.split_off(keylen as usize);
                Self::KeyVal {
                    key,
                    value,
                    timestamp: Some(timestamp),
                }
            }
            TAG_DELETED => {
                // let keylen = u32::from_be_bytes(entry_data[1..5].try_into()?);
                let key = entry_data.split_off(5);
                Self::Deleted {
                    key,
                    timestamp: None,
                }
            }
            TAG_DELETED2 => {
                let timestamp = u32::from_be_bytes(entry_data[1..5].try_into()?);
                // let keylen = u32::from_be_bytes(entry_data[5..9].try_into()?);
                let key = entry_data.split_off(9);
                Self::Deleted {
                    key,
                    timestamp: Some(timestamp),
                }
            }
            TAG_POSLEN32 => {
                let blockpos = u64::from_be_bytes(entry_data[1..9].try_into()?);
                let blocklen = u32::from_be_bytes(entry_data[9..13].try_into()?);
                let key = entry_data.split_off(13);
                Self::PosLen {
                    blockpos,
                    blocklen,
                    key,
                }
            }
            tag => {
                return Err(Error::InvalidEntryTag(tag));
            }
        };
        Ok((entry, 9 + length as u64))
    }
}

pub struct EntryIterator<'a> {
    file: &'a File,
    start: u64,
    end: u64,
}

impl<'a> Iterator for EntryIterator<'a> {
    type Item = Entry;

    fn next(&mut self) -> Option<Self::Item> {
        if self.start >= self.end {
            return None;
        }

        if self.file.seek(SeekFrom::Start(self.start)).is_err() {
            // TODO: Don't swallow the result of the above call
            // Ensure iterator terminates when there's an IO problem
            self.start = self.end;
            return None;
        }

        match Entry::read(self.file) {
            Ok((entry, read_amount)) => {
                self.start += read_amount;
                Some(entry)
            }
            Err(_) => {
                // Ensure iterator terminates when there's a problem reading an Entry
                self.start = self.end;
                None
            }
        }
    }
}
