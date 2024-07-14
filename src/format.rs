use std::path::Path;

const TAG_KV_DATA: u8 = 0x80;
const TAG_DELETED: u8 = 0x81;
const TAG_POSLEN32: u8 = 0x82;
#[allow(dead_code)]
const TAG_TRANSACT: u8 = 0x83;
const TAG_KV_DATA2: u8 = 0x84;
const TAG_DELETED2: u8 = 0x85;
const TAG_END: u8 = 0xFF;

pub struct Tree {
    data: Vec<u8>,
}

impl Tree {
    pub fn from_file(path: impl AsRef<Path>) -> std::io::Result<Self> {
        let data = std::fs::read(path)?;
        if &data[0..4] == "HAN2".as_bytes() {
            Ok(Self { data })
        } else {
            Err(std::io::Error::other("invalid tree format"))
        }
    }

    pub fn root_block(&self) -> Option<Block<'_>> {
        let trailer = self.trailer();
        let start = trailer.root_pos as usize;
        Block::new(&self.data[start..])
    }

    pub fn block_from_poslen_entry(&self, entry: Entry<'_>) -> Option<Block<'_>> {
        let Entry::PosLen {
            blockpos, blocklen, ..
        } = entry
        else {
            return None;
        };
        Block::new(&self.data[blockpos as usize..(blockpos as usize + blocklen as usize)])
    }

    pub fn trailer(&self) -> Trailer<'_> {
        let end = self.data.len();
        let root_pos = u64::from_be_bytes(self.data[end - 8..end].try_into().unwrap());
        let bloom_len = u32::from_be_bytes(self.data[end - 12..end - 8].try_into().unwrap());
        let bloom_start = end - 12 - bloom_len as usize;
        let bloom = &self.data[bloom_start..end - 12];
        if self.data[bloom_start - 4..bloom_start] != [0, 0, 0, 0] {
            panic!("missing trailer padding");
        }
        if root_pos as usize >= end {
            panic!("root_pos is outside bounds of file");
        }
        Trailer {
            bloom,
            bloom_len,
            root_pos,
        }
    }
}

#[derive(Debug)]
pub struct Trailer<'a> {
    bloom: &'a [u8],
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
    type Error = &'static str;

    fn try_from(value: u8) -> Result<Compression, Self::Error> {
        match value {
            0 => Ok(Compression::None),
            1 => Ok(Compression::Snappy),
            2 => Ok(Compression::Gzip),
            3 => Ok(Compression::Lz4),
            _ => Err("invalid compression value"),
        }
    }
}

#[derive(Debug)]
pub struct Block<'a> {
    pub blocklen: u32,
    pub level: u16,
    pub compression: Compression,
    data: &'a [u8],
}

impl<'a> Block<'a> {
    fn new(buffer: &'a [u8]) -> Option<Self> {
        let blocklen = u32::from_be_bytes(buffer[0..4].try_into().ok()?);
        let level = u16::from_be_bytes(buffer[4..6].try_into().ok()?);
        let block_upper_bound = (4 + blocklen) as usize;
        let data = if blocklen > 2 {
            &buffer[7..block_upper_bound]
        } else {
            &buffer[7..7]
        };
        let compression: Compression = buffer[6].try_into().ok()?;
        if compression != Compression::None {
            unimplemented!("Cannot handle compression type {compression:?}");
        }
        if data[0] == TAG_END {
            Some(Self {
                blocklen,
                level,
                compression,
                data,
            })
        } else {
            None
        }
    }

    pub fn entries(&self) -> EntryIterator<'a> {
        EntryIterator { buffer: self.data }
    }
}

#[derive(Debug, Clone, Copy)]
#[allow(dead_code)]
pub enum Entry<'a> {
    KeyVal {
        key: &'a [u8],
        value: &'a [u8],
        timestamp: Option<u32>,
    },
    Deleted {
        key: &'a [u8],
        timestamp: Option<u32>,
    },
    PosLen {
        blockpos: u64,
        blocklen: u32,
        key: &'a [u8],
    },
}

impl<'a> Entry<'a> {
    // TODO: consider using Result with a meaningful error
    fn read(buffer: &'a [u8]) -> Option<(Self, usize)> {
        if buffer[0] != TAG_END {
            println!("First byte wasn't TAG_END");
            return None;
        }
        let length = u32::from_be_bytes(buffer[1..5].try_into().ok()?);
        // println!("Entry length {length}");
        // 5 = crc (4 bytes) + tag_end
        if buffer.len() < (length + 5) as usize {
            println!("Entry was shorter than length! {buffer:?}");
            return None;
        }
        let upper_bound = 9 + length as usize;
        // println!("Entry data: {:?}", &buffer[0..upper_bound]);
        let orig_crc = u32::from_be_bytes(buffer[5..9].try_into().ok()?);
        let crc = const_crc32::crc32(&buffer[9..upper_bound]);
        if crc != orig_crc {
            println!("CRC32 didn't match, computed {crc}, original {orig_crc}");
            return None;
        }
        let entry = match buffer[9] {
            TAG_KV_DATA => {
                let keylen = u32::from_be_bytes(buffer[10..14].try_into().ok()?);
                let key_upper_bound = 14 + keylen as usize;
                let key = &buffer[14..key_upper_bound];
                let value = &buffer[key_upper_bound..upper_bound];
                Self::KeyVal {
                    key,
                    value,
                    timestamp: None,
                }
            }
            TAG_KV_DATA2 => {
                let timestamp = u32::from_be_bytes(buffer[10..14].try_into().ok()?);
                let keylen = u32::from_be_bytes(buffer[14..18].try_into().ok()?);
                let key_upper_bound = 18 + keylen as usize;
                let key = &buffer[18..key_upper_bound];
                let value = &buffer[key_upper_bound..upper_bound];
                Self::KeyVal {
                    key,
                    value,
                    timestamp: Some(timestamp),
                }
            }
            TAG_DELETED => {
                let keylen = u32::from_be_bytes(buffer[10..14].try_into().ok()?);
                let key_upper_bound = 14 + keylen as usize;
                let key = &buffer[14..key_upper_bound];
                Self::Deleted {
                    key,
                    timestamp: None,
                }
            }
            TAG_DELETED2 => {
                let timestamp = u32::from_be_bytes(buffer[10..14].try_into().ok()?);
                let keylen = u32::from_be_bytes(buffer[14..18].try_into().ok()?);
                let key_upper_bound = 18 + keylen as usize;
                let key = &buffer[18..key_upper_bound];
                Self::Deleted {
                    key,
                    timestamp: Some(timestamp),
                }
            }
            TAG_POSLEN32 => {
                let blockpos = u64::from_be_bytes(buffer[10..18].try_into().ok()?);
                let blocklen = u32::from_be_bytes(buffer[18..22].try_into().ok()?);
                let key = &buffer[22..(9 + length as usize)];
                Self::PosLen {
                    blockpos,
                    blocklen,
                    key,
                }
            }
            tag => {
                println!("Unrecognized entry tag {tag}");
                return None;
            }
        };
        Some((entry, upper_bound))
    }
}

pub struct EntryIterator<'a> {
    buffer: &'a [u8],
}

impl<'a> Iterator for EntryIterator<'a> {
    type Item = Entry<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.buffer.len() < 2 {
            return None;
        }
        let (entry, new_offset) = Entry::read(self.buffer)?;
        self.buffer = &self.buffer[new_offset..];
        Some(entry)
    }
}
