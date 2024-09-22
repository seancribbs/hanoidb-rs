use crate::error::*;
use std::fs::File;
use std::io::{ErrorKind, Read, Seek, SeekFrom};
use std::path::Path;

const TAG_KV_DATA: u8 = 0x80;
const TAG_DELETED: u8 = 0x81;
const TAG_POSLEN32: u8 = 0x82;
#[allow(dead_code)]
const TAG_TRANSACT: u8 = 0x83;
const TAG_KV_DATA2: u8 = 0x84;
const TAG_DELETED2: u8 = 0x85;
pub const TAG_END: u8 = 0xFF;

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

    pub fn try_clone(&self) -> Result<Self> {
        let file = self.file.try_clone()?;
        let len = file.metadata()?.len();
        Ok(Self { file, len })
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

    pub fn entries(&self) -> Result<TreeEntryIterator<'_>> {
        TreeEntryIterator::new(self)
    }

    pub fn entries_owned(&self) -> Result<OwnedTreeEntryIterator> {
        OwnedTreeEntryIterator::new(self.try_clone()?)
    }

    pub fn get(&self, key: &[u8]) -> Option<Vec<u8>> {
        // TODO: read the bloom filter and bail early if
        // it's not in the file
        let mut block = self.root_block().ok()?;
        loop {
            // level > 0 -> inner block
            // level == 0 -> leaf block
            if block.level > 0 {
                let entry = block
                    .entries()
                    .take_while(|e| {
                        !matches!(e, Entry::PosLen {
                            key: first_key,
                            ..
                        } if key < first_key)
                    })
                    .last();
                if let Some(inner_entry) = entry {
                    // Go to the next lower level in the tree
                    block = self.block_from_poslen_entry(&inner_entry).ok()?;
                } else {
                    return None;
                }
            } else {
                return block.entries().find_map(|entry| match entry {
                    Entry::KeyVal {
                        key: found_key,
                        value,
                        ..
                    } if key == found_key => Some(value),
                    _ => None,
                });
            }
        }
    }
}

pub struct OwnedTreeEntryIterator {
    tree: Tree,
    levels: Vec<OwnedEntryIterator>,
}

impl OwnedTreeEntryIterator {
    fn new(tree: Tree) -> Result<Self> {
        let root_iter = tree.root_block()?.entries_owned()?;
        Ok(Self {
            tree,
            levels: vec![root_iter],
        })
    }
}

impl Iterator for OwnedTreeEntryIterator {
    type Item = Entry;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            let level = self.levels.last_mut()?;
            match level.next() {
                Some(entry @ Entry::PosLen { .. }) => {
                    let block_iterator = self
                        .tree
                        .block_from_poslen_entry(&entry)
                        .ok()?
                        .entries_owned()
                        .ok()?;
                    self.levels.push(block_iterator);
                    continue;
                }
                entry @ Some(_) => {
                    return entry;
                }
                None => {
                    // pop this iterator off
                    let _ = self.levels.pop();
                    continue;
                }
            }
        }
    }
}

pub struct TreeEntryIterator<'a> {
    tree: &'a Tree,
    levels: Vec<EntryIterator<'a>>,
}

impl<'a> TreeEntryIterator<'a> {
    fn new(tree: &'a Tree) -> Result<Self> {
        Ok(Self {
            tree,
            levels: vec![tree.root_block()?.entries()],
        })
    }
}

impl<'a> Iterator for TreeEntryIterator<'a> {
    type Item = Entry;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            let level = self.levels.last_mut()?;
            match level.next() {
                Some(entry @ Entry::PosLen { .. }) => {
                    let block_iterator = self.tree.block_from_poslen_entry(&entry).ok()?.entries();
                    self.levels.push(block_iterator);
                    continue;
                }
                entry @ Some(_) => {
                    return entry;
                }
                None => {
                    // pop this iterator off
                    let _ = self.levels.pop();
                    continue;
                }
            }
        }
    }
}

#[derive(Debug)]
pub struct Trailer {
    bloom: Vec<u8>,
    bloom_len: u32,
    root_pos: u64,
}

impl Trailer {
    pub fn new(bloom: Vec<u8>, root_pos: u64) -> Result<Self> {
        let bloom_len: u32 = bloom
            .len()
            .try_into()
            .map_err(|_| Error::BloomFilterTooLarge)?;
        Ok(Self {
            bloom,
            bloom_len,
            root_pos,
        })
    }

    pub fn encode(&self) -> Vec<u8> {
        let mut buffer = Vec::with_capacity(self.bloom.len() + 12);
        buffer.extend([0, 0, 0, 0]);
        buffer.extend(&self.bloom);
        buffer.extend(self.bloom_len.to_be_bytes());
        buffer.extend(self.root_pos.to_be_bytes());
        buffer
    }
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
        let mut header = vec![0; 8];
        file.read_exact(&mut header)?;
        let blocklen = u32::from_be_bytes(header[0..4].try_into()?);
        let level = u16::from_be_bytes(header[4..6].try_into()?);
        let compression: Compression = header[6].try_into()?;
        if header[7] == TAG_END {
            Ok(Self {
                start,
                blocklen,
                level,
                compression,
                file,
            })
        } else {
            Err(Error::CorruptedFile(
                "block entries did not start with TAG_END",
            ))
        }
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
            start: self.start + 8, // Skip the header part of the block (8 bytes)
            end: self.start + (self.blocklen as u64),
        }
    }

    pub fn entries_owned(&self) -> Result<OwnedEntryIterator> {
        let file = self.file.try_clone()?;
        Ok(OwnedEntryIterator {
            file,
            start: self.start + 8,
            end: self.start + (self.blocklen as u64),
        })
    }
}

#[derive(Debug, Clone, derive_more::IsVariant)]
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
    pub(crate) fn key(&self) -> &[u8] {
        match self {
            Entry::KeyVal { key, .. } | Entry::Deleted { key, .. } | Entry::PosLen { key, .. } => {
                key.as_slice()
            }
        }
    }

    pub(crate) fn read(mut file: &File) -> Result<(Self, u64)> {
        let mut header = vec![0; 8];
        file.read_exact(&mut header).map_err(|err| {
            if err.kind() == ErrorKind::UnexpectedEof {
                Error::EndOfFile
            } else {
                err.into()
            }
        })?;
        let length = u32::from_be_bytes(header[0..4].try_into()?);
        let orig_crc = u32::from_be_bytes(header[4..8].try_into()?);
        let mut entry_data = vec![0; length as usize];
        file.read_exact(&mut entry_data).map_err(|err| {
            if err.kind() == ErrorKind::UnexpectedEof {
                Error::IncompleteEntry(err)
            } else {
                err.into()
            }
        })?;
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
        let mut tag = vec![0; 1];
        file.read_exact(&mut tag)?;
        if tag[0] != TAG_END {
            return Err(Error::CorruptedFile("Last byte of entry wasn't TAG_END"));
        }
        Ok((entry, 9 + length as u64))
    }

    pub fn encode(&self) -> Vec<u8> {
        let total_size = self.encoded_size();
        let mut entry = Vec::with_capacity(total_size);
        entry.extend(((total_size - 9) as u32).to_be_bytes());
        entry.extend([0, 0, 0, 0]); // spot for CRC32
        match self {
            Entry::KeyVal {
                key,
                value,
                timestamp,
            } => {
                if let Some(ts) = timestamp {
                    entry.push(TAG_KV_DATA2);
                    entry.extend(ts.to_be_bytes());
                } else {
                    entry.push(TAG_KV_DATA);
                }
                // UNSAFE: usize could exceed u32
                let key_size = (key.len() as u32).to_be_bytes();
                entry.extend(key_size);
                entry.extend(key);
                entry.extend(value);
            }
            Entry::Deleted { key, timestamp } => {
                if let Some(ts) = timestamp {
                    entry.push(TAG_DELETED2);
                    entry.extend(ts.to_be_bytes());
                } else {
                    entry.push(TAG_DELETED);
                }
                entry.extend(key);
            }
            Entry::PosLen {
                blockpos,
                blocklen,
                key,
            } => {
                entry.push(TAG_POSLEN32);
                entry.extend(blockpos.to_be_bytes());
                entry.extend(blocklen.to_be_bytes());
                entry.extend(key);
            }
        }
        let crc = crc32fast::hash(&entry[8..(total_size - 1)]).to_be_bytes();
        entry[4..8].copy_from_slice(&crc);
        entry.push(TAG_END);
        entry
    }

    pub(crate) fn encoded_size(&self) -> usize {
        // entry len + crc32 + trailing TAG_END
        9 + match self {
            Entry::KeyVal {
                key,
                value,
                timestamp,
            } => {
                // Tag + optional timestamp u32 + key len + key + value
                1 + timestamp.as_ref().map(|_| 4).unwrap_or_default() + 4 + key.len() + value.len()
            }
            Entry::Deleted { key, timestamp } => {
                // Tag + optional timestamp u32 + key
                1 + timestamp.as_ref().map(|_| 4).unwrap_or_default() + key.len()
            }
            Entry::PosLen { key, .. } => {
                // Tag + blockpos + blocklen + key
                1 + 8 + 4 + key.len()
            }
        }
    }
}

pub struct OwnedEntryIterator {
    file: File,
    start: u64,
    end: u64,
}

impl Iterator for OwnedEntryIterator {
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

        match Entry::read(&self.file) {
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
