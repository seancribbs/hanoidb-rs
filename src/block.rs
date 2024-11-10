use crate::compression::Compression;
use crate::entry::Entry;
use crate::error::*;
use crate::TAG_END;
use std::fs::File;
use std::io::{Read, Seek, SeekFrom};

#[derive(Debug)]
pub struct Block<'a> {
    pub start: u64,
    pub blocklen: u32,
    pub level: u16,
    #[allow(dead_code)]
    pub compression: Compression,
    file: &'a File,
}

impl<'a> Block<'a> {
    pub fn from_start(mut file: &'a File, start: u64) -> Result<Self> {
        file.seek(SeekFrom::Start(start))?;
        let mut header = vec![0; 8];
        file.read_exact(&mut header)?;
        let blocklen = u32::from_be_bytes(header[0..4].try_into()?);
        let level = u16::from_be_bytes(header[4..6].try_into()?);
        let compression: Compression = header[6].try_into()?;

        if blocklen == 0 {
            return Ok(Self {
                start,
                blocklen,
                level,
                compression,
                file,
            });
        }

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

    pub fn from_start_length(file: &'a File, start: u64, length: u32) -> Result<Self> {
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

#[cfg(test)]
mod tests {
    use crate::tree::Tree;
    use crate::writer::Writer;

    use tempfile::tempdir;

    #[test]
    fn block_from_start_accepts_empty_blocks() {
        let dir = tempdir().unwrap();
        let data = dir.as_ref().join("test.data");
        let writer = Writer::new(&data).unwrap();
        writer.close().unwrap();
        let tree = Tree::from_file(&data).unwrap();
        let root_block = tree.root_block().unwrap();
        assert_eq!(root_block.entries().count(), 0)
    }
}
