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

    pub fn from_start_length(file: &'a File, start: u64, length: u32) -> Result<Self> {
        let block = Self::from_start(file, start)?;
        if block.blocklen == length {
            Ok(block)
        } else {
            Err(Error::IncorrectBlockLength(length, block.blocklen))
        }
    }

    pub fn entries(&self) -> Result<EntryIterator> {
        let mut decompressor = self.compression.reader(BlockContentsReader::new(self)?);

        // SAFETY: If the blocklen is 0, then reading from the block will never fill
        // a buffer because start > end. Therefore we don't need to check for the tag
        // byte or advance the reader at all.
        if self.blocklen == 0 {
            return Ok(EntryIterator(decompressor));
        }

        // Each block that has entries contains a TAG_END byte at the beginning.
        // If that is missing, then we can't read entries. If it is present, we
        // need to skip it.
        let mut tag = vec![0u8; 1];
        decompressor.read_exact(&mut tag)?;

        if tag[0] == TAG_END {
            Ok(EntryIterator(decompressor))
        } else {
            Err(Error::CorruptedFile(
                "block entries did not start with TAG_END",
            ))
        }
    }
}

struct BlockContentsReader {
    file: File,
    start: u64,
    end: u64,
}

impl BlockContentsReader {
    fn new(block: &Block) -> Result<Self> {
        let file = block.file.try_clone()?;
        // The 4-byte blocklen field at the head of the block is not included in the length
        let start_after_blocklen = block.start + 4;
        Ok(BlockContentsReader {
            file,
            // Skip the header fields of the block (2 byte level + 1 byte compression)
            start: start_after_blocklen + 2 + 1,
            // End is start + 4 bytes blocklen + [blocklen]
            end: start_after_blocklen + (block.blocklen as u64),
        })
    }
}

impl Read for BlockContentsReader {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        if self.start > self.end {
            return Ok(0);
        }

        self.file.seek(SeekFrom::Start(self.start))?;

        let result = self.file.read(buf);
        if let Ok(len) = result {
            self.start += len as u64;
        }
        result
    }
}

pub struct EntryIterator(Box<dyn Read>);

impl Iterator for EntryIterator {
    type Item = Entry;

    fn next(&mut self) -> Option<Self::Item> {
        Entry::read(&mut self.0).ok()
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
        assert_eq!(root_block.entries().unwrap().count(), 0)
    }
}
