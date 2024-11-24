use crate::block::{Block, EntryIterator};
use crate::entry::Entry;
use crate::error::*;
use crate::trailer::Trailer;
use crate::MAGIC;
use std::fs::File;
use std::io::{Read, Seek, SeekFrom};
use std::path::Path;

pub struct Tree {
    file: File,
    trailer: Trailer,
}

impl Tree {
    pub fn from_file(path: impl AsRef<Path>) -> Result<Self> {
        let mut file = File::open(path)?;
        let len = file.metadata()?.len();
        let mut magic: Vec<u8> = vec![0; 4];
        file.read_exact(&mut magic)?;
        if magic == MAGIC.as_bytes() {
            let trailer = Self::read_trailer(&file, len)?;
            Ok(Self { file, trailer })
        } else {
            Err(Error::InvalidTreeFormat(magic))
        }
    }

    pub fn try_clone(&self) -> Result<Self> {
        let file = self.file.try_clone()?;
        let trailer = self.trailer.clone();
        Ok(Self { file, trailer })
    }

    pub fn root_block(&self) -> Result<Block<'_>> {
        Block::from_start(&self.file, self.trailer.root_pos)
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

    fn read_trailer(mut file: &File, len: u64) -> Result<Trailer> {
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
        if root_pos >= len {
            return Err(Error::CorruptedFile(
                "root block position outside bounds of file",
            ));
        }
        Trailer::new(bloom, root_pos)
    }

    pub fn entries(&self) -> Result<TreeEntryIterator> {
        TreeEntryIterator::new(self.try_clone()?)
    }

    pub fn get_entry(&self, key: &[u8]) -> Result<Option<Entry>> {
        if !self.trailer.bloom.contains(key) {
            return Ok(None);
        }

        let mut block = self.root_block()?;
        loop {
            // level > 0 -> inner block
            // level == 0 -> leaf block
            if block.level > 0 {
                let entry = block
                    .entries()?
                    .take_while(|e| {
                        !matches!(e, Entry::PosLen {
                            key: first_key,
                            ..
                        } if key < first_key)
                    })
                    .last();
                if let Some(inner_entry) = entry {
                    // Go to the next lower level in the tree
                    block = self.block_from_poslen_entry(&inner_entry)?;
                } else {
                    return Ok(None);
                }
            } else {
                return Ok(block.entries()?.find(|entry| entry.key() == key));
            }
        }
    }
}

pub struct TreeEntryIterator {
    tree: Tree,
    levels: Vec<EntryIterator>,
}

impl TreeEntryIterator {
    fn new(tree: Tree) -> Result<Self> {
        let root_iter = tree.root_block()?.entries()?;
        Ok(Self {
            tree,
            levels: vec![root_iter],
        })
    }
}

impl Iterator for TreeEntryIterator {
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
                        .entries()
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
