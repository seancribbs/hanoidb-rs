use crate::block::{Block, EntryIterator};
use crate::entry::Entry;
use crate::error::*;
use crate::range::RangeOverlap;
use crate::scan::ScanRange;
use crate::trailer::Trailer;
use crate::MAGIC;
use std::fs::File;
use std::io::{Read, Seek, SeekFrom};
use std::iter::Peekable;

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

    pub fn entries_in_range<R: ScanRange>(&self, range: R) -> Result<TreeEntryIterator<R>> {
        TreeEntryIterator::with_range(self.try_clone()?, range)
    }

    pub fn entries(&self) -> Result<TreeEntryIterator<std::ops::RangeFull>> {
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

pub struct TreeEntryIterator<R: ScanRange> {
    tree: Tree,
    inner_blocks: Vec<Peekable<EntryIterator<std::ops::RangeFull>>>,
    leaf_block: Option<EntryIterator<R>>,
    range: R,
}

impl TreeEntryIterator<std::ops::RangeFull> {
    fn new(tree: Tree) -> Result<Self> {
        Self::with_range(tree, ..)
    }
}

impl<R: ScanRange> TreeEntryIterator<R> {
    fn with_range(tree: Tree, range: R) -> Result<Self> {
        let mut inner_blocks = vec![];
        let mut leaf_block = None;
        let root = tree.root_block()?;
        if root.is_leaf() {
            leaf_block = Some(root.entries_in_range(range.clone())?);
        } else {
            inner_blocks.push(root.entries()?.peekable());
        }
        Ok(Self {
            tree,
            inner_blocks,
            leaf_block,
            range,
        })
    }
}

// 200..500
// 1 [100, 400, 800] PosLen
// 0 [100-399, 400-500, 800-1100] KeyVal/Deleted
//

impl<R: ScanRange> Iterator for TreeEntryIterator<R> {
    type Item = Entry;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            // Examine the iterator for a leaf block first
            if let Some(leaf) = self.leaf_block.as_mut() {
                match leaf.next() {
                    entry @ Some(_) => return entry,
                    None => {
                        self.leaf_block = None;
                    }
                }
            }
            // We are iterating on an inner block, so try to traverse down the tree
            let level = self.inner_blocks.last_mut()?;
            match level.next() {
                Some(entry) if entry.is_pos_len() => {
                    // Peek at the next entry and determine whether the lower bound of the range crosses it
                    let next_key = level.peek().map(|e| e.key());
                    let should_examine_block = {
                        let range = &self.range;
                        if let Some(end_key) = next_key {
                            // We have an upper bound on the range of this block
                            ((entry.key().to_vec())..(end_key.to_vec())).overlaps(range)
                        } else {
                            // This is the last block in this subtree
                            ((entry.key().to_vec())..).overlaps(range)
                        }
                    };
                    if should_examine_block {
                        let block = self.tree.block_from_poslen_entry(&entry).ok()?;
                        if block.is_leaf() {
                            self.leaf_block = block.entries_in_range(self.range.clone()).ok();
                        } else {
                            self.inner_blocks.push(block.entries().ok()?.peekable());
                        }
                    }
                    continue;
                }
                Some(entry) => {
                    unreachable!("inner block contained leaf entry {entry:?}");
                }
                None => {
                    // pop this iterator off
                    let _ = self.inner_blocks.pop();
                    continue;
                }
            }
        }
    }
}
