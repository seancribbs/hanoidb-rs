use std::fs::{File, OpenOptions};
use std::io::Write;

use crate::error::*;
use crate::format::{Compression, Entry, TAG_END};

const BLOCK_SIZE: usize = 8 * 1024;

#[derive(Default, Clone)]
struct Block {
    level: u16,
    size: usize,
    members: Vec<Entry>,
}

pub struct Writer {
    index_file: File,
    index_file_pos: u64,
    last_node_pos: Option<u64>,
    last_node_size: Option<usize>,
    blocks: Vec<Block>,
    name: String,
    // bloom: BloomFilter,
    compress: Compression,
    value_count: usize,
    tombstone_count: usize,
}

impl Writer {
    pub(crate) fn new(name: String) -> Result<Self> {
        let mut index_file = OpenOptions::new()
            .append(true)
            .create_new(true)
            .open(&name)?;
        index_file.write_all("HAN2".as_bytes())?;
        Ok(Self {
            index_file,
            index_file_pos: 4,
            last_node_pos: None,
            last_node_size: None,
            blocks: Default::default(),
            name,
            compress: Compression::None,
            value_count: 0,
            tombstone_count: 0,
        })
    }

    pub(crate) fn count(&self) -> usize {
        self.value_count + self.tombstone_count
    }

    pub(crate) fn add(&mut self, entry: Entry) -> Result<()> {
        self.append_to_block(0, entry)?;
        Ok(())
    }

    fn append_to_block(&mut self, level: u16, entry: Entry) -> Result<()> {
        let mut tombstone_count = 0;
        let mut value_count = 0;
        if entry.is_deleted() {
            tombstone_count += 1;
        } else if entry.is_key_val() {
            value_count += 1;
        }

        let block: &mut Block = self.get_block_at_level(level);
        let new_size = block.size + entry.encoded_size();
        if let Some(last_entry) = block.members.last() {
            if last_entry.key() > entry.key() {
                return Err(Error::OutOfOrderWrite);
            }
        }
        block.size = new_size;
        block.members.push(entry);

        // TODO: Update bloom filter
        self.tombstone_count += tombstone_count;
        self.value_count += value_count;

        if new_size >= BLOCK_SIZE {
            self.flush_block_buffer()?;
        }
        Ok(())
    }

    fn flush_block_buffer(&mut self) -> Result<()> {
        let block = self
            .blocks
            .pop()
            .expect("cannot flush block that doesn't exist");
        let first_key = block.members.first().unwrap().key().to_owned();
        // block size + level + compression + TAG_END
        let mut buffer = Vec::with_capacity(block.size + 8);
        // TODO: We don't actually know until we've compressed the entries what the
        // total size is going to be.
        buffer.extend(((block.size + 2) as u32).to_be_bytes());
        buffer.extend(block.level.to_be_bytes());
        buffer.push(self.compress as u8);
        buffer.push(TAG_END);
        for entry in block.members {
            buffer.extend(entry.encode());
        }
        self.file.write_all(buffer)?;

        let blockpos = self.index_file_pos;
        let blocklen = buffer.len();
        self.last_node_pos = Some(blockpos);
        self.last_node_size = Some(blocklen);
        self.index_file_pos += blocklen.try_into().unwrap();
        self.append_to_block(
            block.level + 1,
            Entry::PosLen {
                blockpos,
                blocklen: blocklen.try_into().unwrap(),
                key: first_key,
            },
        )?;
        Ok(())
    }

    fn get_block_at_level(&mut self, level: u16) -> &mut Block {
        if self.blocks.is_empty() {
            self.blocks.push(Block {
                level,
                ..Default::default()
            });
        } else {
            // TODO: Can we make this clearer?
            let mut last_level = self.blocks.last().unwrap().level;
            while last_level > level {
                self.blocks.push(Block {
                    level: last_level - 1,
                    ..Default::default()
                });
                last_level = self.blocks.last().unwrap().level;
            }
        }
        self.blocks.last_mut().unwrap()
    }
}
