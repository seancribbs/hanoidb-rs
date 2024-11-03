use std::fs::{File, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};

use fastbloom::BloomFilter;

use crate::error::*;
use crate::format::{Compression, Entry, Trailer, MAGIC, TAG_END};

const BLOCK_SIZE: usize = 8 * 1024;
const FIRST_BLOCK_POS: u64 = 4;

#[derive(Default, Clone)]
struct Block {
    level: u16,
    size: usize,
    members: Vec<Entry>,
}

impl Block {
    fn is_solo_inner_block(&self) -> bool {
        self.level > 0 && self.members.len() == 1
    }
}

pub struct Writer {
    name: PathBuf,
    index_file: File,
    index_file_pos: u64,
    last_node_pos: Option<u64>,
    last_node_size: Option<u32>,
    blocks: Vec<Block>,
    bloom: BloomFilter,
    compress: Compression,
    value_count: usize,
    tombstone_count: usize,
}

impl std::fmt::Debug for Writer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Writer")
            .field("file", &self.name)
            .field("count", &self.count())
            .finish()
    }
}

impl Writer {
    pub fn with_expected_num_items(
        name: impl AsRef<Path>,
        expected_num_items: usize,
    ) -> Result<Self> {
        let bloom = BloomFilter::with_false_pos(0.01).expected_items(expected_num_items);

        let mut index_file = OpenOptions::new()
            .append(true)
            .create_new(true)
            .open(name.as_ref())?;
        index_file.write_all(MAGIC.as_bytes())?;
        Ok(Self {
            name: name.as_ref().to_path_buf(),
            index_file,
            index_file_pos: FIRST_BLOCK_POS,
            last_node_pos: None,
            last_node_size: None,
            blocks: Default::default(),
            bloom,
            compress: Compression::None,
            value_count: 0,
            tombstone_count: 0,
        })
    }

    pub fn count(&self) -> usize {
        self.value_count + self.tombstone_count
    }

    pub fn add(&mut self, entry: Entry) -> Result<()> {
        if !entry.is_pos_len() {
            self.bloom.insert(entry.key());
        }
        self.append_to_block(0, entry)?;
        Ok(())
    }

    pub fn close(mut self) -> Result<()> {
        // Unwritten blocks: call flush_block_buffer to write them
        while let Some(block) = self.blocks.last() {
            // 1 block with 1 entry in it where level is not 0, discard that block
            if block.is_solo_inner_block() {
                break;
            }
            self.flush_block_buffer()?;
        }
        // No blocks: write trailer and close file
        let root_pos = match self.last_node_pos {
            Some(pos) => pos,
            None => {
                // No blocks have been written to the file
                self.index_file.write_all(&[0, 0, 0, 0, 0, 0])?; // header of an empty block: <<0:32/unsigned, 0:16/unsigned>>
                FIRST_BLOCK_POS
            }
        };
        let trailer = Trailer::with_bloom_filter(self.bloom, root_pos);
        self.index_file.write_all(&trailer.encode())?;
        self.index_file.sync_data()?;
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
        self.index_file.write_all(&buffer)?;

        let blockpos = self.index_file_pos;
        let blocklen: u32 = buffer.len().try_into().unwrap();
        self.last_node_pos = Some(blockpos);
        self.last_node_size = Some(blocklen);
        self.index_file_pos += blocklen as u64;
        self.append_to_block(
            block.level + 1,
            Entry::PosLen {
                blockpos,
                blocklen,
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

#[cfg(test)]
pub mod tests {
    use super::*;
    use crate::format::Tree;
    use tempfile::tempdir;

    impl Writer {
        pub fn new(name: impl AsRef<Path>) -> Result<Self> {
            Self::with_expected_num_items(name, 1024)
        }
    }

    // Roundtrip - write some values to a file and read the file back
    #[test]
    fn roundtrip() {
        let dir = tempdir().unwrap();
        let data = dir.as_ref().join("test.data");
        let deleted_key = "deleted".to_owned().into_bytes();
        let key = "key".to_owned().into_bytes();
        let value = "value".to_owned().into_bytes();

        let deleted = Entry::Deleted {
            key: deleted_key.clone(),
            timestamp: None,
        };
        let kv = Entry::KeyVal {
            key: key.clone(),
            value: value.clone(),
            timestamp: None,
        };
        let mut writer = Writer::new(&data).unwrap();

        writer.add(deleted.clone()).unwrap();
        writer.add(kv.clone()).unwrap();
        writer.close().unwrap();

        let tree = Tree::from_file(&data).unwrap();
        assert_eq!(tree.get_entry(&key).unwrap(), Some(kv));
        assert_eq!(tree.get_entry(&deleted_key).unwrap(), Some(deleted));
    }

    // Writes must be in byte-lexical order
    #[test]
    fn writes_are_in_byte_order() {
        let dir = tempdir().unwrap();
        let data = dir.as_ref().join("test.data");
        let deleted_key = "deleted".to_owned().into_bytes();
        let key = "key".to_owned().into_bytes();
        let value = "value".to_owned().into_bytes();

        let deleted = Entry::Deleted {
            key: deleted_key.clone(),
            timestamp: None,
        };
        let kv = Entry::KeyVal {
            key: key.clone(),
            value: value.clone(),
            timestamp: None,
        };
        let mut writer = Writer::new(&data).unwrap();

        // These writes are out of order
        writer.add(kv.clone()).unwrap();
        let error = writer.add(deleted.clone()).unwrap_err();
        assert_eq!(error.to_string(), "out-of-order write");
    }

    // Values and tombstone counts are tracked correctly
    #[test]
    fn key_counts() {
        let dir = tempdir().unwrap();
        let data = dir.as_ref().join("test.data");
        let deleted_key = "deleted".to_owned().into_bytes();
        let key = "key".to_owned().into_bytes();
        let value = "value".to_owned().into_bytes();

        let deleted = Entry::Deleted {
            key: deleted_key.clone(),
            timestamp: None,
        };
        let kv = Entry::KeyVal {
            key: key.clone(),
            value: value.clone(),
            timestamp: None,
        };
        let mut writer = Writer::new(&data).unwrap();

        writer.add(deleted.clone()).unwrap();
        assert_eq!(writer.count(), 1);
        assert_eq!(writer.tombstone_count, 1);
        assert_eq!(writer.value_count, 0);
        writer.add(kv.clone()).unwrap();
        assert_eq!(writer.count(), 2);
        assert_eq!(writer.tombstone_count, 1);
        assert_eq!(writer.value_count, 1);
        writer.close().unwrap();
    }

    // Blocks are closed when they reach 8KB
    #[test]
    fn max_block_size_is_8kb() {
        // write 8kb of data, and then check that an inner
        // node of the btree was created
        let dir = tempdir().unwrap();
        let data = dir.as_ref().join("test.data");
        let mut writer = Writer::new(&data).unwrap();
        let key = write_8kb(&mut writer, 0).unwrap();
        assert_eq!(writer.blocks.len(), 1);
        assert!(writer.blocks[0].is_solo_inner_block());
        let _ = write_8kb(&mut writer, key);
        assert_eq!(writer.blocks.len(), 1);
        assert_eq!(writer.blocks[0].level, 1);
        assert_eq!(writer.blocks[0].members.len(), 2);
    }

    pub fn write_8kb(writer: &mut Writer, mut key: u64) -> Result<u64> {
        let mut written: usize = 0;
        while written < 8192 {
            let entry = Entry::KeyVal {
                key: key.to_be_bytes().to_vec(),
                value: key.to_be_bytes().to_vec(),
                timestamp: None,
            };
            let entry_size = entry.encoded_size();
            writer.add(entry)?;
            written += entry_size;
            key += 1;
        }
        Ok(key)
    }

    // Solo inner nodes are pruned on close
    #[test]
    fn inner_nodes_have_fanout_gt_1() {
        // write 8kb of data, then close.
        // write 8kb of data, and then check that an inner
        // node of the btree was created
        let dir = tempdir().unwrap();
        let data = dir.as_ref().join("test.data");
        let mut writer = Writer::new(&data).unwrap();
        let _key = write_8kb(&mut writer, 0).unwrap();
        assert_eq!(writer.blocks.len(), 1);
        assert!(writer.blocks[0].is_solo_inner_block());
        writer.close().unwrap();
        // check the contents of the written file to make sure there isn't an
        // inner node with a single entry
        let tree = Tree::from_file(&data).unwrap();
        let root = tree.root_block().unwrap();
        assert_eq!(root.level, 0);
        assert_eq!(root.start, 4);
        assert!(root.blocklen >= 8192);
    }

    // Write an empty file
    #[test]
    fn empty_file() {
        let dir = tempdir().unwrap();
        let data = dir.as_ref().join("test.data");
        let writer = Writer::new(&data).unwrap();
        writer.close().unwrap();
        let contents = std::fs::read(&data).unwrap();
        let bloom = BloomFilter::with_false_pos(0.01).expected_items(1024);
        let bloom_len = bloom.as_slice().len() * 8;
        // magic - 4
        // blocklen - 4
        // level - 2
        // pad - 4
        // bloom - X
        // bloom_len - 4
        // root_pos - 8
        assert_eq!(contents.len(), 26 + bloom_len);
        assert_eq!(&contents[0..4], "HAN3".as_bytes()); // magic
        assert_eq!(&contents[4..8], &[0, 0, 0, 0]); // blocklen = 0
        assert_eq!(&contents[8..10], &[0, 0]); // level = 0
        assert_eq!(&contents[10..14], &[0, 0, 0, 0]); // pad
                                                      // skip bloom filter
        assert_eq!(
            &contents[14 + bloom_len..18 + bloom_len],
            (bloom_len as u32).to_be_bytes()
        ); // bloom_len
        assert_eq!(
            &contents[18 + bloom_len..26 + bloom_len],
            &[0, 0, 0, 0, 0, 0, 0, 4]
        ); // root_pos

        let _tree = Tree::from_file(&data).unwrap();
        // TODO: Fix Block::from_start to accept empty blocks
    }
}
