use crate::error::*;

use fastbloom::BloomFilter;

#[derive(Debug, PartialEq, Eq, Clone)]
pub struct Trailer {
    pub bloom: BloomFilter,
    pub root_pos: u64,
}

impl Trailer {
    pub fn with_bloom_filter(bloom: BloomFilter, root_pos: u64) -> Self {
        Self { bloom, root_pos }
    }

    pub fn new(raw_bloom: Vec<u8>, root_pos: u64) -> Result<Self> {
        // Bloom filter is too big for our file format
        if raw_bloom.len() > u32::MAX as usize {
            return Err(Error::BloomFilterTooLarge);
        }

        // The file is empty, so the bloom filter was written as 0-length
        if raw_bloom.is_empty() {
            return Ok(Self {
                bloom: BloomFilter::with_false_pos(0.01).expected_items(1024),
                root_pos,
            });
        }

        let bloom: BloomFilter = postcard::from_bytes(&raw_bloom)?;

        Ok(Self::with_bloom_filter(bloom, root_pos))
    }

    pub fn encode(&self) -> Result<Vec<u8>> {
        let raw_bloom: Vec<u8> = postcard::to_stdvec(&self.bloom)?;

        let mut buffer = Vec::with_capacity(raw_bloom.len() + 12);
        buffer.extend([0, 0, 0, 0]);
        buffer.extend(&raw_bloom);
        buffer.extend((raw_bloom.len() as u32).to_be_bytes());
        buffer.extend(self.root_pos.to_be_bytes());
        Ok(buffer)
    }
}
