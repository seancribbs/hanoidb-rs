use crate::error::*;

use crate::{TAG_DELETED, TAG_DELETED2, TAG_END, TAG_KV_DATA, TAG_KV_DATA2, TAG_POSLEN32};

use std::io::{ErrorKind, Read};

#[derive(Debug, Clone, PartialEq, Eq)]
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
    ///Returns `true` if this value is of type `
    ///KeyVal
    ///`. Returns `false` otherwise
    #[inline]
    #[must_use]
    pub const fn is_key_val(&self) -> bool {
        matches!(self, Entry::KeyVal { .. })
    }
    ///Returns `true` if this value is of type `
    ///Deleted
    ///`. Returns `false` otherwise
    #[inline]
    #[must_use]
    pub const fn is_deleted(&self) -> bool {
        matches!(self, Entry::Deleted { .. })
    }
    ///Returns `true` if this value is of type `
    ///PosLen
    ///`. Returns `false` otherwise
    #[inline]
    #[must_use]
    pub const fn is_pos_len(&self) -> bool {
        matches!(self, Entry::PosLen { .. })
    }

    pub fn key(&self) -> &[u8] {
        match self {
            Entry::KeyVal { key, .. } | Entry::Deleted { key, .. } | Entry::PosLen { key, .. } => {
                key.as_slice()
            }
        }
    }

    pub fn read(file: &mut impl Read) -> Result<Self> {
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
                let key = entry_data.split_off(1);
                Self::Deleted {
                    key,
                    timestamp: None,
                }
            }
            TAG_DELETED2 => {
                let timestamp = u32::from_be_bytes(entry_data[1..5].try_into()?);
                let key = entry_data.split_off(5);
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
        Ok(entry)
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

    pub fn encoded_size(&self) -> usize {
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
