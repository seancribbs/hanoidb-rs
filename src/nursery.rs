use std::collections::BTreeMap;
use std::fs::{File, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};

use crate::error::*;
use crate::format::Entry;

enum Value {
    Plain(Vec<u8>),
    // Timestampped(Vec<u8>, time value?)
    Deleted,
}

pub struct Nursery {
    log: File,
    directory: PathBuf,
    min_level: u32,
    max_level: u32,
    data: BTreeMap<Vec<u8>, Value>,
    total_size: usize,
    step: usize,
    merge_done: usize,
}

impl Nursery {
    pub fn new(directory: impl AsRef<Path>, min_level: u32, max_level: u32) -> Result<Self> {
        let directory = directory.as_ref().to_path_buf();
        let file = directory.join("nursery.log");
        // TODO: deal with recovery
        let log = OpenOptions::new()
            .create_new(true)
            .append(true)
            .open(file)?;
        Ok(Self {
            log,
            directory,
            min_level,
            max_level,
            data: Default::default(),
            total_size: 0,
            step: 0,
            merge_done: 0,
        })
    }

    pub fn add(&mut self, key: Vec<u8>, value: Vec<u8>) -> Result<()> {
        let bin_entry = Entry::KeyVal {
            key: key.clone(),
            value: value.clone(),
            timestamp: None,
        }
        .encode();
        self.data.insert(key, Value::Plain(value));
        self.log.write(&bin_entry);
        self.log.sync_data()?;
        self.total_size += bin_entry.len();
        // TODO: invoke incremental merge
        self.step += 1;
        Ok(())
    }
}
