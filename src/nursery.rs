use std::collections::BTreeMap;
use std::fs::{remove_file, File, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};

use crate::error::*;
use crate::format::Entry;
use crate::writer::Writer;

#[derive(Debug, Clone)]
enum Value {
    Plain(Vec<u8>),
    // Timestampped(Vec<u8>, time value?)
    Deleted,
}

type NurseryData = BTreeMap<Vec<u8>, Value>;

#[derive(Debug)]
pub struct Nursery {
    log: File,
    directory: PathBuf,
    min_level: u32,
    max_level: u32,
    data: NurseryData,
    total_size: usize,
    step: usize,
    merge_done: usize,
}

impl Nursery {
    pub fn new(directory: impl AsRef<Path>, min_level: u32, max_level: u32) -> Result<Self> {
        let directory = directory.as_ref().to_path_buf();
        let file = directory.join("nursery.log");
        Self::recover(&file)?;
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
        self.write_internal(key, Value::Plain(value), bin_entry)
    }

    pub fn remove(&mut self, key: Vec<u8>) -> Result<()> {
        let bin_entry = Entry::Deleted {
            key: key.clone(),
            timestamp: None,
        }
        .encode();
        self.write_internal(key, Value::Deleted, bin_entry)
    }

    fn write_internal(&mut self, key: Vec<u8>, value: Value, bin_entry: Vec<u8>) -> Result<()> {
        self.data.insert(key, value);
        self.log.write_all(&bin_entry)?;
        self.log.sync_data()?;
        self.total_size += bin_entry.len();
        // TODO: invoke incremental merge
        // Number of merge steps should be key count of min level / 2 (e.g. keys = 1024 -> 512 steps)
        // -define(INC_MERGE_STEP, ?BTREE_SIZE(MinLevel) div 2).
        self.step += 1;
        Ok(())
    }

    fn recover(log_file: impl AsRef<Path>) -> Result<()> {
        if !log_file.as_ref().exists() {
            return Ok(());
        }

        let file = OpenOptions::new().read(true).open(&log_file)?;
        let mut data: BTreeMap<Vec<u8>, Entry> = Default::default();
        loop {
            let entry = match Entry::read(&file) {
                Ok((entry, _)) => entry,
                Err(err) => {
                    if !matches!(err, Error::EndOfFile) {
                        eprintln!("Error reading {}, {err}", log_file.as_ref().display());
                    }
                    break;
                }
            };

            if entry.is_pos_len() {
                unreachable!("nursery log contained b-tree internal entries");
            }

            data.insert(entry.key().to_owned(), entry);
        }

        // Write out nursery.data from the recovered log
        if !data.is_empty() {
            let mut data_file = log_file.as_ref().to_path_buf();
            data_file.set_file_name("nursery.data");
            let mut writer = Writer::new(data_file)?;
            for (_, entry) in data.into_iter() {
                writer.add(entry)?;
            }
            writer.close()?;
        }

        // TODO: Inject nursery.data into the first level

        remove_file(log_file)?;
        Ok(())
    }
}
