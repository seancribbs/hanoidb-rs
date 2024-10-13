use std::collections::BTreeMap;
use std::fs::{remove_file, File, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};

use crate::db::Command;
use crate::error::*;
use crate::format::Entry;
use crate::writer::Writer;

#[derive(Debug, Clone)]
pub(crate) enum Value {
    Plain(Vec<u8>),
    // Timestampped(Vec<u8>, time value?)
    Deleted,
}

type NurseryData = BTreeMap<Vec<u8>, Value>;

#[derive(Debug)]
pub struct Nursery {
    log: File,
    directory: PathBuf,
    data: NurseryData,
    min_level: u32,
    total_size: usize,
    step: usize,
    merge_done: usize,
}

impl Nursery {
    pub fn new(directory: impl AsRef<Path>, min_level: u32) -> Result<(Self, Option<Command>)> {
        let directory = directory.as_ref().to_path_buf();
        let file = directory.join("nursery.log");
        let recovery = Self::recover(&file, min_level)?;
        let log = OpenOptions::new()
            .create_new(true)
            .append(true)
            .open(file)?;
        Ok((
            Self {
                log,
                directory,
                data: Default::default(),
                min_level,
                total_size: 0,
                step: 0,
                merge_done: 0,
            },
            recovery,
        ))
    }

    pub fn get_value(&self, key: &[u8]) -> Option<&Value> {
        self.data.get(key)
    }

    pub fn add(&mut self, key: Vec<u8>, value: Vec<u8>) -> Result<Vec<Command>> {
        let bin_entry = Entry::KeyVal {
            key: key.clone(),
            value: value.clone(),
            timestamp: None,
        }
        .encode();
        self.write_internal(key, Value::Plain(value), bin_entry)
    }

    pub fn delete(&mut self, key: Vec<u8>) -> Result<Vec<Command>> {
        let bin_entry = Entry::Deleted {
            key: key.clone(),
            timestamp: None,
        }
        .encode();
        self.write_internal(key, Value::Deleted, bin_entry)
    }

    fn write_internal(
        &mut self,
        key: Vec<u8>,
        value: Value,
        bin_entry: Vec<u8>,
    ) -> Result<Vec<Command>> {
        self.data.insert(key, value);
        self.log.write_all(&bin_entry)?;
        self.log.sync_data()?;
        self.total_size += bin_entry.len();
        let mut commands = vec![];

        // Check if the in-memory data is big enough to promote to the next level
        let min_level_size = 1 << self.min_level;
        if self.data.len() >= min_level_size {
            let filename = self.directory.join("nursery.data");
            let mut writer = Writer::new(&filename)?;
            let data = std::mem::take(&mut self.data);
            for (key, value) in data.into_iter() {
                let entry = match value {
                    Value::Plain(value) => Entry::KeyVal {
                        key,
                        value,
                        timestamp: None,
                    },
                    Value::Deleted => Entry::Deleted {
                        key,
                        timestamp: None,
                    },
                };
                writer.add(entry)?;
            }
            writer.close()?;
            commands.push(Command::PromoteFile {
                path: filename,
                target_level: self.min_level,
            });

            // Truncate the log file and replace the existing handle
            self.log = OpenOptions::new()
                .write(true)
                .truncate(true)
                .open(self.directory.join("nursery.log"))?;
        }

        // Trigger incremental merge
        let min_steps_to_merge = min_level_size / 2;
        if self.step + 1 >= min_steps_to_merge {
            commands.push(Command::Merge {
                steps: self.step + 1,
                target_level: self.min_level,
            });
            self.step = 0;
        } else {
            self.step += 1;
        }
        Ok(commands)
    }

    fn recover(log_file: impl AsRef<Path>, target_level: u32) -> Result<Option<Command>> {
        if !log_file.as_ref().exists() {
            return Ok(None);
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
        let command = if !data.is_empty() {
            let mut data_file = log_file.as_ref().to_path_buf();
            data_file.set_file_name("nursery.data");
            let mut writer = Writer::new(&data_file)?;
            for (_, entry) in data.into_iter() {
                writer.add(entry)?;
            }
            writer.close()?;

            Some(Command::PromoteFile {
                path: data_file,
                target_level,
            })
        } else {
            None
        };

        remove_file(log_file)?;
        Ok(command)
    }
}
