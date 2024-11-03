use std::collections::BTreeMap;
use std::fs::{remove_file, File, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};

use crate::db::Command;
use crate::error::*;
use crate::format::Entry;
use crate::writer::Writer;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Value {
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
            let mut writer = Writer::with_expected_num_items(&filename, min_level_size)?;
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
            let mut writer = Writer::with_expected_num_items(&data_file, 1 << target_level)?;
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

#[cfg(test)]
mod tests {
    const MIN_LEVEL: u32 = 10;
    use super::*;
    use tempfile::tempdir;

    // Creating a new one without recovery
    #[test]
    fn fresh_nursery() {
        let dir = tempdir().unwrap();
        let (nursery, command) = Nursery::new(&dir, MIN_LEVEL).unwrap();
        assert!(command.is_none(), "fresh nursery wasn't empty");
        let recovery_data = dir.as_ref().join("nursery.data");
        let log = dir.as_ref().join("nursery.log");
        assert!(
            !std::fs::exists(recovery_data).unwrap(),
            "recovery data was written on fresh nursery"
        );
        assert_eq!(nursery.total_size, 0);
        assert!(nursery.data.is_empty());
        assert_eq!(0, std::fs::metadata(&log).unwrap().len());
    }

    // Recovering an existing nursery (just a log)
    #[test]
    fn recover_nursery() {
        let dir = tempdir().unwrap();
        let recovery_data = dir.as_ref().join("nursery.data");
        let log = dir.as_ref().join("nursery.log");
        // Create a nursery and immediately drop it, leaving data in its log.
        {
            let (mut nursery, _) = Nursery::new(&dir, MIN_LEVEL).unwrap();
            let commands = nursery
                .add("key".as_bytes().to_owned(), "value".as_bytes().to_owned())
                .unwrap();
            assert!(commands.is_empty());
        }
        let (nursery, command) = Nursery::new(&dir, MIN_LEVEL).unwrap();
        assert!(
            std::fs::exists(&recovery_data).unwrap(),
            "recovery data was not written for fresh nursery"
        );
        assert_eq!(
            command,
            Some(Command::PromoteFile {
                path: recovery_data,
                target_level: 10
            })
        );
        assert_eq!(nursery.total_size, 0);
        assert!(nursery.data.is_empty());
        assert_eq!(0, std::fs::metadata(&log).unwrap().len());
    }

    // Write a KV pair and read it back
    #[test]
    fn write_and_read() {
        let dir = tempdir().unwrap();
        let log = dir.as_ref().join("nursery.log");
        let key = "key".as_bytes().to_owned();
        let value = "value".as_bytes().to_owned();
        let (mut nursery, _) = Nursery::new(&dir, MIN_LEVEL).unwrap();
        let commands = nursery.add(key.clone(), value.clone()).unwrap();
        assert!(
            commands.is_empty(),
            "empty nursery should not cause merges or promotions after one key is inserted"
        );
        assert_ne!(
            0,
            std::fs::metadata(&log).unwrap().len(),
            "nursery log was not written to"
        );
        assert_ne!(0, nursery.total_size);
        assert_eq!(Some(&Value::Plain(value)), nursery.get_value(&key));
    }

    // Delete a key and read it back
    #[test]
    fn delete_and_read() {
        let dir = tempdir().unwrap();
        let log = dir.as_ref().join("nursery.log");
        let key = "key".as_bytes().to_owned();
        let (mut nursery, _) = Nursery::new(&dir, MIN_LEVEL).unwrap();
        let commands = nursery.delete(key.clone()).unwrap();
        assert!(
            commands.is_empty(),
            "empty nursery should not cause merges or promotions after one key is inserted"
        );
        assert_ne!(
            0,
            std::fs::metadata(&log).unwrap().len(),
            "nursery log was not written to"
        );
        assert_ne!(0, nursery.total_size);
        assert_eq!(Some(&Value::Deleted), nursery.get_value(&key));
    }

    // Writing enough values to force merging
    #[test]
    fn trigger_incremental_merge() {
        let dir = tempdir().unwrap();
        let log = dir.as_ref().join("nursery.log");
        let (mut nursery, _) = Nursery::new(&dir, MIN_LEVEL).unwrap();
        let mut commands = vec![];
        // Write 512 KV pairs into the nursery, triggering
        // incremental merge at 1/2 the smallest level size
        for i in 0..512 {
            let key = format!("key-{i}").into_bytes();
            let value = format!("value-{i}").into_bytes();
            let step_commands = nursery.add(key, value).unwrap();
            commands.extend(step_commands);
        }
        assert_ne!(
            0,
            std::fs::metadata(&log).unwrap().len(),
            "nursery log was not written to"
        );
        assert_ne!(0, nursery.total_size);
        assert_eq!(1, commands.len(), "should have produced a merge");
        assert_eq!(
            commands[0],
            Command::Merge {
                steps: 512,
                target_level: 10
            }
        );
    }

    // Writing enough values to force promotion
    #[test]
    fn trigger_promotion() {
        let dir = tempdir().unwrap();
        let log = dir.as_ref().join("nursery.log");
        let data = dir.as_ref().join("nursery.data");

        let (mut nursery, _) = Nursery::new(&dir, MIN_LEVEL).unwrap();
        let mut commands = vec![];
        // Write 1024 KV pairs into the nursery, triggering promotion
        // of the nursery data into the first level
        for i in 0..1024 {
            let key = format!("key-{i}").into_bytes();
            let value = format!("value-{i}").into_bytes();
            let step_commands = nursery.add(key, value).unwrap();
            commands.extend(step_commands);
        }
        assert_eq!(
            0,
            std::fs::metadata(&log).unwrap().len(),
            "nursery log was not truncated after data promotion"
        );
        assert_ne!(
            0,
            std::fs::metadata(&data).unwrap().len(),
            "nursery data file for promotion was empty"
        );
        assert_ne!(0, nursery.total_size);
        assert_eq!(
            3,
            commands.len(),
            "should have produced two merges and a promotion"
        );
        assert_eq!(
            commands,
            [
                Command::Merge {
                    steps: 512,
                    target_level: 10
                },
                Command::PromoteFile {
                    path: data.clone(),
                    target_level: 10
                },
                Command::Merge {
                    steps: 512,
                    target_level: 10
                },
            ]
        );
    }
}
