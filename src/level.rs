use std::path::{Path, PathBuf};

use crate::db::Command;
use crate::error::*;
use crate::format::*;
use crate::merger::*;

pub struct Level {
    level: u32,
    path: PathBuf,
    a: Option<Tree>,
    b: Option<Tree>,
    c: Option<Tree>,
    merger: Option<Merger>,
}

impl Level {
    pub fn new(path: impl AsRef<Path>, level: u32) -> Result<Self> {
        let path: PathBuf = path.as_ref().to_path_buf();
        let a_file = data_file_name(&path, level, "A");
        let a = a_file
            .exists()
            .then(|| Tree::from_file(a_file))
            .transpose()?;
        let b_file = data_file_name(&path, level, "B");
        let b = b_file
            .exists()
            .then(|| Tree::from_file(b_file))
            .transpose()?;
        let c_file = data_file_name(&path, level, "C");
        let c = c_file
            .exists()
            .then(|| Tree::from_file(c_file))
            .transpose()?;
        let mut level = Self {
            level,
            path,
            a,
            b,
            c,
            merger: None,
        };
        level.maybe_create_merger()?;
        Ok(level)
    }

    pub fn get_entry(&self, key: &[u8]) -> Result<Option<Entry>> {
        for tree in [&self.c, &self.b, &self.a].into_iter().flatten() {
            let entry = tree.get_entry(key)?;
            if entry.is_some() {
                return Ok(entry);
            }
        }
        Ok(None)
    }

    pub fn promote_file(&mut self, path: PathBuf) -> Result<Vec<Command>> {
        if self.a.is_none() {
            let new_filename = self.data_file_name("A");
            std::fs::rename(&path, &new_filename)?;
            self.a = Some(Tree::from_file(new_filename)?);
        } else if self.b.is_none() {
            let new_filename = self.data_file_name("B");
            std::fs::rename(&path, &new_filename)?;
            self.b = Some(Tree::from_file(new_filename)?);
        } else if self.c.is_none() {
            let new_filename = self.data_file_name("C");
            std::fs::rename(&path, &new_filename)?;
            self.c = Some(Tree::from_file(new_filename)?);
        } else {
            unreachable!("level is full");
        };
        Ok(vec![])
    }

    pub fn merge(
        &mut self,
        work_completed: usize,
        work_unit: usize,
        min_level: u32,
        max_level: u32,
    ) -> Result<Vec<Command>> {
        self.maybe_create_merger()?;
        if let Some(merger) = self.merger.take() {
            let mut commands = vec![];

            // Compute how many steps to take in this merge

            // TODO: track how much was actually done previously
            // Put this in the merger?
            let work_left_here = level_size(self.level) * 2;
            // TODO: can self.level ever be greater than max_level as configured?
            // let max_level = max_level.max(self.level);
            let depth = max_level - min_level + 1;
            let work_units_left = ((depth as usize) * work_unit).saturating_sub(work_completed);
            // This assumes the "fast" merge strategy, as opposed to the "predictable" strategy.
            // See https://github.com/basho/hanoidb/issues/7
            let steps = work_units_left.min(work_left_here);
            let work_including_here = steps + work_completed;

            // If there's no merge budget left, stop propagating merge commands
            // to the next level
            if steps == 0 {
                return Ok(vec![]);
            }

            match merger.incremental_merge(steps)? {
                MergeOutcome::Continue(new_merger) => {
                    // Merge is incomplete, put it back into the struct member and
                    // propagate merge work
                    commands.push(Command::Merge {
                        steps: work_including_here,
                        target_level: self.level + 1,
                    });
                    self.merger.replace(new_merger);
                }
                MergeOutcome::Complete {
                    count,
                    steps: completed,
                } => {
                    // Merge completed, the X-{level}.data file is possibly ready
                    // to be promoted to the next level
                    match count {
                        0 => {
                            // This file merged into 0 entries, so cleanup
                            self.close_and_delete_a_and_b()?;
                            if self.c.take().is_some() {
                                let a = self.data_file_name("A");
                                let c = self.data_file_name("C");
                                std::fs::rename(&c, &a)?;
                                self.a.replace(Tree::from_file(a)?);
                            }
                        }
                        count if count <= level_size(self.level) => {
                            // The merged file is small enough to fit into this level.
                            // Move it to "M" temporarily so it will be picked up in recovery
                            let a = self.data_file_name("A");
                            let m = self.data_file_name("M");
                            std::fs::rename(self.data_file_name("X"), &m)?;
                            self.close_and_delete_a_and_b()?;
                            std::fs::rename(&m, &a)?;
                            self.a.replace(Tree::from_file(a)?);
                            if self.c.take().is_some() {
                                let c = self.data_file_name("C");
                                let b = self.data_file_name("B");
                                std::fs::rename(&c, &b)?;
                                self.b.replace(Tree::from_file(b)?);
                            }
                        }
                        _ => {
                            self.close_and_delete_a_and_b()?;
                            commands.push(Command::PromoteFile {
                                path: self.data_file_name("X"),
                                target_level: self.level + 1,
                            });
                        }
                    }
                    // If there's still more merge work to be done, trigger the next level
                    commands.push(Command::Merge {
                        steps: work_including_here - completed,
                        target_level: self.level + 1,
                    });
                }
            }
            Ok(commands)
        } else {
            let commands = if self.level < max_level {
                vec![Command::Merge {
                    steps: work_completed,
                    target_level: self.level + 1,
                }]
            } else {
                vec![]
            };
            Ok(commands)
        }
    }

    fn maybe_create_merger(&mut self) -> Result<()> {
        if let (Some(a_tree), Some(b_tree), None) = (&self.a, &self.b, &self.merger) {
            self.merger = Some(Merger::new(&self.path, self.level, a_tree, b_tree)?);
        }
        Ok(())
    }

    fn data_file_name(&self, prefix: &str) -> PathBuf {
        data_file_name(&self.path, self.level, prefix)
    }

    fn close_and_delete_a_and_b(&mut self) -> Result<()> {
        assert!(self.a.is_some() && self.b.is_some());
        let _ = self.a.take();
        let _ = self.b.take();
        std::fs::remove_file(self.data_file_name("A"))?;
        std::fs::remove_file(self.data_file_name("B"))?;
        Ok(())
    }
}

fn data_file_name(path: &Path, level: u32, prefix: &str) -> PathBuf {
    path.join(format!("{prefix}-{level}.data"))
}

#[inline]
pub fn level_size(level: u32) -> usize {
    1 << level as usize
}
