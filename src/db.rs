use std::collections::VecDeque;
use std::path::{Path, PathBuf};

use crate::error::*;
use crate::level::{level_size, Level};
use crate::nursery::Nursery;

pub struct HanoiDB {
    path: PathBuf,
    nursery: Nursery,
    min_level: u32,
    max_level: u32,
    levels: Vec<Level>,
}

impl HanoiDB {
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        Self::open_with_min_and_max_levels(path, 10, 25)
    }

    pub fn open_with_min_and_max_levels(
        path: impl AsRef<Path>,
        min_level: u32,
        max_level: u32,
    ) -> Result<Self> {
        let path = path.as_ref().to_path_buf();
        let (nursery, recovery) = Nursery::new(&path, min_level)?;
        let levels = (min_level..=max_level)
            .map(|level| Level::new(&path, level))
            .collect::<Result<Vec<Level>>>()?;
        let mut db = Self {
            path,
            nursery,
            min_level,
            max_level,
            levels,
        };
        // Promote nursery.data into the first level, if it was recovered
        if let Some(command) = recovery {
            let commands = db.handle_command(command)?;
            assert!(commands.is_empty());
        }
        Ok(db)
    }

    fn handle_commands(&mut self, commands: Vec<Command>) -> Result<()> {
        let mut commands = VecDeque::from(commands);
        while let Some(command) = commands.pop_front() {
            // TODO: Should we handle additional commands immediately in a
            // recursive fashion?
            let extra_commands = self.handle_command(command)?;
            commands.extend(extra_commands);
        }
        Ok(())
    }

    fn handle_command(&mut self, command: Command) -> Result<Vec<Command>> {
        let step_size = level_size(self.min_level) / 2;
        let min_level = self.min_level;
        let max_level = self.max_level;
        match command {
            Command::PromoteFile { path, target_level } => {
                self.level_mut(target_level).unwrap().promote_file(path)
            }
            Command::Merge {
                steps,
                target_level,
            } if target_level <= self.max_level => self
                .level_mut(target_level)
                .unwrap()
                .merge(steps, step_size, min_level, max_level),
            Command::Merge { .. } => {
                // NOTE: If we reached the largest level already, no more merges
                // can be done
                Ok(vec![])
            }
        }
    }

    fn level_mut(&mut self, level: u32) -> Option<&mut Level> {
        assert!(
            (self.min_level..=self.max_level).contains(&level),
            "invalid level number"
        );
        let index = level - self.min_level;
        self.levels.get_mut(index as usize)
    }
}

#[derive(Debug, Clone)]
pub(crate) enum Command {
    PromoteFile { path: PathBuf, target_level: u32 },
    Merge { steps: usize, target_level: u32 },
}
