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
    pub(crate) fn new(path: impl AsRef<Path>, level: u32) -> Result<Self> {
        let path: PathBuf = path.as_ref().to_path_buf();
        let a_file = path.join(format!("A-{level}.data"));
        let a = a_file
            .exists()
            .then(|| Tree::from_file(a_file))
            .transpose()?;
        let b_file = path.join(format!("B-{level}.data"));
        let b = b_file
            .exists()
            .then(|| Tree::from_file(b_file))
            .transpose()?;
        let c_file = path.join(format!("C-{level}.data"));
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

    pub(crate) fn promote_file(&mut self, path: PathBuf) -> Result<()> {
        if self.a.is_none() {
            let new_filename = self.path.join(format!("A-{}.data", self.level));
            std::fs::rename(&path, &new_filename)?;
            self.a = Some(Tree::from_file(new_filename)?);
        } else if self.b.is_none() {
            let new_filename = self.path.join(format!("B-{}.data", self.level));
            std::fs::rename(&path, &new_filename)?;
            self.b = Some(Tree::from_file(new_filename)?);
        } else if self.c.is_none() {
            let new_filename = self.path.join(format!("C-{}.data", self.level));
            std::fs::rename(&path, &new_filename)?;
            self.c = Some(Tree::from_file(new_filename)?);
        } else {
            unreachable!("level is full");
        };
        Ok(())
    }

    pub(crate) fn merge(&mut self, _steps: i32) -> Result<Vec<Command>> {
        // TODO: implement all of this
        // Situation A - If the merger closes, add promote file to the commands
        // Situation B - If there's still more merge work to be done, trigger the next level
        todo!()
    }

    fn maybe_create_merger(&mut self) -> Result<()> {
        if let (Some(a_tree), Some(b_tree), None) = (&self.a, &self.b, &self.merger) {
            self.merger = Some(Merger::new(&self.path, self.level, a_tree, b_tree)?);
        }
        Ok(())
    }
}
