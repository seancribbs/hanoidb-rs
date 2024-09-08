use std::path::{Path, PathBuf};

use crate::error::*;
use crate::format::*;
use crate::merger::*;

pub struct Level {
    level: usize,
    path: PathBuf,
    a: Option<Tree>,
    b: Option<Tree>,
    c: Option<Tree>,
    merger: Option<Merger>,
}

impl Level {
    pub(crate) fn new(path: impl AsRef<Path>, level: usize) -> Result<Self> {
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

    fn maybe_create_merger(&mut self) -> Result<()> {
        if let (Some(a_tree), Some(b_tree), None) = (&self.a, &self.b, &self.merger) {
            self.merger = Some(Merger::new(&self.path, self.level, a_tree, b_tree)?);
        }
        Ok(())
    }
}
