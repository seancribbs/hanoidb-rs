use std::cmp::Ordering;
use std::iter::Peekable;
use std::path::Path;

use crate::error::*;
use crate::format::{OwnedTreeEntryIterator, Tree};
use crate::writer::Writer;

pub struct Merger {
    a: Peekable<OwnedTreeEntryIterator>,
    b: Peekable<OwnedTreeEntryIterator>,
    x: Writer,
    n: usize,
}

impl std::fmt::Debug for Merger {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Merger")
            .field("writer", &self.x)
            .field("n", &self.n)
            .finish()
    }
}

impl Merger {
    pub(crate) fn new(
        path: impl AsRef<Path>,
        level: usize,
        a_tree: &Tree,
        b_tree: &Tree,
    ) -> Result<Self> {
        let a = a_tree.entries_owned()?.peekable();
        let b = b_tree.entries_owned()?.peekable();
        let xfile = path.as_ref().to_path_buf().join(format!("X-{level}.data"));
        let x = Writer::new(&xfile)?;
        Ok(Self { a, b, x, n: 0 })
    }

    pub(crate) fn incremental_merge(mut self, work: usize) -> Result<Option<Self>> {
        self.n += work;
        while self.n > 0 {
            let step = self.merge_step()?;
            if step == 0 {
                return self.x.close().map(|_| None);
            }
            self.n = self.n.saturating_sub(step);
        }
        Ok(Some(self))
    }

    fn merge_step(&mut self) -> Result<usize> {
        match (self.a.peek(), self.b.peek()) {
            (None, None) => Ok(0),
            (Some(_), None) => {
                let entry = self.a.next().unwrap();
                self.x.add(entry).map(|_| 1)
            }
            (None, Some(_)) => {
                let entry = self.b.next().unwrap();
                self.x.add(entry).map(|_| 1)
            }
            (Some(a), Some(b)) => {
                let order = a.key().cmp(b.key());
                let (entry, count) = match order {
                    Ordering::Less => (self.a.next().unwrap(), 1),
                    Ordering::Equal => {
                        // discard A because it's older
                        let _ = self.a.next().unwrap();
                        (self.b.next().unwrap(), 2)
                    }
                    Ordering::Greater => (self.b.next().unwrap(), 1),
                };
                self.x.add(entry).map(|_| count)
            }
        }
    }
}