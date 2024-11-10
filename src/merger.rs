use std::cmp::Ordering;
use std::iter::Peekable;
use std::path::Path;

use crate::error::*;
use crate::tree::{OwnedTreeEntryIterator, Tree};
use crate::writer::Writer;

pub struct Merger {
    a: Peekable<OwnedTreeEntryIterator>,
    b: Peekable<OwnedTreeEntryIterator>,
    x: Writer,
}

impl std::fmt::Debug for Merger {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Merger").field("writer", &self.x).finish()
    }
}

impl Merger {
    pub fn new(path: impl AsRef<Path>, level: u32, a_tree: &Tree, b_tree: &Tree) -> Result<Self> {
        let a = a_tree.entries_owned()?.peekable();
        let b = b_tree.entries_owned()?.peekable();
        let xfile = path.as_ref().to_path_buf().join(format!("X-{level}.data"));
        let x = Writer::with_expected_num_items(&xfile, 1 << (level + 1))?;
        Ok(Self { a, b, x })
    }

    pub fn incremental_merge(mut self, work: usize) -> Result<MergeOutcome> {
        for i in 0..work {
            let step = self.merge_step()?;
            if step == 0 {
                let count = self.x.count();
                return self.x.close().map(|_| MergeOutcome::Complete {
                    count,
                    steps: i + 1,
                });
            }
        }
        Ok(MergeOutcome::Continue(self))
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

#[allow(clippy::large_enum_variant)]
// NOTE: We need to consume the merger when its work is complete, but if
// it is incomplete, it should continue to persist. Therefore, we suppress
// the large enum variant warning to allow it to be threaded through.
//
// The other option is to pass a smart pointer of some sort (Option<Merger>?) to
// the incremental_merge method so that the contents can be consumed when the merge
// finishes.
pub enum MergeOutcome {
    Continue(Merger),
    Complete { count: usize, steps: usize },
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::entry::Entry;
    use crate::tree::Tree;
    use crate::writer::tests::write_8kb;
    use tempfile::tempdir;

    // Completed incremental merge returns Complete and consumes the merger
    #[test]
    fn complete_incremental_merge() {
        let dir = tempdir().unwrap();

        // "A"
        let a_data = dir.as_ref().join("A-10.data");
        let mut a_writer = Writer::new(&a_data).unwrap();
        a_writer
            .add(Entry::KeyVal {
                key: "a".as_bytes().to_vec(),
                value: "a_value".as_bytes().to_vec(),
                timestamp: None,
            })
            .unwrap();
        a_writer.close().unwrap();

        // "B"
        let b_data = dir.as_ref().join("B-10.data");
        let mut b_writer = Writer::new(&b_data).unwrap();
        b_writer
            .add(Entry::KeyVal {
                key: "b".as_bytes().to_vec(),
                value: "b_value".as_bytes().to_vec(),
                timestamp: None,
            })
            .unwrap();
        b_writer.close().unwrap();

        // Open the trees and do a complete merge
        let a_tree = Tree::from_file(&a_data).unwrap();
        let b_tree = Tree::from_file(&b_data).unwrap();
        let merger = Merger::new(&dir, 10, &a_tree, &b_tree).unwrap();

        let result = merger.incremental_merge(512).unwrap();
        let MergeOutcome::Complete { count, steps } = result else {
            panic!("merge did not complete");
        };
        assert_eq!(count, 2);
        assert_eq!(steps, 3);
        assert!(std::fs::exists(dir.as_ref().join("X-10.data")).unwrap());
    }

    // Incomplete incremental merges return Continue
    #[test]
    fn incomplete_merge() {
        let dir = tempdir().unwrap();

        // "A"
        let a_data = dir.as_ref().join("A-10.data");
        let mut a_writer = Writer::new(&a_data).unwrap();
        let _a_end = write_8kb(&mut a_writer, 0).unwrap();
        a_writer.close().unwrap();

        // "B"
        let b_data = dir.as_ref().join("B-10.data");
        let mut b_writer = Writer::new(&b_data).unwrap();
        let _b_end = write_8kb(&mut b_writer, 0).unwrap();
        b_writer.close().unwrap();

        // Open the trees and do an incomplete merge
        let a_tree = Tree::from_file(&a_data).unwrap();
        let b_tree = Tree::from_file(&b_data).unwrap();
        let merger = Merger::new(&dir, 10, &a_tree, &b_tree).unwrap();

        let result = merger.incremental_merge(1).unwrap();
        assert!(matches!(result, MergeOutcome::Continue(_)));
    }

    // Completed merge with disjoint keysets results in a merged file with all keys
    #[test]
    fn complete_merge_with_disjoint_keys() {
        let dir = tempdir().unwrap();

        // "A"
        let a_data = dir.as_ref().join("A-10.data");
        let mut a_writer = Writer::new(&a_data).unwrap();
        let a_key = "a".as_bytes().to_vec();
        a_writer
            .add(Entry::KeyVal {
                key: a_key.clone(),
                value: "a_value".as_bytes().to_vec(),
                timestamp: None,
            })
            .unwrap();
        a_writer.close().unwrap();

        // "B"
        let b_data = dir.as_ref().join("B-10.data");
        let mut b_writer = Writer::new(&b_data).unwrap();
        let b_key = "b".as_bytes().to_vec();
        b_writer
            .add(Entry::KeyVal {
                key: b_key.clone(),
                value: "b_value".as_bytes().to_vec(),
                timestamp: None,
            })
            .unwrap();
        b_writer.close().unwrap();

        // Open the trees and do a complete merge
        let a_tree = Tree::from_file(&a_data).unwrap();
        let b_tree = Tree::from_file(&b_data).unwrap();
        let merger = Merger::new(&dir, 10, &a_tree, &b_tree).unwrap();

        let result = merger.incremental_merge(512).unwrap();
        assert!(matches!(result, MergeOutcome::Complete { .. }));

        let x_tree = Tree::from_file(dir.as_ref().join("X-10.data")).unwrap();
        assert!(x_tree.get_entry(&a_key).unwrap().is_some());
        assert!(x_tree.get_entry(&b_key).unwrap().is_some());
    }

    // Merging overlapping keysets prefers the new file ("B")
    #[test]
    fn complete_merge_with_overlapping() {
        let dir = tempdir().unwrap();

        // "A"
        let a_data = dir.as_ref().join("A-10.data");
        let mut a_writer = Writer::new(&a_data).unwrap();
        let a_key = "a".as_bytes().to_vec();
        a_writer
            .add(Entry::KeyVal {
                key: a_key.clone(),
                value: "a_value".as_bytes().to_vec(),
                timestamp: None,
            })
            .unwrap();
        a_writer.close().unwrap();

        // "B"
        let b_data = dir.as_ref().join("B-10.data");
        let mut b_writer = Writer::new(&b_data).unwrap();
        let b_value = "b_value".as_bytes().to_vec();
        b_writer
            .add(Entry::KeyVal {
                key: a_key.clone(),
                value: b_value.clone(),
                timestamp: None,
            })
            .unwrap();
        b_writer.close().unwrap();

        // Open the trees and do a complete merge
        let a_tree = Tree::from_file(&a_data).unwrap();
        let b_tree = Tree::from_file(&b_data).unwrap();
        let merger = Merger::new(&dir, 10, &a_tree, &b_tree).unwrap();

        let result = merger.incremental_merge(512).unwrap();
        assert!(matches!(result, MergeOutcome::Complete { .. }));

        let x_tree = Tree::from_file(dir.as_ref().join("X-10.data")).unwrap();
        assert_eq!(
            x_tree.get_entry(&a_key).unwrap().unwrap(),
            Entry::KeyVal {
                key: a_key,
                value: b_value,
                timestamp: None
            }
        );
    }
}
