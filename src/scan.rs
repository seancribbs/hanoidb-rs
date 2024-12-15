use crate::entry::Entry;
use crate::nursery::Value;
use crate::tree::{Tree, TreeEntryIterator};
use crate::{error::*, nursery};
use crate::{level::Level, nursery::Nursery};
use std::cmp::Ordering;
use std::collections::btree_map;
use std::iter::Peekable;
use std::time::SystemTime;

pub struct Scanner {
    id: u128,
    nursery: Peekable<btree_map::IntoIter<Vec<u8>, Value>>,
    levels: Vec<Peekable<LevelScanner>>,
}

impl Scanner {
    pub fn new(nursery: &Nursery, levels: &[Level]) -> Result<Self> {
        let id = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let levels = levels
            .iter()
            .map(|level| LevelScanner::new(level, &id).map(|l| l.peekable()))
            .collect::<Result<Vec<_>>>()?;
        Ok(Self {
            id,
            nursery: nursery.data().clone().into_iter().peekable(),
            levels,
        })
    }

    fn consume_level_keys(&mut self, smallest_key_index: usize, key: &[u8]) {
        for index in smallest_key_index + 1..self.levels.len() {
            if self.levels[index].peek().map(|e| e.key()) == Some(key) {
                let _ = self.levels[index].next();
            }
        }
    }
}

impl Iterator for Scanner {
    type Item = (Vec<u8>, Vec<u8>);

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            let keys_and_indexes: Vec<(usize, Option<&[u8]>)> = self
                .levels
                .iter_mut()
                .map(|i| i.peek().map(|e| e.key()))
                .enumerate()
                .collect();
            let smallest_key_index: usize = keys_and_indexes
                .iter()
                .min_by(|a, b| match (a.1, b.1) {
                    (None, None) => Ordering::Equal,
                    (Some(_), None) => Ordering::Less,
                    (None, Some(_)) => Ordering::Greater,
                    _ => a.1.cmp(&b.1),
                })
                .map(|(i, _)| *i)
                .expect("no levels to scan");

            match self.nursery.peek().cloned() {
                Some((nursery_key, nursery_value))
                    if keys_and_indexes[smallest_key_index]
                        .1
                        .map(|k| k >= &nursery_key)
                        .unwrap_or(true) =>
                {
                    // consume all the iterators in the levels that are the same as the nursery key
                    self.consume_level_keys(0, &nursery_key);
                    match nursery_value {
                        Value::Plain(value) => return Some((nursery_key, value)),
                        Value::Deleted => {
                            // key was deleted, try the next one
                            continue;
                        }
                    };
                }
                // Either the nursery was exhausted, or the levels had a smaller key
                _ => (),
            }
            // Consume the first level iterator as the return value.
            return Some(match self.levels[smallest_key_index].next() {
                Some(entry) if entry.is_deleted() || entry.is_key_val() => {
                    let key = entry.key();

                    self.consume_level_keys(smallest_key_index, key);
                    if let Entry::KeyVal { key, value, .. } = entry {
                        (key, value)
                    } else {
                        // Key was deleted, there's nothing to return yet, try the next one
                        continue;
                    }
                }
                None => return None,
                _ => unreachable!("level iterator emitted Entry::PosLen"),
            });
        }
    }
}

struct LevelScanner {
    trees: Vec<Peekable<TreeEntryIterator>>,
}

impl LevelScanner {
    fn new(level: &Level, id: &u128) -> Result<Self> {
        let mut trees = vec![];
        for source_file in level.tree_files().iter() {
            let scan_file = source_file.with_extension(format!("scan-{id}"));
            std::fs::hard_link(source_file, &scan_file)?;
            trees.push(Tree::from_file(scan_file)?.entries()?.peekable());
        }

        Ok(Self { trees })
    }
}

impl Iterator for LevelScanner {
    type Item = Entry;

    fn next(&mut self) -> Option<Self::Item> {
        // If there are no trees in this level, don't do anything.
        if self.trees.is_empty() {
            return None;
        }

        // Goal: Find the smallest key that is not deleted
        // Peek all the trees (newest to oldest)
        // [A, B, C] => A; consume iterator on first tree
        // [B, A, C] => A; consume iterator on the second tree (same for third)
        // [A, A, A] => A(1); consume iterator on all
        // [B, A, A] => A(2); consume iterator on 2/3
        //
        // [AX, A, A] => LOOP; consume iterator all
        // [B, AX, A] => LOOP; consume iterator on 2/3
        //
        // [BX, A, A] => A(2); consume iterator on 2/3
        // [BX, B, A] => A(3); consume iterator on 3
        //
        // [_, A, A] => A(2); consume iterator on 3
        // (drop empty iterators?)
        let keys_and_indexes: Vec<_> = self
            .trees
            .iter_mut()
            .map(|i| i.peek().map(|e| e.key()))
            .enumerate()
            .collect();
        let smallest_key_index: usize = keys_and_indexes
            .iter()
            .min_by(|a, b| match (a.1, b.1) {
                (None, None) => Ordering::Equal,
                (Some(_), None) => Ordering::Less,
                (None, Some(_)) => Ordering::Greater,
                _ => a.1.cmp(&b.1),
            })
            .map(|(i, _)| *i)
            .expect("no trees to scan in level");

        // Consume the first iterator as the return value.
        Some(match self.trees[smallest_key_index].next() {
            Some(entry) if entry.is_deleted() || entry.is_key_val() => {
                let key = entry.key();
                // loop through the rest of the iterators and consume this key
                for index in smallest_key_index + 1..self.trees.len() {
                    if self.trees[index].peek().map(|e| e.key()) == Some(key) {
                        let _ = self.trees[index].next();
                    }
                }
                entry
            }
            None => return None,
            _ => unreachable!("tree iterator emitted Entry::PosLen"),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::level::Level;
    use crate::writer::Writer;

    use tempfile::tempdir;
    #[test]
    fn level_scanner() {
        let dir = tempdir().unwrap();

        // A - 1 2 4
        // B - 2 3 5 6
        // C - 1 3 6T
        //
        // => C1, B2, C3, A4, B5, C6T
        let mut a_file = Writer::new(dir.path().join("A-10.data")).unwrap();
        for key in [1, 2, 4] {
            a_file
                .add(Entry::KeyVal {
                    key: format!("{key}").into_bytes(),
                    value: format!("A{key}").into_bytes(),
                    timestamp: None,
                })
                .unwrap();
        }
        a_file.close().unwrap();
        let mut b_file = Writer::new(dir.path().join("B-10.data")).unwrap();
        for key in [2, 3, 5, 6] {
            b_file
                .add(Entry::KeyVal {
                    key: format!("{key}").into_bytes(),
                    value: format!("B{key}").into_bytes(),
                    timestamp: None,
                })
                .unwrap();
        }
        b_file.close().unwrap();
        let mut c_file = Writer::new(dir.path().join("C-10.data")).unwrap();
        c_file
            .add(Entry::KeyVal {
                key: "1".to_owned().into_bytes(),
                value: "C1".to_owned().into_bytes(),
                timestamp: None,
            })
            .unwrap();
        c_file
            .add(Entry::KeyVal {
                key: "3".to_owned().into_bytes(),
                value: "C3".to_owned().into_bytes(),
                timestamp: None,
            })
            .unwrap();
        c_file
            .add(Entry::Deleted {
                key: "6".to_owned().into_bytes(),
                timestamp: None,
            })
            .unwrap();
        c_file.close().unwrap();

        let level = Level::new(&dir, 10, Default::default()).unwrap();
        let id: u128 = 123456;
        let scanner = LevelScanner::new(&level, &id).unwrap();
        // => C1, B2, C3, A4, B5, C6T
        assert_eq!(
            scanner.collect::<Vec<Entry>>(),
            vec![
                Entry::KeyVal {
                    key: "1".to_owned().into_bytes(),
                    value: "C1".to_owned().into_bytes(),
                    timestamp: None,
                },
                Entry::KeyVal {
                    key: "2".to_string().into_bytes(),
                    value: "B2".to_string().into_bytes(),
                    timestamp: None,
                },
                Entry::KeyVal {
                    key: "3".to_owned().into_bytes(),
                    value: "C3".to_owned().into_bytes(),
                    timestamp: None,
                },
                Entry::KeyVal {
                    key: "4".to_string().into_bytes(),
                    value: "A4".to_string().into_bytes(),
                    timestamp: None,
                },
                Entry::KeyVal {
                    key: "5".to_string().into_bytes(),
                    value: "B5".to_string().into_bytes(),
                    timestamp: None,
                },
                Entry::Deleted {
                    key: "6".to_owned().into_bytes(),
                    timestamp: None,
                },
            ]
        )
    }
}
