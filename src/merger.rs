use std::cmp::Ordering;
use std::fs::{File, OpenOptions};
use std::io::Write;
use std::iter::Peekable;
use std::path::Path;

use crate::error::*;
use crate::format::{Entry, TreeEntryIterator};
use crate::writer::Writer;

pub struct Merger<'a> {
    a: Peekable<TreeEntryIterator<'a>>,
    b: Peekable<TreeEntryIterator<'a>>,
    x: Writer,
}

impl<'a> Merger<'a> {
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
