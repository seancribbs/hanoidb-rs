mod block;
mod compression;
mod db;
mod entry;
mod error;
mod level;
mod merger;
mod nursery;
mod trailer;
mod tree;
mod writer;

const TAG_KV_DATA: u8 = 0x80;
const TAG_DELETED: u8 = 0x81;
const TAG_POSLEN32: u8 = 0x82;
#[allow(dead_code)]
const TAG_TRANSACT: u8 = 0x83;
const TAG_KV_DATA2: u8 = 0x84;
const TAG_DELETED2: u8 = 0x85;
const TAG_END: u8 = 0xFF;
const MAGIC: &str = "HAN3";

pub use compression::Compression;
pub use db::{HanoiDB, OpenOptions};
pub use error::*;
