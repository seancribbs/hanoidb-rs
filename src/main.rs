// #![allow(dead_code)]
use std::path::Path;

use error::Result;
use nursery::Nursery;

mod db;
mod error;
mod format;
mod level;
mod merger;
mod nursery;
mod writer;

fn main() -> Result<()> {
    let dir = std::env::args().nth(1).expect("required directory to read");
    println!("==============\n CREATING NURSERY:");
    let nursery = Nursery::new(&dir, 10)?;
    let mut nursery_data = Path::new(&dir).to_path_buf();
    nursery_data.push("nursery.data");
    if nursery_data.exists() {
        println!("Nursery recover data exists!");
        let tree = format::Tree::from_file(&nursery_data).expect("couldn't load nursery.data");
        let root = tree.root_block().expect("tree didn't have root block");
        println!(
            "    ===========\nNURSERY ROOT BLOCK len: {}, level: {}, compression: {:?}",
            root.blocklen, root.level, root.compression
        );
        println!(" Root block has {} entries", root.entries().count());
        for entry in root.entries() {
            println!("        {entry:?}");
        }
    }
    println!("  {nursery:?}");

    let file = std::fs::read_dir(dir)
        .expect("argument is not a directory")
        .next()
        .expect("directory must have at least one file in it")
        .unwrap()
        .path();
    println!("Reading {}", file.display());
    let tree = format::Tree::from_file(file).expect("didn't load file");
    let root = tree.root_block().expect("tree didn't have root block");
    println!(
        "===============\nROOT BLOCK len: {}, level: {}, compression: {:?}",
        root.blocklen, root.level, root.compression
    );
    let first_entry = root
        .entries()
        .next()
        .expect("root block should have at least one entry");
    println!("   {first_entry:?}");
    let child_block = tree
        .block_from_poslen_entry(&first_entry)
        .expect("could not find child block");
    println!(
        "===============\nCHILD BLOCK len: {}, level: {}, compression: {:?}",
        child_block.blocklen, child_block.level, child_block.compression
    );
    for entry in child_block.entries() {
        println!("\t{entry:?}");
    }
    println!("===============\nSEARCHING FOR KEY");
    let key = vec![97, 97, 97, 97];
    let result = tree.get(&key);
    println!("Found key {key:?}: {result:?}");
    Ok(())
}
