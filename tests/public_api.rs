use tempfile::tempdir;

use hanoidb::*;

#[test]
fn read_empty_database() {
    let dir = tempdir().unwrap();
    let db = HanoiDB::open(&dir).unwrap();
    let value = db.get("key".as_bytes()).unwrap();
    assert!(value.is_none());
}

#[test]
fn insert_kv_pair() {
    let dir = tempdir().unwrap();
    let mut db = HanoiDB::open(&dir).unwrap();
    let key = String::from("key").into_bytes();
    let value = String::from("value").into_bytes();
    db.insert(key.clone(), value.clone()).unwrap();
    assert_eq!(db.get(&key).unwrap(), Some(value));
}

#[test]
fn insert_and_delete() {
    let dir = tempdir().unwrap();
    let mut db = HanoiDB::open(&dir).unwrap();
    let key = String::from("key").into_bytes();
    let value = String::from("value").into_bytes();
    db.insert(key.clone(), value.clone()).unwrap();
    assert_eq!(db.get(&key).unwrap(), Some(value));
    db.delete(key.clone()).unwrap();
    assert_eq!(db.get(&key).unwrap(), None);
}

#[test]
fn open_existing_dir() {
    let dir = tempdir().unwrap();
    let key = String::from("key").into_bytes();
    let value = String::from("value").into_bytes();
    {
        // Open the database, insert a value and then drop it.
        let mut db = HanoiDB::open(&dir).unwrap();
        db.insert(key.clone(), value.clone()).unwrap();
        drop(db);
    }
    let db = HanoiDB::open(&dir).unwrap();
    assert_eq!(db.get(&key).unwrap(), Some(value));
}

#[test]
fn create_database_with_options() {
    let dir = tempdir().unwrap();
    let mut db = OpenOptions::new(&dir)
        .with_compression(Compression::Lz4)
        .open()
        .unwrap();
    for i in 0..1024 {
        let key = format!("key-{i}").into_bytes();
        let value = format!("value-{i}").into_bytes();
        db.insert(key, value)
            .map_err(|err| {
                eprintln!("Could not insert key {i} because {err:?}");
                eprintln!("Directory contents:\n{}", ls(&dir));
                err
            })
            .unwrap();
    }
    for i in 0..1024 {
        let key = format!("key-{i}").into_bytes();
        let value = format!("value-{i}").into_bytes();
        let found_value = db.get(&key).unwrap();
        assert_eq!(Some(value), found_value, "Expected to find key {}", i);
    }
}

#[test]
fn lots_of_entries() {
    let dir = tempdir().unwrap();
    let mut db = HanoiDB::open(&dir).unwrap();
    for i in 0..2048 {
        let key = format!("key-{i}").into_bytes();
        let value = format!("value-{i}").into_bytes();
        db.insert(key, value)
            .map_err(|err| {
                eprintln!("Could not insert key {i} because {err:?}");
                eprintln!("Directory contents:\n{}", ls(&dir));
                err
            })
            .unwrap();
    }
    for i in 0..2048 {
        let key = format!("key-{i}").into_bytes();
        let value = format!("value-{i}").into_bytes();
        let found_value = db.get(&key).unwrap();
        assert_eq!(Some(value), found_value, "Expected to find key {}", i);
    }
}

#[test]
fn full_database_scan() {
    let dir = tempdir().unwrap();
    let mut db = HanoiDB::open(&dir).unwrap();
    let num_keys = 1024;
    for i in 0..num_keys {
        let key = format!("key-{i:04}").into_bytes();
        let value = format!("value-{i}").into_bytes();
        db.insert(key, value)
            .map_err(|err| {
                eprintln!("Could not insert key {i} because {err:?}");
                eprintln!("Directory contents:\n{}", ls(&dir));
                err
            })
            .unwrap();
    }
    let scanner = db.scan().unwrap();
    let mut count = 0;
    for (i, (k, v)) in scanner.enumerate() {
        let key = format!("key-{i:04}").into_bytes();
        let value = format!("value-{i}").into_bytes();
        assert_eq!(k, key);
        assert_eq!(v, value);
        count += 1;
    }
    assert_eq!(count, num_keys);
}

#[test]
fn database_range_scan() {
    let dir = tempdir().unwrap();
    let mut db = HanoiDB::open(&dir).unwrap();
    let num_keys = 1024;
    for i in 0..num_keys {
        let key = format!("key-{i:04}").into_bytes();
        let value = format!("value-{i}").into_bytes();
        db.insert(key, value)
            .map_err(|err| {
                eprintln!("Could not insert key {i} because {err:?}");
                eprintln!("Directory contents:\n{}", ls(&dir));
                err
            })
            .unwrap();
    }
    let lower_bound = "key-0100".to_string().into_bytes();
    let upper_bound = "key-0500".to_string().into_bytes();
    let bounds = lower_bound.as_ref()..upper_bound.as_ref();
    let scanner = db.range_scan(bounds).unwrap();
    let mut count = 0;
    for (i, (k, v)) in scanner.enumerate() {
        let i = i + 100;
        let key = format!("key-{i:04}").into_bytes();
        let value = format!("value-{i}").into_bytes();
        assert_eq!(k, key);
        assert_eq!(v, value);
        count += 1;
    }
    assert_eq!(count, 400);
}

fn ls(path: impl AsRef<std::path::Path>) -> String {
    std::fs::read_dir(path)
        .unwrap()
        .flat_map(|dir_entry| {
            dir_entry
                .ok()
                .map(|d| format!("  {}\n", d.file_name().to_string_lossy()))
        })
        .collect()
}
