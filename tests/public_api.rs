use tempfile::tempdir;

use hanoidb::HanoiDB;

#[test]
fn read_empty_database() {
    let dir = tempdir().unwrap();
    let db = HanoiDB::open(&dir).unwrap();
    let value = db.get("key".as_bytes()).unwrap();
    assert!(value.is_none());
    dir.close().unwrap();
}

#[test]
fn insert_kv_pair() {
    let dir = tempdir().unwrap();
    let mut db = HanoiDB::open(&dir).unwrap();
    let key = String::from("key").into_bytes();
    let value = String::from("value").into_bytes();
    db.insert(key.clone(), value.clone()).unwrap();
    assert_eq!(db.get(&key).unwrap(), Some(value));
    dir.close().unwrap();
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
    dir.close().unwrap();
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
    dir.close().unwrap();
}

#[test]
fn lots_of_entries() {
    let dir = tempdir().unwrap();
    let mut db = HanoiDB::open(&dir).unwrap();
    for i in 0..2048 {
        let key = format!("key-{i}").into_bytes();
        let value = format!("value-{i}").into_bytes();
        db.insert(key, value).unwrap();
    }
    dir.close().unwrap();
}
