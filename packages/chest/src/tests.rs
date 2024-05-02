use cuid::cuid2;

use crate::{filter::bloom::BloomFilter, value::Value};

use super::*;

fn ensure_dir_exists(dir_path: &PathBuf) -> std::io::Result<()> {
    if !(dir_path.exists() || !dir_path.is_dir()) {
        std::fs::create_dir(dir_path)?;
    }
    Ok(())
}

fn get_test_tempdir() -> PathBuf {
    let dungeon_tests_dir = std::env::temp_dir().join("dungeon-tests");
    ensure_dir_exists(&dungeon_tests_dir).unwrap();
    let path = dungeon_tests_dir.join(format!("chest-{}", cuid2()));
    ensure_dir_exists(&path).unwrap();
    path
}

#[test]
fn memtable_set_get() {
    let chest_dir = get_test_tempdir();
    let mut chest = Chest::new(
        chest_dir.to_str().unwrap(),
        1024,
        8,
        Box::new(BloomFilter::default()),
    )
    .unwrap();
    let val1 = TimeStampedValue::new(Value::String("John Doe".to_owned()));
    chest.set("name", val1.clone()).unwrap();
    assert_eq!(chest.get("name").unwrap(), Some(val1));
}

#[test]
fn test_flush() {
    let chest_dir = get_test_tempdir();
    let mut chest = Chest::new(
        chest_dir.to_str().unwrap(),
        2,
        8,
        Box::new(BloomFilter::default()),
    )
    .unwrap();
    chest
        .set(
            "name",
            TimeStampedValue::new(Value::String("John Doe".to_owned())),
        )
        .unwrap();
    assert_eq!(chest.len(), 1);
    chest
        .set("age", TimeStampedValue::new(Value::Integer(5)))
        .unwrap();
    assert_eq!(chest.len(), 0);
}
#[test]
fn test_read_from_sstable() {
    let chest_dir = get_test_tempdir();
    let mut chest = Chest::new(
        chest_dir.to_str().unwrap(),
        2,
        8,
        Box::new(BloomFilter::default()),
    )
    .unwrap();
    let val1 = TimeStampedValue::new(Value::String("bar".to_string()));
    chest.set("foo", val1.clone()).unwrap();
    let val2 = TimeStampedValue::new(Value::String("bar2".to_string()));
    chest.set("foo2", val2.clone()).unwrap();
    assert_eq!(chest.len(), 0);
    assert_eq!(chest.get("foo").unwrap(), Some(val1));
    assert_eq!(chest.get("foo2").unwrap(), Some(val2));
}

#[test]
fn test_reinitialize_chest() {
    let chest_dir = get_test_tempdir();
    let mut chest = Chest::new(
        chest_dir.to_str().unwrap(),
        1024,
        8,
        Box::new(BloomFilter::default()),
    )
    .unwrap();
    let val1 = TimeStampedValue::new(Value::String("bar".to_owned()));

    chest.set("foo", val1.clone()).unwrap();
    drop(chest);

    let chest2 = Chest::new(
        chest_dir.to_str().unwrap(),
        1024,
        8,
        Box::new(BloomFilter::default()),
    )
    .unwrap();
    assert_eq!(chest2.get("foo").unwrap(), Some(val1));
}
#[test]
fn test_merge_sstables() {
    let chest_dir = get_test_tempdir();
    let mut chest = Chest::new(
        chest_dir.to_str().unwrap(),
        1,
        8,
        Box::new(BloomFilter::default()),
    )
    .unwrap();
    let val = TimeStampedValue::new(Value::String("barz".to_string()));
    chest
        .set(
            "foo",
            TimeStampedValue::new(Value::String("bar".to_string())),
        )
        .unwrap();
    chest.set("foo", val.clone()).unwrap();

    let mut iter_chest_sstables = chest.sstables.iter().cloned();
    let mut table1 = iter_chest_sstables.next().unwrap();
    let mut table2 = iter_chest_sstables.next().unwrap();

    let merged = table1
        .0
        .merge(&mut table2.0, generate_sstable_name())
        .unwrap();
    assert_eq!(merged.get("foo").unwrap(), Some(val));
}

#[test]
fn test_merge_sstables_on_limit() {
    let chest_dir = get_test_tempdir();
    let mut chest = Chest::new(
        chest_dir.to_str().unwrap(),
        1,
        1,
        Box::new(BloomFilter::default()),
    )
    .unwrap();
    let val1 = TimeStampedValue::new(Value::Integer(1));
    let val2 = TimeStampedValue::new(Value::Integer(2));
    chest.set("foo", val1.clone()).unwrap();
    chest.set("bar", val2.clone()).unwrap();
    assert_eq!(chest.sstables.len(), 1);
    assert_eq!(chest.get("foo").unwrap(), Some(val1));
    assert_eq!(chest.get("bar").unwrap(), Some(val2));
}
#[test]
fn test_overwrite_on_merge() {
    let chest_dir = get_test_tempdir();
    let mut chest = Chest::new(
        chest_dir.to_str().unwrap(),
        1,
        1,
        Box::new(BloomFilter::default()),
    )
    .unwrap();
    let val = TimeStampedValue::new(Value::Integer(1));
    chest.set("foo", val).unwrap();
    let val = TimeStampedValue::new(Value::Integer(6));
    chest.set("foo", val.clone()).unwrap();
    assert_eq!(chest.sstables.len(), 1);
    assert_eq!(chest.get("foo").unwrap(), Some(val));
    let val = TimeStampedValue::new(Value::Integer(4));
    chest.set("foo", val.clone()).unwrap();
    assert_eq!(chest.get("foo").unwrap(), Some(val));
}
#[test]
fn merging_delete_old_sstables() {
    let chest_dir = get_test_tempdir();
    let mut chest = Chest::new(
        chest_dir.to_str().unwrap(),
        1,
        1,
        Box::new(BloomFilter::default()),
    )
    .unwrap();
    let val1 = TimeStampedValue::new(Value::Integer(1));
    let val2 = TimeStampedValue::new(Value::Integer(2));
    chest.set("foo", val1.clone()).unwrap();
    chest.set("bar", val2.clone()).unwrap();
    drop(chest);
    let chest = Chest::new(
        chest_dir.to_str().unwrap(),
        1,
        1,
        Box::new(BloomFilter::default()),
    )
    .unwrap();
    assert_eq!(chest.sstables.len(), 1);
    assert_eq!(chest.get("foo").unwrap(), Some(val1));
    assert_eq!(chest.get("bar").unwrap(), Some(val2));
}
