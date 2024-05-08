use std::os::unix::fs::MetadataExt;

use cuid::cuid2;
use rmp_serde::to_vec;

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
    chest
        .set(
            "name",
            TimeStampedValue::new(Value::String("John Doe".to_owned())),
        )
        .unwrap();
    assert_eq!(
        chest.get("name").unwrap().unwrap().value,
        Value::String("John Doe".to_owned())
    );
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
    chest
        .set(
            "foo",
            TimeStampedValue::new(Value::String("bar".to_string())),
        )
        .unwrap();
    chest
        .set(
            "foo2",
            TimeStampedValue::new(Value::String("bar2".to_string())),
        )
        .unwrap();
    assert_eq!(chest.len(), 0);
    assert_eq!(
        chest.get("foo").unwrap().unwrap().value,
        Value::String("bar".to_owned())
    );
    assert_eq!(
        chest.get("foo2").unwrap().unwrap().value,
        Value::String("bar2".to_owned())
    );
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

    chest
        .set(
            "foo",
            TimeStampedValue::new(Value::String("bar".to_owned())),
        )
        .unwrap();
    drop(chest);

    let chest2 = Chest::new(
        chest_dir.to_str().unwrap(),
        1024,
        8,
        Box::new(BloomFilter::default()),
    )
    .unwrap();
    assert_eq!(
        chest2.get("foo").unwrap().unwrap().value,
        Value::String("bar".to_owned())
    );
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
    chest
        .set(
            "foo",
            TimeStampedValue::new(Value::String("bar".to_string())),
        )
        .unwrap();
    chest
        .set(
            "foo",
            TimeStampedValue::new(Value::String("barz".to_string())),
        )
        .unwrap();

    let mut iter_chest_sstables = chest.sstables.iter().cloned();
    let mut table1 = iter_chest_sstables.next().unwrap();
    let mut table2 = iter_chest_sstables.next().unwrap();

    let merged = table1
        .0
        .merge(&mut table2.0, generate_sstable_name())
        .unwrap();
    assert_eq!(
        merged.get("foo").unwrap().unwrap().value,
        Value::String("barz".to_owned())
    );
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
    chest
        .set("foo", TimeStampedValue::new(Value::Integer(1)))
        .unwrap();
    chest
        .set("bar", TimeStampedValue::new(Value::Integer(2)))
        .unwrap();
    assert_eq!(chest.sstables.len(), 1);
    assert_eq!(chest.get("foo").unwrap().unwrap().value, Value::Integer(1));
    assert_eq!(chest.get("bar").unwrap().unwrap().value, Value::Integer(2));
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
    chest
        .set("foo", TimeStampedValue::new(Value::Integer(1)))
        .unwrap();
    chest
        .set("foo", TimeStampedValue::new(Value::Integer(6)))
        .unwrap();
    assert_eq!(chest.sstables.len(), 1);
    assert_eq!(chest.get("foo").unwrap().unwrap().value, Value::Integer(6));
    chest
        .set("foo", TimeStampedValue::new(Value::Integer(4)))
        .unwrap();
    assert_eq!(chest.get("foo").unwrap().unwrap().value, Value::Integer(4));
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
    chest
        .set("foo", TimeStampedValue::new(Value::Integer(1)))
        .unwrap();
    chest
        .set("bar", TimeStampedValue::new(Value::Integer(2)))
        .unwrap();
    drop(chest);
    let chest = Chest::new(
        chest_dir.to_str().unwrap(),
        1,
        1,
        Box::new(BloomFilter::default()),
    )
    .unwrap();
    assert_eq!(chest.sstables.len(), 1);
    assert_eq!(chest.get("foo").unwrap().unwrap().value, Value::Integer(1));
    assert_eq!(chest.get("bar").unwrap().unwrap().value, Value::Integer(2));
}

#[test]
fn test_delete_value() {
    let chest_dir = get_test_tempdir();
    let mut chest = Chest::new(
        chest_dir.to_str().unwrap(),
        4,
        1,
        Box::new(BloomFilter::default()),
    )
    .unwrap();
    chest
        .set("count", TimeStampedValue::new(Value::Integer(0)))
        .unwrap();
    chest
        .set("count", TimeStampedValue::new(Value::Integer(1)))
        .unwrap();
    assert_eq!(
        chest.get("count").unwrap().unwrap().value,
        Value::Integer(1)
    );
    chest.delete("count").unwrap();
    assert_eq!(chest.get("count").unwrap(), None);
}

#[test]
fn test_delete_from_sstable() {
    let chest_dir = get_test_tempdir();
    let mut chest = Chest::new(
        chest_dir.to_str().unwrap(),
        1,
        1,
        Box::new(BloomFilter::default()),
    )
    .unwrap();
    chest
        .set("count", TimeStampedValue::new(Value::Integer(0)))
        .unwrap();
    assert_eq!(
        chest.get("count").unwrap().unwrap().value,
        Value::Integer(0)
    );
    chest.delete("count").unwrap();
    assert_eq!(chest.get("count").unwrap(), None);
}

#[test]
fn test_clean_sstable() {
    let chest_dir = get_test_tempdir();
    let mut chest = Chest::new(
        chest_dir.to_str().unwrap(),
        1,
        1,
        Box::new(BloomFilter::default()),
    )
    .unwrap();
    chest
        .set("foo", TimeStampedValue::new(Value::Integer(0)))
        .unwrap();
    chest
        .set("foo", TimeStampedValue::new(Value::Integer(1)))
        .unwrap();
    assert_eq!(chest.get("foo").unwrap().unwrap().value, Value::Integer(1));
    chest
        .set("bar", TimeStampedValue::new(Value::Float(1.5)))
        .unwrap();
    chest
        .set("bar", TimeStampedValue::new(Value::Float(3.5)))
        .unwrap();
    assert_eq!(chest.sstables.len(), 1);
    let expected_size = to_vec(&TimeStampedValue::new(Value::Integer(1)))
        .unwrap()
        .len()
        + to_vec(&TimeStampedValue::new(Value::Float(3.5)))
            .unwrap()
            .len();
    let table = &chest.sstables.iter().next().unwrap().0;
    let data_file_path = table.get_data_file_path();
    let metadata = std::fs::metadata(data_file_path).unwrap();
    let file_size = metadata.size();
    assert_eq!(expected_size as u64, file_size);
    assert!(false);
}
