use cuid::cuid2;

use crate::filter::bloom::BloomFilter;

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
        .set("name", Value::String("John Doe".to_owned()))
        .unwrap();
    assert_eq!(
        chest.get("name").unwrap(),
        Some(Value::String("John Doe".to_owned()))
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
        .set("name", Value::String("John Doe".to_owned()))
        .unwrap();
    assert_eq!(chest.len(), 1);
    chest.set("age", Value::Integer(5)).unwrap();
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
    chest.set("foo", Value::String("bar".to_string())).unwrap();
    chest
        .set("foo2", Value::String("bar2".to_string()))
        .unwrap();
    assert_eq!(chest.len(), 0);
    assert_eq!(
        chest.get("foo").unwrap(),
        Some(Value::String("bar".to_string()))
    );
    assert_eq!(
        chest.get("foo2").unwrap(),
        Some(Value::String("bar2".to_string()))
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

    chest.set("foo", Value::String("bar".to_owned())).unwrap();
    drop(chest);

    let chest2 = Chest::new(
        chest_dir.to_str().unwrap(),
        1024,
        8,
        Box::new(BloomFilter::default()),
    )
    .unwrap();
    assert_eq!(
        chest2.get("foo").unwrap(),
        Some(Value::String("bar".to_owned()))
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
    chest.set("foo", Value::String("bar".to_string())).unwrap();
    chest.set("foo", Value::String("barz".to_string())).unwrap();

    let mut iter_chest_sstables = chest.sstables.iter().cloned();
    let mut table1 = iter_chest_sstables.next().unwrap();
    let mut table2 = iter_chest_sstables.next().unwrap();

    let merged = table1
        .0
        .merge(&mut table2.0, generate_sstable_name())
        .unwrap();
    assert_eq!(
        merged.get("foo").unwrap(),
        Some(Value::String("barz".to_owned()))
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
    chest.set("foo", Value::Integer(1)).unwrap();
    chest.set("bar", Value::Integer(2)).unwrap();
    assert_eq!(chest.sstables.len(), 1);
    assert_eq!(chest.get("foo").unwrap(), Some(Value::Integer(1)));
    assert_eq!(chest.get("bar").unwrap(), Some(Value::Integer(2)));
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
    chest.set("foo", Value::Integer(1)).unwrap();
    chest.set("foo", Value::Integer(2)).unwrap();
    assert_eq!(chest.sstables.len(), 1);
    assert_eq!(chest.get("foo").unwrap(), Some(Value::Integer(2)));
    chest.set("foo", Value::Integer(6)).unwrap();
    assert_eq!(chest.get("foo").unwrap(), Some(Value::Integer(6)));
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
    chest.set("foo", Value::Integer(1)).unwrap();
    chest.set("bar", Value::Integer(2)).unwrap();
    drop(chest);
    let chest = Chest::new(
        chest_dir.to_str().unwrap(),
        1,
        1,
        Box::new(BloomFilter::default()),
    )
    .unwrap();
    assert_eq!(chest.sstables.len(), 1);
    assert_eq!(chest.get("foo").unwrap(), Some(Value::Integer(1)));
    assert_eq!(chest.get("bar").unwrap(), Some(Value::Integer(2)));
}
