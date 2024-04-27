use cuid::cuid2;

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
    let mut chest = Chest::new(chest_dir.to_str().unwrap(), 1024, 8);
    chest.set("name", Value::String("John Doe".to_owned()));
    assert_eq!(
        chest.get("name"),
        Some(Value::String("John Doe".to_owned()))
    );
}

#[test]
fn test_flush() {
    let chest_dir = get_test_tempdir();
    let mut chest = Chest::new(chest_dir.to_str().unwrap(), 2, 8);
    chest.set("name", Value::String("John Doe".to_owned()));
    assert_eq!(chest.len(), 1);
    chest.set("age", Value::Integer(5));
    assert_eq!(chest.len(), 0);
}
#[test]
fn test_read_from_sstable() {
    let chest_dir = get_test_tempdir();
    let mut chest = Chest::new(chest_dir.to_str().unwrap(), 2, 8);
    chest.set("foo", Value::String("bar".to_string()));
    chest.set("foo2", Value::String("bar2".to_string()));
    assert_eq!(chest.len(), 0);
    assert_eq!(chest.get("foo"), Some(Value::String("bar".to_string())));
    assert_eq!(chest.get("foo2"), Some(Value::String("bar2".to_string())));
}

#[test]
fn test_reinitialize_chest() {
    let chest_dir = get_test_tempdir();
    let mut chest = Chest::new(chest_dir.to_str().unwrap(), 1024, 8);

    chest.set("foo", Value::String("bar".to_owned()));
    drop(chest);

    let chest2 = Chest::new(chest_dir.to_str().unwrap(), 1024, 8);
    assert_eq!(chest2.get("foo"), Some(Value::String("bar".to_owned())));
}
#[test]
fn test_merge_sstables() {
    let chest_dir = get_test_tempdir();
    let mut chest = Chest::new(chest_dir.to_str().unwrap(), 1, 8);
    chest.set("foo", Value::String("bar".to_string()));
    chest.set("foo1", Value::String("bar1".to_string()));

    let mut iter_chest_sstables = chest.sstables.iter().cloned();
    let table1 = iter_chest_sstables.next().unwrap();
    let table2 = iter_chest_sstables.next().unwrap();

    let merged = table1.0.merge(table2.0, generate_sstable_name());
    assert_eq!(merged.get("foo"), Some(Value::String("bar".to_owned())));
    assert_eq!(merged.get("foo1"), Some(Value::String("bar1".to_owned())));
}

#[test]
fn test_merge_sstables_on_limit() {
    let chest_dir = get_test_tempdir();
    let mut chest = Chest::new(chest_dir.to_str().unwrap(), 1, 1);
    chest.set("foo", Value::Integer(1));
    chest.set("bar", Value::Integer(2));
    assert_eq!(chest.sstables.len(), 1);
    assert_eq!(chest.get("foo"), Some(Value::Integer(1)));
    assert_eq!(chest.get("bar"), Some(Value::Integer(2)));
}
