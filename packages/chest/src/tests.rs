use super::*;

fn ensure_dir_exists(dir_path: &PathBuf) -> std::io::Result<()> {
    if !dir_path.exists() {
        std::fs::create_dir(dir_path)?;
    }
    Ok(())
}

fn get_test_tempdir(custom_name: &str) -> PathBuf {
    let path = std::env::temp_dir().join(custom_name);
    if path.exists() {
        std::fs::remove_dir_all(&path).unwrap();
    }
    ensure_dir_exists(&path).unwrap();
    path
}

#[test]
fn memtable_set_get() {
    let chest_dir = get_test_tempdir("dungeon-testing-1");
    let mut chest = Chest::new(chest_dir, 1024);
    chest.set("name", Value::String("John Doe".to_owned()));
    assert_eq!(
        chest.get("name"),
        Some(Value::String("John Doe".to_owned()))
    );
}

#[test]
fn test_flush() {
    let chest_dir = get_test_tempdir("dungeon-testing-2");
    let mut chest = Chest::new(chest_dir, 2);
    chest.set("name", Value::String("John Doe".to_owned()));
    assert_eq!(chest.len(), 1);
    chest.set("age", Value::Integer(5));
    assert_eq!(chest.len(), 0);
}
#[test]
fn test_read_from_sstable() {
    let chest_dir = get_test_tempdir("dungeon-testing-3");
    let mut chest = Chest::new(chest_dir, 2);
    chest.set("foo", Value::String("bar".to_string()));
    chest.set("foo2", Value::String("bar2".to_string()));
    assert_eq!(chest.len(), 0);
    assert_eq!(chest.get("foo"), Some(Value::String("bar".to_string())));
    assert_eq!(chest.get("foo2"), Some(Value::String("bar2".to_string())));
}
