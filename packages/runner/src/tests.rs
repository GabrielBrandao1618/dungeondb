use std::path::PathBuf;

use cuid::cuid2;

use chest::filter::bloom::BloomFilter;
use query::ast::Expression;

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
fn test_eval_literal() {
    let chest_dir = get_test_tempdir();
    let mut chest = Chest::new(
        chest_dir.to_str().unwrap(),
        1024,
        8,
        Box::new(BloomFilter::default()),
    )
    .unwrap();
    let result = run_query(
        &mut chest,
        Statement::Expr(Expression::Literal(Literal::Integer(1))),
    )
    .unwrap();
    assert_eq!(result, Literal::Integer(1));
}
