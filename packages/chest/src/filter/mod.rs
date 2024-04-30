pub mod bloom;

pub trait Filter {
    fn contains(&self, item: &str) -> bool;
    fn insert(&mut self, item: &str);
}
