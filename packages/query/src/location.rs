#[derive(Debug, PartialEq)]
pub struct Location {
    start: usize,
    end: usize,
}

impl From<(usize, usize)> for Location {
    fn from(value: (usize, usize)) -> Self {
        let (start, end) = value;
        Location { start, end }
    }
}

impl Default for Location {
    fn default() -> Self {
        (0, 0).into()
    }
}
