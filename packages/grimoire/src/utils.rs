use pest::iterators::Pair;
use query::location::Location;

use crate::parser::Rule;

pub fn get_location_from_ast(ast: &Pair<Rule>) -> Location {
    let span = ast.as_span();
    let start = span.start();
    let end = span.end();
    (start, end).into()
}
