use errors::DungeonResult;
use query::ast::Statement;
use statement::parse_statement;

mod expression;
mod literal;
mod parser;
mod statement;
mod utils;

pub fn parse(input: &str) -> DungeonResult<Statement> {
    parse_statement(input)
}
