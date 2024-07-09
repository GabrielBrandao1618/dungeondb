mod value;

use chest::{value::TimeStampedValue, Chest};
use errors::DungeonResult;
use grimoire::parse;
use query::ast::{Literal, Statement};
use value::{value_from_query, value_to_query};

#[cfg(test)]
mod tests;

pub fn run_query(chest: &mut Chest, query: Statement) -> DungeonResult<Literal> {
    match query {
        Statement::Expr(expr) => match expr {
            query::ast::Expression::Literal(lit) => Ok(lit),
            query::ast::Expression::Get(expr) => {
                let found = chest
                    .get(&expr.key)?
                    .map_or(Literal::Null, |v| value_to_query(v.value));
                Ok(found)
            }
        },
        Statement::Set(stmt) => {
            let value = run_query(chest, Statement::Expr(stmt.value))?;
            chest.set(&stmt.key, TimeStampedValue::new(value_from_query(value)))?;
            Ok(Literal::Null)
        }
        Statement::Delete(stmt) => {
            chest.delete(&stmt.key)?;
            Ok(Literal::Null)
        }
    }
}

pub fn run_statement(chest: &mut Chest, input: &str) -> DungeonResult<Literal> {
    let parsed = parse(input)?;
    run_query(chest, parsed)
}
