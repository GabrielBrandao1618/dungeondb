mod value;

use chest::{
    value::{TimeStampedValue, Value},
    Chest,
};
use errors::DungeonResult;
use grimoire::parse;
use query::ast::Statement;
use value::value_from_query;

#[cfg(test)]
mod tests;

pub fn run_query(chest: &mut Chest, query: Statement) -> DungeonResult<Value> {
    match query {
        Statement::Expr(expr) => match expr {
            query::ast::Expression::Literal(lit) => Ok(value_from_query(lit)),
            query::ast::Expression::Get(expr) => {
                let found = chest.get(&expr.key)?.map_or(Value::Invalid, |v| v.value);
                Ok(found)
            }
        },
        Statement::Set(stmt) => {
            let value = run_query(chest, Statement::Expr(stmt.value))?;
            chest.set(&stmt.key, TimeStampedValue::new(value))?;
            Ok(Value::Invalid)
        }
        Statement::Delete(stmt) => {
            chest.delete(&stmt.key)?;
            Ok(Value::Invalid)
        }
    }
}

pub fn run_statement(chest: &mut Chest, input: &str) -> DungeonResult<Value> {
    let parsed = parse(input)?;
    run_query(chest, parsed)
}
