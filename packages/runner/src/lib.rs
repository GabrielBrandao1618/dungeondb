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
                let found = chest.get(&expr.val.key)?.map_or(Value::Null, |v| v.value);
                Ok(found)
            }
        },
        Statement::Set(stmt) => {
            let value = run_query(chest, Statement::Expr(stmt.val.value))?;
            chest.set(&stmt.val.key, TimeStampedValue::new(value))?;
            Ok(Value::Null)
        }
        Statement::Delete(stmt) => {
            chest.delete(&stmt.val.key)?;
            Ok(Value::Null)
        }
    }
}

pub fn run_statement(chest: &mut Chest, input: &str) -> DungeonResult<Value> {
    let parsed = parse(input)?;
    run_query(chest, parsed)
}
