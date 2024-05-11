use chest::Chest;
use errors::DungeonResult;
use query::ast::{Literal, Statement};

#[cfg(test)]
mod tests;

pub fn run_query(chest: &mut Chest, query: Statement) -> DungeonResult<Literal> {
    match query {
        Statement::Expr(expr) => match expr {
            query::ast::Expression::Literal(lit) => Ok(lit),
            query::ast::Expression::Get(expr) => {
                let found = chest
                    .get(&expr.key)?
                    .map_or(Literal::Null, |v| v.to_query());
                Ok(found)
            }
        },
        Statement::Set(_) => todo!(),
        Statement::Delete(_) => todo!(),
    }
}
