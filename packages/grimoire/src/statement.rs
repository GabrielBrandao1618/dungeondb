use errors::{DungeonError, DungeonResult};
use pest::Parser;
use query::ast::{DeleteStmt, LocatedElement, SetStmt, Statement};

use crate::{
    expression::parse_expression,
    parser::{GrimoireParser, Rule},
    utils::get_location_from_ast,
};

pub fn parse_statement(input: &str) -> DungeonResult<Statement> {
    let ast = GrimoireParser::parse(Rule::statement, input)
        .map_err(|_| DungeonError::new("Could not parse statement"))?
        .next()
        .ok_or(DungeonError::new("Could not parse statement"))?;
    let inner_ast = ast
        .into_inner()
        .next()
        .ok_or(DungeonError::new("Could not parse statetent"))?;
    let location = get_location_from_ast(&inner_ast);
    match inner_ast.as_rule() {
        Rule::delete_stmt => {
            let ast_key = inner_ast
                .into_inner()
                .next()
                .ok_or(DungeonError::new("Could not parse delete statement"))?;
            Ok(Statement::Delete(LocatedElement::new(
                DeleteStmt {
                    key: ast_key.as_str().to_owned(),
                },
                location,
            )))
        }
        Rule::set_stmt => {
            let mut inner_stmt = inner_ast.into_inner();
            let ast_key = inner_stmt
                .next()
                .ok_or(DungeonError::new("Could not get key"))?;
            let ast_val = inner_stmt
                .next()
                .ok_or(DungeonError::new("Could not get value"))?;

            let parsed_key = ast_key.as_str().to_owned();
            let parsed_value = parse_expression(ast_val.as_str())?;

            Ok(Statement::Set(LocatedElement::new(
                SetStmt {
                    key: parsed_key,
                    value: parsed_value,
                },
                location,
            )))
        }
        Rule::expression => {
            let parsed = parse_expression(inner_ast.as_str())?;
            Ok(Statement::Expr(parsed))
        }
        _ => unreachable!(),
    }
}

#[cfg(test)]
mod tests {
    use query::ast::{DeleteStmt, Expression, Literal, LocatedElement, SetStmt, Statement};

    use super::parse_statement;

    #[test]
    fn test_parse_set_statement() {
        let parsed = parse_statement("set count 1").unwrap();
        assert_eq!(
            parsed,
            Statement::Set(LocatedElement::new(
                SetStmt {
                    key: "count".to_owned(),
                    value: Expression::Literal(Literal::Integer(LocatedElement::new(
                        1,
                        (0, 1).into()
                    )))
                },
                (0, 11).into()
            ))
        );
    }
    #[test]
    fn test_parse_delete_statement() {
        let parsed = parse_statement("delete count").unwrap();
        assert_eq!(
            parsed,
            Statement::Delete(LocatedElement::new(
                DeleteStmt {
                    key: "count".to_owned()
                },
                (0, 12).into()
            ))
        );
    }
}
