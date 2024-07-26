use errors::{DungeonError, DungeonResult};
use pest::Parser;
use query::ast::{Expression, GetExpr, LocatedElement};

use crate::{
    literal::parse_literal,
    parser::{GrimoireParser, Rule},
};

pub fn parse_expression(input: &str) -> DungeonResult<Expression> {
    let ast = GrimoireParser::parse(Rule::expression, input)
        .map_err(|_| DungeonError::new("Could not parse expression"))?
        .next()
        .ok_or(DungeonError::new("Could not parse expression"))?;
    let inner_value = ast
        .into_inner()
        .next()
        .ok_or(DungeonError::new("Could not get expression value"))?;
    match inner_value.as_rule() {
        Rule::literal => Ok(Expression::Literal(parse_literal(inner_value.as_str())?)),
        Rule::get_expr => {
            let ast_key = inner_value
                .into_inner()
                .next()
                .ok_or(DungeonError::new("Could not parse get expression"))?;
            Ok(Expression::Get(LocatedElement::from_value(GetExpr {
                key: ast_key.as_str().to_owned(),
            })))
        }
        _ => unreachable!(),
    }
}

#[cfg(test)]
mod tests {
    use query::ast::{Expression, GetExpr, Literal, LocatedElement};

    use super::parse_expression;

    #[test]
    fn test_parse_literal() {
        let parsed = parse_expression("1").unwrap();
        assert_eq!(
            parsed,
            Expression::Literal(Literal::Integer(LocatedElement::from_value(1)))
        );
    }
    #[test]
    fn test_parse_get_expr() {
        let parsed = parse_expression("get name").unwrap();
        assert_eq!(
            parsed,
            Expression::Get(LocatedElement::from_value(GetExpr {
                key: "name".to_owned()
            }))
        );
    }
}
