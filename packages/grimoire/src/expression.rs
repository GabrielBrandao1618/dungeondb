use errors::{DungeonError, DungeonResult};
use pest::Parser;
use query::ast::Expression;

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
    println!("{:#?}", inner_value);
    match inner_value.as_rule() {
        Rule::literal => Ok(Expression::Literal(parse_literal(inner_value.as_str())?)),
        Rule::get_expr => todo!(),
        _ => unreachable!(),
    }
}

#[cfg(test)]
mod tests {
    use query::ast::{Expression, Literal};

    use super::parse_expression;

    #[test]
    fn test_parse_literal() {
        let parsed = parse_expression("1").unwrap();
        assert_eq!(parsed, Expression::Literal(Literal::Integer(1)));
    }
}
