use errors::{DungeonError, DungeonResult};
use pest::Parser;
use query::ast::Literal;

use crate::parser::{GrimoireParser, Rule};

pub fn parse_literal(input: &str) -> DungeonResult<Literal> {
    let ast = GrimoireParser::parse(Rule::literal, input)
        .map_err(|_| DungeonError::new("Could not parse literal"))?
        .next()
        .ok_or(DungeonError::new("Could not parse literal"))?
        .into_inner()
        .next()
        .ok_or(DungeonError::new("Could not parse literal"))?;
    match ast.as_rule() {
        Rule::string => Ok(Literal::String(
            ast.into_inner()
                .next()
                .ok_or(DungeonError::new("Could not parse string"))?
                .as_str()
                .to_owned(),
        )),
        Rule::boolean => {
            let inner_bool = ast
                .into_inner()
                .next()
                .ok_or(DungeonError::new("Could not parse boolean"))?;
            match inner_bool.as_rule() {
                Rule::bool_true => Ok(Literal::Boolean(true)),
                Rule::bool_false => Ok(Literal::Boolean(false)),
                _ => unreachable!(),
            }
        }
        Rule::integer => {
            let parsed: i64 = ast
                .as_str()
                .parse()
                .map_err(|_| DungeonError::new("Could not parse number"))?;
            Ok(Literal::Integer(parsed))
        }
        Rule::float => {
            let parsed: f64 = ast
                .as_str()
                .parse()
                .map_err(|_| DungeonError::new("Could not parse number"))?;
            Ok(Literal::Float(parsed))
        }
        _ => unreachable!(),
    }
}

#[cfg(test)]
mod tests {
    use query::ast::Literal;

    use super::parse_literal;

    #[test]
    fn test_parse_string() {
        let parsed = parse_literal(r#""apple""#).unwrap();
        assert_eq!(parsed, Literal::String("apple".to_owned()));
    }
    #[test]
    fn test_parse_boolean() {
        assert_eq!(parse_literal("true").unwrap(), Literal::Boolean(true));
        assert_eq!(parse_literal("false").unwrap(), Literal::Boolean(false));
    }
    #[test]
    fn test_parse_integer() {
        assert_eq!(parse_literal("10").unwrap(), Literal::Integer(10));
        assert_eq!(parse_literal("0").unwrap(), Literal::Integer(0));
        assert_eq!(parse_literal("15").unwrap(), Literal::Integer(15));
    }
    #[test]
    fn test_parse_float() {
        assert_eq!(parse_literal("10.0").unwrap(), Literal::Float(10.0));
        assert_eq!(parse_literal("10.5").unwrap(), Literal::Float(10.5));
        assert_eq!(parse_literal("0.0").unwrap(), Literal::Float(0.0));
    }
}
