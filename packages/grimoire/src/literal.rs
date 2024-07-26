use errors::{DungeonError, DungeonResult};
use pest::Parser;
use query::ast::{Literal, LocatedElement};

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
        Rule::string => Ok(Literal::String(LocatedElement::from_value(
            ast.into_inner()
                .next()
                .ok_or(DungeonError::new("Could not parse string"))?
                .as_str()
                .to_owned(),
        ))),
        Rule::boolean => {
            let inner_bool = ast
                .into_inner()
                .next()
                .ok_or(DungeonError::new("Could not parse boolean"))?;
            match inner_bool.as_rule() {
                Rule::bool_true => Ok(Literal::Boolean(LocatedElement::from_value(true))),
                Rule::bool_false => Ok(Literal::Boolean(LocatedElement::from_value(false))),
                _ => unreachable!(),
            }
        }
        Rule::integer => {
            let parsed: i64 = ast
                .as_str()
                .parse()
                .map_err(|_| DungeonError::new("Could not parse number"))?;
            Ok(Literal::Integer(LocatedElement::from_value(parsed)))
        }
        Rule::float => {
            let parsed: f64 = ast
                .as_str()
                .parse()
                .map_err(|_| DungeonError::new("Could not parse number"))?;
            Ok(Literal::Float(LocatedElement::from_value(parsed)))
        }
        _ => unreachable!(),
    }
}

#[cfg(test)]
mod tests {
    use query::{
        ast::{Literal, LocatedElement},
        location::Location,
    };

    use super::parse_literal;

    #[test]
    fn test_parse_string() {
        let parsed = parse_literal(r#""apple""#).unwrap();
        assert_eq!(
            parsed,
            Literal::String(LocatedElement::new("apple".to_owned(), Location::default()))
        );
    }
    #[test]
    fn test_parse_boolean() {
        assert_eq!(
            parse_literal("true").unwrap(),
            Literal::Boolean(LocatedElement::new(true, Location::default()))
        );
        assert_eq!(
            parse_literal("false").unwrap(),
            Literal::Boolean(LocatedElement::new(false, Location::default()))
        );
    }
    #[test]
    fn test_parse_integer() {
        assert_eq!(
            parse_literal("10").unwrap(),
            Literal::Integer(LocatedElement::from_value(10))
        );
        assert_eq!(
            parse_literal("0").unwrap(),
            Literal::Integer(LocatedElement::from_value(0))
        );
        assert_eq!(
            parse_literal("15").unwrap(),
            Literal::Integer(LocatedElement::from_value(15))
        );
    }
    #[test]
    fn test_parse_float() {
        assert_eq!(
            parse_literal("10.0").unwrap(),
            Literal::Float(LocatedElement::from_value(10.0))
        );
        assert_eq!(
            parse_literal("10.5").unwrap(),
            Literal::Float(LocatedElement::from_value(10.5))
        );
        assert_eq!(
            parse_literal("0.0").unwrap(),
            Literal::Float(LocatedElement::from_value(0.0))
        );
    }
}
