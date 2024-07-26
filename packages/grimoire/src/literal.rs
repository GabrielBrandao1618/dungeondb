use errors::{DungeonError, DungeonResult};
use pest::Parser;
use query::ast::{Literal, LocatedElement};

use crate::{
    parser::{GrimoireParser, Rule},
    utils::get_location_from_ast,
};

pub fn parse_literal(input: &str) -> DungeonResult<Literal> {
    let ast = GrimoireParser::parse(Rule::literal, input)
        .map_err(|_| DungeonError::new("Could not parse literal"))?
        .next()
        .ok_or(DungeonError::new("Could not parse literal"))?
        .into_inner()
        .next()
        .ok_or(DungeonError::new("Could not parse literal"))?;
    let location = get_location_from_ast(&ast);
    match ast.as_rule() {
        Rule::string => Ok(Literal::String(LocatedElement::new(
            ast.into_inner()
                .next()
                .ok_or(DungeonError::new("Could not parse string"))?
                .as_str()
                .to_owned(),
            location,
        ))),
        Rule::boolean => {
            let inner_bool = ast
                .into_inner()
                .next()
                .ok_or(DungeonError::new("Could not parse boolean"))?;
            let location = get_location_from_ast(&inner_bool);
            match inner_bool.as_rule() {
                Rule::bool_true => Ok(Literal::Boolean(LocatedElement::new(true, location))),
                Rule::bool_false => Ok(Literal::Boolean(LocatedElement::new(false, location))),
                _ => unreachable!(),
            }
        }
        Rule::integer => {
            let parsed: i64 = ast
                .as_str()
                .parse()
                .map_err(|_| DungeonError::new("Could not parse number"))?;
            Ok(Literal::Integer(LocatedElement::new(parsed, location)))
        }
        Rule::float => {
            let parsed: f64 = ast
                .as_str()
                .parse()
                .map_err(|_| DungeonError::new("Could not parse number"))?;
            Ok(Literal::Float(LocatedElement::new(parsed, location)))
        }
        _ => unreachable!(),
    }
}

#[cfg(test)]
mod tests {
    use query::ast::{Literal, LocatedElement};

    use super::parse_literal;

    #[test]
    fn test_parse_string() {
        let parsed = parse_literal(r#""apple""#).unwrap();
        assert_eq!(
            parsed,
            Literal::String(LocatedElement::new("apple".to_owned(), (0, 7).into()))
        );
    }
    #[test]
    fn test_parse_boolean() {
        assert_eq!(
            parse_literal("true").unwrap(),
            Literal::Boolean(LocatedElement::new(true, (0, 4).into()))
        );
        assert_eq!(
            parse_literal("false").unwrap(),
            Literal::Boolean(LocatedElement::new(false, (0, 5).into()))
        );
    }
    #[test]
    fn test_parse_integer() {
        assert_eq!(
            parse_literal("10").unwrap(),
            Literal::Integer(LocatedElement::new(10, (0, 2).into()))
        );
        assert_eq!(
            parse_literal("0").unwrap(),
            Literal::Integer(LocatedElement::new(0, (0, 1).into()))
        );
        assert_eq!(
            parse_literal("15").unwrap(),
            Literal::Integer(LocatedElement::new(15, (0, 2).into()))
        );
    }
    #[test]
    fn test_parse_float() {
        assert_eq!(
            parse_literal("10.0").unwrap(),
            Literal::Float(LocatedElement::new(10.0, (0, 4).into()))
        );
        assert_eq!(
            parse_literal("10.5").unwrap(),
            Literal::Float(LocatedElement::new(10.5, (0, 4).into()))
        );
        assert_eq!(
            parse_literal("0.0").unwrap(),
            Literal::Float(LocatedElement::new(0.0, (0, 3).into()))
        );
    }
}
