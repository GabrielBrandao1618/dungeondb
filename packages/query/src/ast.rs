use std::fmt::Display;

use crate::location::Location;

#[derive(Debug, PartialEq)]
pub struct LocatedElement<T> {
    pub val: T,
    pub location: Location,
}
impl<T> LocatedElement<T> {
    pub fn new(val: T, location: Location) -> Self {
        Self { val, location }
    }
    pub fn from_value(val: T) -> Self {
        Self {
            val,
            location: Location::default(),
        }
    }
}

#[derive(Debug, PartialEq)]
pub enum Literal {
    String(LocatedElement<String>),
    Integer(LocatedElement<i64>),
    Float(LocatedElement<f64>),
    Boolean(LocatedElement<bool>),
    Null(LocatedElement<()>),
}
impl Display for Literal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Literal::String(val) => write!(f, "{}", val.val),
            Literal::Integer(val) => write!(f, "{}", val.val),
            Literal::Float(val) => write!(f, "{}", val.val),
            Literal::Boolean(val) => write!(f, "{}", val.val),
            Literal::Null(_) => write!(f, "null"),
        }
    }
}
#[derive(Debug, PartialEq)]
pub struct GetExpr {
    pub key: String,
}
#[derive(Debug, PartialEq)]
pub enum Expression {
    Literal(Literal),
    Get(LocatedElement<GetExpr>),
}

#[derive(Debug, PartialEq)]
pub struct SetStmt {
    pub key: String,
    pub value: Expression,
}
#[derive(Debug, PartialEq)]
pub struct DeleteStmt {
    pub key: String,
}

#[derive(Debug, PartialEq)]
pub enum Statement {
    Expr(Expression),
    Set(LocatedElement<SetStmt>),
    Delete(LocatedElement<DeleteStmt>),
}
