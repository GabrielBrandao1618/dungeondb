use std::fmt::Display;

#[derive(Debug, PartialEq)]
pub enum Literal {
    String(String),
    Integer(i64),
    Float(f64),
    Boolean(bool),
    Null,
}
impl Display for Literal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Literal::String(val) => write!(f, "{val}"),
            Literal::Integer(val) => write!(f, "{val}"),
            Literal::Float(val) => write!(f, "{val}"),
            Literal::Boolean(val) => write!(f, "{val}"),
            Literal::Null => write!(f, "null"),
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
    Get(GetExpr),
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
    Set(SetStmt),
    Delete(DeleteStmt),
}
