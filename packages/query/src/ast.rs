pub enum Literal {
    String(String),
    Integer(i64),
    Float(f64),
    Boolean(bool),
}
pub struct GetExpr {
    pub key: String,
}
pub enum Expression {
    Literal(Literal),
    Get(GetExpr),
}

pub struct SetStmt {
    pub key: String,
    pub value: Expression,
}
pub struct DeleteStmt {
    pub key: String,
}

pub enum Statement {
    Expr(Expression),
    Set(SetStmt),
    Delete(DeleteStmt),
}
