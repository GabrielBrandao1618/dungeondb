use chest::value::Value;
use query::ast::Literal;

pub fn value_to_query(src: Value) -> Literal {
    match src {
        Value::Integer(v) => Literal::Integer(v),
        Value::Float(v) => Literal::Float(v),
        Value::String(v) => Literal::String(v),
        Value::Boolean(v) => Literal::Boolean(v),
        Value::Invalid => unreachable!(),
    }
}
pub fn value_from_query(lit: Literal) -> Value {
    match lit {
        Literal::String(v) => Value::String(v),
        Literal::Integer(v) => Value::Integer(v),
        Literal::Float(v) => Value::Float(v),
        Literal::Boolean(v) => Value::Boolean(v),
        Literal::Null => Value::Invalid,
    }
}
