use chest::value::Value;
use query::ast::Literal;

pub fn value_from_query(lit: Literal) -> Value {
    match lit {
        Literal::String(v) => Value::String(v.val),
        Literal::Integer(v) => Value::Integer(v.val),
        Literal::Float(v) => Value::Float(v.val),
        Literal::Boolean(v) => Value::Boolean(v.val),
        Literal::Null(_) => Value::Null,
    }
}
