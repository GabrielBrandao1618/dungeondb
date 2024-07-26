use chest::value::Value;
use query::ast::Literal;

pub fn value_from_query(lit: Literal) -> Value {
    match lit {
        Literal::String(v) => Value::String(v),
        Literal::Integer(v) => Value::Integer(v),
        Literal::Float(v) => Value::Float(v),
        Literal::Boolean(v) => Value::Boolean(v),
        Literal::Null => Value::Null,
    }
}
