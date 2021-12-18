use datafusion::logical_plan::{Column, Literal, Operator, lit as df_lit, lit_timestamp_nano as df_lit_timestamp_nano, Expr as DFExpr, TimestampLiteral};
use datafusion::logical_plan::Expr;
use datafusion::scalar::ScalarValue;

pub enum Expr {
    Column(Column),
    BinaryExpr {
        /// Left-hand side of the expression
        left: Box<Expr>,
        /// The comparison operator
        op: Operator,
        /// Right-hand side of the expression
        right: Box<Expr>,
    },
    IsNotNull(Box<Expr>),
    /// Whether an expression is Null. This expression is never null.
    IsNull(Box<Expr>),
    Literal(ScalarValue),
}

/// return a new expression l <op> r
pub fn binary_expr(l: Expr, op: Operator, r: Expr) -> Expr {
    Expr::BinaryExpr {
        left: Box::new(l),
        op,
        right: Box::new(r),
    }
}

/// return a new expression with a logical AND
pub fn and(left: Expr, right: Expr) -> Expr {
    Expr::BinaryExpr {
        left: Box::new(left),
        op: Operator::And,
        right: Box::new(right),
    }
}

/// return a new expression with a logical OR
pub fn or(left: Expr, right: Expr) -> Expr {
    Expr::BinaryExpr {
        left: Box::new(left),
        op: Operator::Or,
        right: Box::new(right),
    }
}

/// Create a column expression based on a qualified or unqualified column name
pub fn col(ident: &str) -> Expr {
    Expr::Column(ident.into())
}

/// Create a literal expression
pub fn lit<T: Literal>(n: T) -> Expr {
    if let DFExpr::Literal(v) = df_lit(n) {
        Expr::Literal(v)
    } else {
        unreachable!()
    }
}

/// Create a literal timestamp expression
pub fn lit_timestamp_nano<T: TimestampLiteral>(n: T) -> Expr {
    if let DFExpr::Literal(v) = df_lit_timestamp_nano(n) {
        Expr::Literal(v)
    } else {
        unreachable!()
    }
}