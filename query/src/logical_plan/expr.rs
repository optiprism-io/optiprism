use crate::error::{Error, Result};
use crate::physical_plan::expressions::aggregate::{state_types, PartitionedAggregate};
use datafusion::arrow::datatypes::DataType;

use datafusion::logical_plan::{
    lit as df_lit, Column, DFField, DFSchema, Expr as DFExpr, Literal, Operator,
};
use datafusion::physical_plan::aggregates;
use datafusion::physical_plan::aggregates::{
    return_type, AccumulatorFunctionImplementation, StateTypeFunction,
};
use datafusion::physical_plan::expressions::binary_operator_data_type;
use datafusion::physical_plan::functions::{ReturnTypeFunction, Signature, Volatility};
use datafusion::physical_plan::udaf::AggregateUDF;

use datafusion::scalar::ScalarValue;
use std::fmt;

use std::sync::Arc;

#[derive(Clone)]
pub enum Expr {
    Alias(Box<Expr>, String),
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
    /// Represents the call of an aggregate built-in function with arguments.
    AggregateFunction {
        /// Name of the function
        fun: aggregates::AggregateFunction,
        /// List of expressions to feed to the functions as arguments
        args: Vec<Expr>,
        /// Whether this is a DISTINCT aggregation or not
        distinct: bool,
    },
    // Represents the call of an partitioned aggregate built-in function with arguments.
    // fun is an inner aggregator with values partitioned by key
    // outer_fun is a result aggregator
    AggregatePartitionedFunction {
        partition_by: Box<Expr>,
        /// Name of the aggregate function per a partition
        fun: aggregates::AggregateFunction,
        /// Name of the final (outer) function, the result of function
        outer_fun: aggregates::AggregateFunction,
        /// List of expressions to feed to the functions as arguments
        args: Vec<Expr>,
        /// Whether this is a DISTINCT aggregation or not
        distinct: bool,
    },
    Wildcard,
}

fn create_partitioned_function(
    partition_by: &Expr,
    fun: &str,
    outer_fun: &str,
    distinct: bool,
    args: &[Expr],
) -> Result<String> {
    let args: Vec<String> = args.iter().map(|arg| format!("{:?}", arg)).collect();
    let distinct_str = match distinct {
        true => "DISTINCT ",
        false => "",
    };
    Ok(format!(
        "{}({}({}{}) PARTITION BY {:?})",
        outer_fun,
        fun,
        distinct_str,
        args.join(", "),
        partition_by
    ))
}

fn create_function_name(
    fun: &str,
    distinct: bool,
    args: &[Expr],
    input_schema: &DFSchema,
) -> Result<String> {
    let names: Vec<String> = args
        .iter()
        .map(|e| create_name(e, input_schema))
        .collect::<Result<_>>()?;
    let distinct_str = match distinct {
        true => "DISTINCT ",
        false => "",
    };
    Ok(format!("{}({}{})", fun, distinct_str, names.join(",")))
}

/// Returns a readable name of an expression based on the input schema.
/// This function recursively transverses the expression for names such as "CAST(a > 2)".
fn create_name(e: &Expr, input_schema: &DFSchema) -> Result<String> {
    match e {
        Expr::Alias(_, name) => Ok(name.clone()),
        Expr::Column(c) => Ok(c.flat_name()),
        Expr::Literal(value) => Ok(format!("{:?}", value)),
        Expr::BinaryExpr { left, op, right } => {
            let left = create_name(left, input_schema)?;
            let right = create_name(right, input_schema)?;
            Ok(format!("{} {:?} {}", left, op, right))
        }
        Expr::IsNull(expr) => {
            let expr = create_name(expr, input_schema)?;
            Ok(format!("{} IS NULL", expr))
        }
        Expr::IsNotNull(expr) => {
            let expr = create_name(expr, input_schema)?;
            Ok(format!("{} IS NOT NULL", expr))
        }

        Expr::AggregateFunction {
            fun,
            distinct,
            args,
            ..
        } => create_function_name(&fun.to_string(), *distinct, args, input_schema),
        Expr::AggregatePartitionedFunction {
            partition_by,
            fun,
            outer_fun,
            args,
            distinct,
        } => create_partitioned_function(
            partition_by,
            &fun.to_string(),
            &outer_fun.to_string(),
            *distinct,
            args,
        ),
        Expr::Wildcard => unimplemented!(),
    }
}

impl Expr {
    pub fn from_df_expr(expr: &DFExpr) -> Expr {
        match expr {
            DFExpr::Column(column) => Expr::Column(column.clone()),
            DFExpr::Literal(v) => Expr::Literal(v.clone()),
            DFExpr::BinaryExpr { left, op, right } => Expr::BinaryExpr {
                left: Box::new(Expr::from_df_expr(left)),
                op: *op,
                right: Box::new(Expr::from_df_expr(right)),
            },
            DFExpr::IsNotNull(expr) => Expr::IsNotNull(Box::new(Expr::from_df_expr(expr))),
            DFExpr::IsNull(expr) => Expr::IsNull(Box::new(Expr::from_df_expr(expr))),
            DFExpr::AggregateFunction {
                fun,
                args,
                distinct,
            } => Expr::AggregateFunction {
                fun: fun.clone(),
                args: args.iter().map(Expr::from_df_expr).collect(),
                distinct: *distinct,
            },
            DFExpr::Wildcard => Expr::Wildcard,
            DFExpr::Alias(e, v) => Expr::Alias(Box::new(Expr::from_df_expr(e)), v.clone()),
            _ => unimplemented!(),
        }
    }

    pub fn to_df_expr(&self, input_schema: &DFSchema) -> Result<DFExpr> {
        match self {
            Expr::Column(column) => Ok(DFExpr::Column(column.clone())),
            Expr::Literal(v) => Ok(DFExpr::Literal(v.clone())),
            Expr::BinaryExpr { left, op, right } => Ok(DFExpr::BinaryExpr {
                left: Box::new(left.to_df_expr(input_schema)?),
                op: *op,
                right: Box::new(right.to_df_expr(input_schema)?),
            }),
            Expr::IsNotNull(expr) => {
                Ok(DFExpr::IsNotNull(Box::new(expr.to_df_expr(input_schema)?)))
            }
            Expr::IsNull(expr) => Ok(DFExpr::IsNull(Box::new(expr.to_df_expr(input_schema)?))),
            Expr::AggregateFunction {
                fun,
                args,
                distinct,
            } => Ok(DFExpr::AggregateFunction {
                fun: fun.clone(),
                args: args
                    .iter()
                    .map(|e| e.to_df_expr(input_schema))
                    .collect::<Result<_>>()?,
                distinct: *distinct,
            }),
            Expr::AggregatePartitionedFunction {
                partition_by,
                fun,
                outer_fun,
                args,
                distinct: _,
            } => {
                // determine arguments data types
                let data_types = args
                    .iter()
                    .map(|x| x.get_type(input_schema))
                    .collect::<Result<Vec<DataType>>>()?;
                // determine return type
                let rtype = return_type(outer_fun, &data_types)?;

                // determine state types
                let state_types: Vec<DataType> = state_types(rtype.clone(), outer_fun)?;

                // make partitioned aggregate factory
                let pagg = PartitionedAggregate::try_new(
                    partition_by.get_type(input_schema)?,
                    rtype.clone(),
                    fun.clone(),
                    outer_fun.clone(),
                )?;

                // factory closure
                let acc_fn: AccumulatorFunctionImplementation = Arc::new(move || {
                    pagg.create_accumulator()
                        .map_err(Error::into_datafusion_plan_error)
                });
                let return_type_fn: ReturnTypeFunction =
                    Arc::new(move |_| Ok(Arc::new(rtype.clone())));
                let state_type_fn: StateTypeFunction =
                    Arc::new(move |_| Ok(Arc::new(state_types.clone())));

                let udf = AggregateUDF::new(
                    "AggregatePartitioned", // TODO make name based on aggregates
                    &Signature::any(2, Volatility::Immutable),
                    &return_type_fn,
                    &acc_fn,
                    &state_type_fn,
                );

                // join partition and function arguments into one vector
                let mut df_args = vec![partition_by.to_df_expr(input_schema)?];
                let mut df_args_more = args
                    .iter()
                    .map(|e| e.to_df_expr(input_schema))
                    .collect::<Result<Vec<DFExpr>>>()?;
                df_args.append(&mut df_args_more);

                Ok(DFExpr::AggregateUDF {
                    fun: Arc::new(udf),
                    args: df_args,
                })
            }
            Expr::Wildcard => Ok(DFExpr::Wildcard),
            Expr::Alias(e, n) => Ok(DFExpr::Alias(
                Box::new(e.to_df_expr(input_schema)?),
                n.clone(),
            )),
        }
    }

    /// Returns the [arrow::datatypes::DataType] of the expression based on [arrow::datatypes::Schema].
    ///
    /// # Errors
    ///
    /// This function errors when it is not possible to compute its [arrow::datatypes::DataType].
    /// This happens when e.g. the expression refers to a column that does not exist in the schema, or when
    /// the expression is incorrectly typed (e.g. `[utf8] + [bool]`).
    pub fn get_type(&self, schema: &DFSchema) -> Result<DataType> {
        match self {
            Expr::Alias(expr, _) => expr.get_type(schema),
            Expr::Column(c) => Ok(schema.field_from_column(c)?.data_type().clone()),
            Expr::Literal(l) => Ok(l.get_datatype()),
            Expr::AggregateFunction { fun, args, .. } => {
                let data_types = args
                    .iter()
                    .map(|e| e.get_type(schema))
                    .collect::<Result<Vec<_>>>()?;
                Ok(aggregates::return_type(fun, &data_types)?)
            }
            Expr::AggregatePartitionedFunction {
                outer_fun, args, ..
            } => {
                let data_types = args
                    .iter()
                    .map(|e| e.get_type(schema))
                    .collect::<Result<Vec<_>>>()?;
                Ok(aggregates::return_type(outer_fun, &data_types)?)
            }
            Expr::IsNull(_) => Ok(DataType::Boolean),
            Expr::IsNotNull(_) => Ok(DataType::Boolean),
            Expr::BinaryExpr {
                ref left,
                ref right,
                ref op,
            } => Ok(binary_operator_data_type(
                &left.get_type(schema)?,
                op,
                &right.get_type(schema)?,
            )?),
            Expr::Wildcard => unimplemented!(),
        }
    }

    /// Returns the nullability of the expression based on [arrow::datatypes::Schema].
    ///
    /// # Errors
    ///
    /// This function errors when it is not possible to compute its nullability.
    /// This happens when the expression refers to a column that does not exist in the schema.
    pub fn nullable(&self, input_schema: &DFSchema) -> Result<bool> {
        match self {
            Expr::Alias(expr, _) => expr.nullable(input_schema),
            Expr::Column(c) => Ok(input_schema.field_from_column(c)?.is_nullable()),
            Expr::Literal(value) => Ok(value.is_null()),
            Expr::AggregateFunction { .. } => Ok(true),
            Expr::AggregatePartitionedFunction { .. } => Ok(true),
            Expr::IsNull(_) => Ok(false),
            Expr::IsNotNull(_) => Ok(false),
            Expr::BinaryExpr {
                ref left,
                ref right,
                ..
            } => Ok(left.nullable(input_schema)? || right.nullable(input_schema)?),
            Expr::Wildcard => unimplemented!(),
        }
    }

    /// Returns the name of this expression based on [crate::logical_plan::DFSchema].
    ///
    /// This represents how a column with this expression is named when no alias is chosen
    pub fn name(&self, input_schema: &DFSchema) -> Result<String> {
        create_name(self, input_schema)
    }

    /// Returns a [arrow::datatypes::Field] compatible with this expression.
    pub fn to_field(&self, input_schema: &DFSchema) -> Result<DFField> {
        match self {
            Expr::Column(c) => Ok(DFField::new(
                c.relation.as_deref(),
                &c.name,
                self.get_type(input_schema)?,
                self.nullable(input_schema)?,
            )),
            _ => Ok(DFField::new(
                None,
                &self.name(input_schema)?,
                self.get_type(input_schema)?,
                self.nullable(input_schema)?,
            )),
        }
    }
}

fn fmt_partitioned_function(
    f: &mut fmt::Formatter,
    partition_by: &Expr,
    fun: &str,
    outer_fun: &str,
    distinct: bool,
    args: &[Expr],
) -> fmt::Result {
    let args: Vec<String> = args.iter().map(|arg| format!("{:?}", arg)).collect();
    let distinct_str = match distinct {
        true => "DISTINCT ",
        false => "",
    };
    write!(
        f,
        "{}({}({}{}) PARTITION BY {:?})",
        outer_fun,
        fun,
        distinct_str,
        args.join(", "),
        partition_by
    )
}

fn fmt_function(f: &mut fmt::Formatter, fun: &str, distinct: bool, args: &[Expr]) -> fmt::Result {
    let args: Vec<String> = args.iter().map(|arg| format!("{:?}", arg)).collect();
    let distinct_str = match distinct {
        true => "DISTINCT ",
        false => "",
    };
    write!(f, "{}({}{})", fun, distinct_str, args.join(", "))
}

impl fmt::Debug for Expr {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Expr::Column(c) => write!(f, "{}", c),
            Expr::Literal(v) => write!(f, "{:?}", v),
            Expr::IsNull(expr) => write!(f, "{:?} IS NULL", expr),
            Expr::IsNotNull(expr) => write!(f, "{:?} IS NOT NULL", expr),
            Expr::BinaryExpr { left, op, right } => {
                write!(f, "{:?} {:?} {:?}", left, op, right)
            }
            Expr::AggregateFunction {
                fun,
                distinct,
                ref args,
                ..
            } => fmt_function(f, &fun.to_string(), *distinct, args),
            Expr::AggregatePartitionedFunction {
                partition_by,
                fun,
                outer_fun,
                args,
                distinct,
            } => fmt_partitioned_function(
                f,
                partition_by,
                &fun.to_string(),
                &outer_fun.to_string(),
                *distinct,
                args,
            ),
            Expr::Wildcard => write!(f, "*"),
            Expr::Alias(expr, alias) => write!(f, "{:?} AS {}", expr, alias),
        }
    }
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
    if ident.is_empty() {
        panic!()
    }
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

pub fn lit_timestamp(ts: i64) -> Expr {
    lit(ScalarValue::TimestampMicrosecond(Some(ts)))
}

pub fn is_null(col: Expr) -> Expr {
    Expr::IsNull(Box::new(col))
}

pub fn is_not_null(col: Expr) -> Expr {
    Expr::IsNotNull(Box::new(col))
}
