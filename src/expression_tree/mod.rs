pub mod expr;
mod context;
// mod binary_op;
mod value;
pub mod boolean_op;
pub mod binary_op;
mod literal;
mod cond_value;
pub mod iterative_count_op;
mod iterative_sum_op;
pub mod value_op;
mod scalar;
pub mod count;
mod sum;
mod sequence;

#[cfg(test)]
mod tests {
    use datafusion::prelude::ExecutionContext;
    use datafusion::datasource::CsvReadOptions;
    use datafusion::error::Result;
    use crate::expression_tree::binary_op::BinaryOp;
    use crate::expression_tree::boolean_op::{And, Gt, Or, Lt, Eq};
    use crate::expression_tree::iterative_count_op::IterativeCountOp;
    // use crate::expression_tree::value_op::ValueOp;
    use crate::expression_tree::iterative_sum_op::IterativeSumOp;
    use arrow::alloc::free_aligned;
    use crate::expression_tree::expr::Expr;
    use crate::expression_tree::value_op::ValueOp;

    #[tokio::test]
    async fn test() -> Result<()> {
        let mut ctx = ExecutionContext::new();
        let batches = ctx.read_csv("tests/user1_events.csv", CsvReadOptions::new())?.collect().await?;

        let a = BinaryOp::<_, And>::new(
            Box::new(IterativeCountOp::<Gt>::new(
                Box::new(BinaryOp::<_, And>::new(
                    Box::new(ValueOp::<_, Eq>::new(2, Some("search"))),
                    Box::new(BinaryOp::<_, Or>::new(
                        Box::new(ValueOp::<_, Eq>::new(3, Some("rock"))),
                        Box::new(ValueOp::<_, Eq>::new(3, Some("pop"))),
                    )),
                )),
                1,
            )),
            Box::new(IterativeSumOp::<_, Lt>::new(
                Box::new(ValueOp::<_, Eq>::new(2, Some("buy"))),
                4,
                100i64,
            )),
        );

        assert_eq!(true, a.evaluate(&batches[0], 0));
        Ok(())
    }
}