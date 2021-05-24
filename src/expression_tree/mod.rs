mod expr;
mod context;
// mod binary_op;
mod value;
mod boolean_op;
mod binary_op;
mod literal;
mod cond_value;
mod iterative_binary_op;
// mod value_op;
mod iterative_count;
mod iterative_sum;
mod value_op;
// mod relation_op;
// mod count;
// mod iterative_binary_op;
// mod value;
// mod literal;

#[cfg(test)]
mod tests {
    use datafusion::prelude::ExecutionContext;
    use datafusion::datasource::CsvReadOptions;
    use datafusion::error::Result;
    use crate::expression_tree::binary_op::BinaryOp;
    use crate::expression_tree::boolean_op::{And, Gt, Or, Lt, Eq};
    use crate::expression_tree::iterative_binary_op::IterativeBinaryOp;
    use crate::expression_tree::iterative_count::IterativeCountOp;
    // use crate::expression_tree::value_op::ValueOp;
    use crate::expression_tree::iterative_sum::IterativeSumOp;
    use arrow::alloc::free_aligned;
    use crate::expression_tree::expr::Expr;

/*    #[tokio::test]
    async fn test() -> Result<()> {
        let mut ctx = ExecutionContext::new();
        let batches = ctx.read_csv("tests/events.csv", CsvReadOptions::new())?.collect().await?;

        let a = BinaryOp::<_, And>::new(
            Box::new(IterativeCountOp::<Gt>::new(
                Box::new(BinaryOp::<_, And>::new(
                    Box::new(ValueOp::<_, Eq>::new(2, Some("search".to_string()))),
                    Box::new(BinaryOp::<_, Or>::new(
                        Box::new(ValueOp::<_, Eq>::new(3, Some("rock".to_string()))),
                        Box::new(ValueOp::<_, Eq>::new(3, Some("pop".to_string()))),
                    )),
                )),
                1,
                false,
            )),
            Box::new(IterativeSumOp::<_, Lt>::new(
                Box::new(ValueOp::<_, Eq>::new(2, Some("buy".to_string()))),
                4,
                100i8,
                false,
            )),
        );

        assert_eq!(true, a.evaluate(&batches[0], 0));
        Ok(())
    }*/
}