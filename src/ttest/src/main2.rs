#![allow(dead_code)]

use itertools::Itertools;

struct Column(Vec<i64>);

struct RecordBatch(Vec<Column>);

struct Funnel {
    seq: Vec<usize>,
    pending_matched: SeqMatch,
    pending_partition_key: Option<i64>,
}

impl Funnel {
    fn new(seq: Vec<usize>) -> Self {
        Funnel {
            seq,
            pending_partition_key: None,
            pending_matched: SeqMatch::Unfinished(0),
        }
    }

    fn push(&mut self, mut batch: RecordBatch) -> Option<Vec<bool>> {
        // I'll let this thing panic since we don't return `Result` to
        // deal with bad inputs (less than two columns and/or inconsistent
        // column lengths)

        batch.0.truncate(2);
        let values_column = batch
            .0
            .pop()
            .expect("expected batch with at least 2 columns");
        let keys_column = batch
            .0
            .pop()
            .expect("expected batch with at least 2 columns");

        assert_eq!(keys_column.0.len(), values_column.0.len());

        let mut result: Vec<bool> = Vec::new();

        // Build an iterator to go over two zipped columns simultaneously, additionally
        // grouping by key knowing they're always ordered.
        let key_value_pairs = keys_column.0.into_iter().zip(values_column.0.into_iter());
        let groups = key_value_pairs.group_by(|(k, _)| *k);
        let mut groups_iter = groups.into_iter().peekable();

        // Connect the previous state if there any:
        let mut override_first_partition_check = 0;

        if let Some(pending_partition_key) = self.pending_partition_key {
            match self.pending_matched {
                // In case of the previous batch already had the sequence for the partition,
                // we fast-forward it to a next partition adding `true` to the result since
                // the partition is now finished.
                SeqMatch::Contains => {
                    result.push(true);
                    groups_iter.next_if(|(key, _)| *key == pending_partition_key);
                }

                // If the previous partition had no full match:
                SeqMatch::Unfinished(n) => {
                    // ... check if the batch starts from the same partition:
                    match groups_iter.peek().map(|(key, _)| key) {
                        // Empty batch, nothing to do
                        None => return None,
                        // The partition is continued in the batch, need to check against
                        // the sequence, but this time only partially
                        Some(key) if *key == pending_partition_key => {
                            override_first_partition_check = n;
                        }
                        // Keys are different, previous partition ended with no match:
                        _ => {
                            result.push(false);
                        }
                    }
                }
            }
        };

        // First batch group is special since it may contain a continuation of the previous
        // parition, so it reuses accumulated statistics
        let first_seq_match = groups_iter.next().map(|(key, values)| {
            (
                key,
                contains_sequence(
                    values.map(|(_, v)| v),
                    &self.seq[override_first_partition_check..],
                ),
            )
        });

        // Next groups are processed with full sequence
        let mut seq_matches =
            first_seq_match
                .into_iter()
                .chain(groups_iter.map(|(key, values)| {
                    (key, contains_sequence(values.map(|(_, v)| v), &self.seq))
                }))
                .peekable();

        while let Some((key, seq_match)) = seq_matches.next() {
            if seq_matches.peek().is_none() {
                // We're hitting the last partition of the batch and since we cannot know
                // how it will look like after we need to put it into pending state:
                self.pending_matched = seq_match;
                self.pending_partition_key = Some(key);
            } else {
                // There will be more partitions to process, so this one is finished:
                result.push(matches!(seq_match, SeqMatch::Contains));
            }
        }

        (!result.is_empty()).then_some(result)
    }

    fn finalize(&self) -> bool {
        matches!(self.pending_matched, SeqMatch::Contains)
    }
}

#[derive(Debug, PartialEq)]
enum SeqMatch {
    /// There is a match
    Contains,
    /// No match yet, contains a number of matched items at the end
    Unfinished(usize),
}

/// Checks if `source` conains `seq` in the order, but allowing gaps between items.
fn contains_sequence(source: impl IntoIterator<Item = i64>, seq: &[usize]) -> SeqMatch {
    let mut idx = 0;

    for x in source.into_iter() {
        if idx == seq.len() {
            return SeqMatch::Contains;
        }
        if seq[idx]
            .try_into()
            .map(|seq_value: i64| seq_value == x)
            .unwrap_or(false)
        // if `usize` cannot fit then they're definitely unequal
        {
            idx += 1;
        }
    }

    if idx == seq.len() {
        SeqMatch::Contains
    } else {
        SeqMatch::Unfinished(idx)
    }
}

fn main() {}
#[cfg(test)]
mod tests {
    use crate::contains_sequence;
    use crate::Column;
    use crate::Funnel;
    use crate::RecordBatch;
    use crate::SeqMatch;

    #[test]
    fn test_contains_sequence() {
        assert_eq!(
            contains_sequence(vec![5, 4, 1, 2, 3, 6], &[1, 2, 3]),
            SeqMatch::Contains
        );
        assert_eq!(
            contains_sequence(vec![5, 4, 1, 2, 3, 6], &[1, 2, 4]),
            SeqMatch::Unfinished(2)
        );
        assert_eq!(
            contains_sequence(vec![5, 4, 1, 2, 3, 6], &[5, 4, 4]),
            SeqMatch::Unfinished(2)
        );
        assert_eq!(
            contains_sequence(vec![5, 4, 1, 2, 3, 6], &[5, 4, 1]),
            SeqMatch::Contains
        );
        assert_eq!(
            contains_sequence(vec![5, 4, 1, 2, 3, 6], &[3, 6, 1]),
            SeqMatch::Unfinished(2)
        );
        assert_eq!(
            contains_sequence(vec![5, 4, 1, 2, 3, 6], &[3, 6]),
            SeqMatch::Contains
        );
        assert_eq!(
            contains_sequence(vec![5, 4, 3, 3, 3, 6], &[3, 7]),
            SeqMatch::Unfinished(1)
        );
        assert_eq!(
            contains_sequence(vec![5, 4, 1, 2, 3, 6], &[]),
            SeqMatch::Contains
        );
        assert_eq!(contains_sequence(vec![], &[]), SeqMatch::Contains);
        assert_eq!(contains_sequence(vec![], &[1]), SeqMatch::Unfinished(0));
    }

    #[test]
    fn it_works() {
        let mut funnel = Funnel::new(vec![1, 2, 3]);
        let b1 = RecordBatch(vec![
            Column(vec![1, 1, 1, 2, 2]),
            Column(vec![1, 2, 3, 1, 2]),
        ]);
        let res = funnel.push(b1);
        // партиция 1 найдена, результат партиции 2 ещё не определён
        assert_eq!(Some(vec![true]), res);
        let b2 = RecordBatch(vec![
            Column(vec![2, 3, 3, 3, 3, 3, 4]),
            Column(vec![3, 1, 2, 1, 4, 3, 1]),
        ]);
        let res = funnel.push(b2);
        // партиция 2 собрана из двух батчей и найдена, также найдена партиция 3
        assert_eq!(Some(vec![true, true]), res);
        let b3 = RecordBatch(vec![Column(vec![4]), Column(vec![2])]);
        let res = funnel.push(b3);
        // поскольку в батче только одна партиция, мы не можем знать, есть ли она дальше
        // в батчах, поэтому возвращаем None
        assert_eq!(None, res);
        let res = funnel.finalize();
        // финализируем результат, то есть вычисляем результат 4й прартиции. 4 партиция не имеет
        // полной последовательности, поэтому falsr
        assert_eq!(false, res);
    }
}
