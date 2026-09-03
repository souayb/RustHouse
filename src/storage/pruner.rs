use crate::meta::Segment;

#[derive(Debug, Clone)]
pub enum Predicate {
    Eq(String, serde_json::Value),
    Gt(String, serde_json::Value),
    Lt(String, serde_json::Value),
    Gte(String, serde_json::Value),
    Lte(String, serde_json::Value),
    And(Box<Predicate>, Box<Predicate>),
    Or(Box<Predicate>, Box<Predicate>),
}

pub struct SegmentPruner;

impl SegmentPruner {
    /// Prune segments that cannot possibly match the predicate
    pub fn prune_segments(segments: &[Segment], predicate: &Predicate) -> Vec<Segment> {
        segments
            .iter()
            .filter(|seg| Self::can_match(seg, predicate))
            .cloned()
            .collect()
    }

    fn can_match(segment: &Segment, predicate: &Predicate) -> bool {
        Self::can_match_with(predicate, &|column| match (
            segment.stats.min_values.get(column),
            segment.stats.max_values.get(column),
        ) {
            (Some(min), Some(max)) => Some((min.clone(), max.clone())),
            _ => None,
        })
    }

    /// Evaluate `predicate` against a statistics lookup that returns `(min, max)` for a
    /// column, or `None` when statistics are absent (then the range is kept).
    pub fn can_match_with(
        predicate: &Predicate,
        stats: &dyn Fn(&str) -> Option<(serde_json::Value, serde_json::Value)>,
    ) -> bool {
        match predicate {
            Predicate::Eq(column, _)
            | Predicate::Gt(column, _)
            | Predicate::Lt(column, _)
            | Predicate::Gte(column, _)
            | Predicate::Lte(column, _) => match stats(column) {
                Some((min, max)) => range_can_match(&min, &max, predicate),
                None => true,
            },
            Predicate::And(left, right) => {
                Self::can_match_with(left, stats) && Self::can_match_with(right, stats)
            }
            Predicate::Or(left, right) => {
                Self::can_match_with(left, stats) || Self::can_match_with(right, stats)
            }
        }
    }
}

/// Can a range `[min, max]` contain a row satisfying a leaf predicate? Shared by the hot-tier
/// `SegmentPruner` and Parquet row-group pruning (LAKE-PQT-007). `And`/`Or` recurse on the
/// same range; a null (absent) bound keeps the range.
pub fn range_can_match(
    min: &serde_json::Value,
    max: &serde_json::Value,
    predicate: &Predicate,
) -> bool {
    if min.is_null() || max.is_null() {
        return match predicate {
            Predicate::And(l, r) => range_can_match(min, max, l) && range_can_match(min, max, r),
            Predicate::Or(l, r) => range_can_match(min, max, l) || range_can_match(min, max, r),
            _ => true,
        };
    }
    match predicate {
        Predicate::Eq(_, value) => {
            compare_values(value, min) >= 0 && compare_values(value, max) <= 0
        }
        Predicate::Gt(_, value) => compare_values(max, value) > 0,
        Predicate::Lt(_, value) => compare_values(min, value) < 0,
        Predicate::Gte(_, value) => compare_values(max, value) >= 0,
        Predicate::Lte(_, value) => compare_values(min, value) <= 0,
        Predicate::And(l, r) => range_can_match(min, max, l) && range_can_match(min, max, r),
        Predicate::Or(l, r) => range_can_match(min, max, l) || range_can_match(min, max, r),
    }
}

fn compare_values(a: &serde_json::Value, b: &serde_json::Value) -> i32 {
    {
        use serde_json::Value;

        match (a, b) {
            (Value::Number(a), Value::Number(b)) => {
                let a_f64 = a.as_f64().unwrap_or(0.0);
                let b_f64 = b.as_f64().unwrap_or(0.0);
                if a_f64 < b_f64 {
                    -1
                } else if a_f64 > b_f64 {
                    1
                } else {
                    0
                }
            }
            (Value::String(a), Value::String(b)) => {
                if a < b {
                    -1
                } else if a > b {
                    1
                } else {
                    0
                }
            }
            (Value::Bool(a), Value::Bool(b)) => {
                if a < b {
                    -1
                } else if a > b {
                    1
                } else {
                    0
                }
            }
            _ => 0,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::meta::Segment;
    use std::path::PathBuf;

    #[test]
    fn test_range_can_match_shared_with_segment_pruner() {
        // REQ: LAKE-PQT-007 — the free function and the segment pruner agree on every leaf.
        let seg = create_test_segment("s", 101, 200);
        let (min, max) = (serde_json::json!(101), serde_json::json!(200));
        let preds = [
            Predicate::Eq("id".into(), serde_json::json!(150)),
            Predicate::Eq("id".into(), serde_json::json!(50)),
            Predicate::Gt("id".into(), serde_json::json!(200)),
            Predicate::Gte("id".into(), serde_json::json!(200)),
            Predicate::Lt("id".into(), serde_json::json!(101)),
            Predicate::Lte("id".into(), serde_json::json!(101)),
            Predicate::And(
                Box::new(Predicate::Gt("id".into(), serde_json::json!(150))),
                Box::new(Predicate::Lt("id".into(), serde_json::json!(160))),
            ),
            Predicate::Or(
                Box::new(Predicate::Lt("id".into(), serde_json::json!(0))),
                Box::new(Predicate::Gt("id".into(), serde_json::json!(199))),
            ),
        ];
        for p in &preds {
            let via_segment =
                !SegmentPruner::prune_segments(std::slice::from_ref(&seg), p).is_empty();
            assert_eq!(range_can_match(&min, &max, p), via_segment, "{p:?}");
        }
        // Absent (null) statistics keep the range.
        assert!(range_can_match(
            &serde_json::Value::Null,
            &serde_json::Value::Null,
            &Predicate::Eq("id".into(), serde_json::json!(1))
        ));
    }

    fn create_test_segment(id: &str, min_id: i64, max_id: i64) -> Segment {
        let mut segment = Segment::new(
            id.to_string(),
            "test_table".to_string(),
            PathBuf::from(format!("/data/{}", id)),
        );

        segment
            .stats
            .min_values
            .insert("id".to_string(), serde_json::json!(min_id));
        segment
            .stats
            .max_values
            .insert("id".to_string(), serde_json::json!(max_id));
        segment.stats.row_count = (max_id - min_id + 1) as usize;

        segment
    }

    #[test]
    fn test_prune_eq_predicate() {
        let segments = vec![
            create_test_segment("seg1", 1, 100),
            create_test_segment("seg2", 101, 200),
            create_test_segment("seg3", 201, 300),
        ];

        // Query: WHERE id = 150
        let predicate = Predicate::Eq("id".to_string(), serde_json::json!(150));
        let pruned = SegmentPruner::prune_segments(&segments, &predicate);

        // Only seg2 should remain
        assert_eq!(pruned.len(), 1);
        assert_eq!(pruned[0].id, "seg2");
    }

    #[test]
    fn test_prune_gt_predicate() {
        let segments = vec![
            create_test_segment("seg1", 1, 100),
            create_test_segment("seg2", 101, 200),
            create_test_segment("seg3", 201, 300),
        ];

        // Query: WHERE id > 190
        let predicate = Predicate::Gt("id".to_string(), serde_json::json!(190));
        let pruned = SegmentPruner::prune_segments(&segments, &predicate);

        // seg2 and seg3 should remain (their max values > 190)
        assert_eq!(pruned.len(), 2);
        assert_eq!(pruned[0].id, "seg2");
        assert_eq!(pruned[1].id, "seg3");
    }

    #[test]
    fn test_prune_lt_predicate() {
        let segments = vec![
            create_test_segment("seg1", 1, 100),
            create_test_segment("seg2", 101, 200),
            create_test_segment("seg3", 201, 300),
        ];

        // Query: WHERE id < 110
        let predicate = Predicate::Lt("id".to_string(), serde_json::json!(110));
        let pruned = SegmentPruner::prune_segments(&segments, &predicate);

        // seg1 and seg2 should remain (their min values < 110)
        assert_eq!(pruned.len(), 2);
        assert_eq!(pruned[0].id, "seg1");
        assert_eq!(pruned[1].id, "seg2");
    }

    #[test]
    fn test_prune_and_predicate() {
        let segments = vec![
            create_test_segment("seg1", 1, 100),
            create_test_segment("seg2", 101, 200),
            create_test_segment("seg3", 201, 300),
        ];

        // Query: WHERE id > 50 AND id < 150
        let predicate = Predicate::And(
            Box::new(Predicate::Gt("id".to_string(), serde_json::json!(50))),
            Box::new(Predicate::Lt("id".to_string(), serde_json::json!(150))),
        );
        let pruned = SegmentPruner::prune_segments(&segments, &predicate);

        // seg1 and seg2 should remain
        assert_eq!(pruned.len(), 2);
        assert_eq!(pruned[0].id, "seg1");
        assert_eq!(pruned[1].id, "seg2");
    }

    #[test]
    fn test_prune_or_predicate() {
        let segments = vec![
            create_test_segment("seg1", 1, 100),
            create_test_segment("seg2", 101, 200),
            create_test_segment("seg3", 201, 300),
        ];

        // Query: WHERE id < 50 OR id > 250
        let predicate = Predicate::Or(
            Box::new(Predicate::Lt("id".to_string(), serde_json::json!(50))),
            Box::new(Predicate::Gt("id".to_string(), serde_json::json!(250))),
        );
        let pruned = SegmentPruner::prune_segments(&segments, &predicate);

        // seg1 and seg3 should remain
        assert_eq!(pruned.len(), 2);
        assert_eq!(pruned[0].id, "seg1");
        assert_eq!(pruned[1].id, "seg3");
    }

    #[test]
    fn test_prune_no_segments_match() {
        let segments = vec![
            create_test_segment("seg1", 1, 100),
            create_test_segment("seg2", 101, 200),
        ];

        // Query: WHERE id > 300
        let predicate = Predicate::Gt("id".to_string(), serde_json::json!(300));
        let pruned = SegmentPruner::prune_segments(&segments, &predicate);

        // No segments should remain
        assert_eq!(pruned.len(), 0);
    }

    #[test]
    fn test_prune_all_segments_match() {
        let segments = vec![
            create_test_segment("seg1", 1, 100),
            create_test_segment("seg2", 101, 200),
            create_test_segment("seg3", 201, 300),
        ];

        // Query: WHERE id > 0 (always true for our data)
        let predicate = Predicate::Gt("id".to_string(), serde_json::json!(0));
        let pruned = SegmentPruner::prune_segments(&segments, &predicate);

        // All segments should remain
        assert_eq!(pruned.len(), 3);
    }

    #[test]
    fn test_string_comparison() {
        let mut segment = Segment::new(
            "seg1".to_string(),
            "test".to_string(),
            PathBuf::from("/data/seg1"),
        );

        segment
            .stats
            .min_values
            .insert("name".to_string(), serde_json::json!("apple"));
        segment
            .stats
            .max_values
            .insert("name".to_string(), serde_json::json!("zebra"));

        let segments = vec![segment];

        // Should match
        let pred1 = Predicate::Eq("name".to_string(), serde_json::json!("banana"));
        let pruned1 = SegmentPruner::prune_segments(&segments, &pred1);
        assert_eq!(pruned1.len(), 1);

        // Should not match
        let pred2 = Predicate::Gt("name".to_string(), serde_json::json!("zoo"));
        let pruned2 = SegmentPruner::prune_segments(&segments, &pred2);
        assert_eq!(pruned2.len(), 0);
    }
}
