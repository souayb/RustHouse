//! Acceptance tests for specs/features/CORE-PRN-segment-pruning.md

use rusthouse::meta::Segment;
use rusthouse::storage::{Predicate, SegmentPruner};
use serde_json::json;
use std::path::PathBuf;

fn segment(id: &str, min: i64, max: i64) -> Segment {
    let mut s = Segment::new(id.to_string(), "t".to_string(), PathBuf::from(id));
    s.stats.min_values.insert("id".to_string(), json!(min));
    s.stats.max_values.insert("id".to_string(), json!(max));
    s.stats.row_count = (max - min + 1) as usize;
    s
}

#[test]
fn core_prn_005_missing_stats_keeps_segment() {
    // REQ: CORE-PRN-005
    let no_stats = Segment::new("s".to_string(), "t".to_string(), PathBuf::from("s"));
    let preds = [
        Predicate::Eq("id".into(), json!(5)),
        Predicate::Gt("id".into(), json!(5)),
        Predicate::Lt("id".into(), json!(5)),
        Predicate::Gte("id".into(), json!(5)),
        Predicate::Lte("id".into(), json!(5)),
    ];
    for p in preds {
        let kept = SegmentPruner::prune_segments(std::slice::from_ref(&no_stats), &p);
        assert_eq!(
            kept.len(),
            1,
            "segment without stats must be kept for {p:?}"
        );
    }
}

/// Exhaustive check over a small domain: for every segment [lo, hi] and every value v in it,
/// a predicate that v satisfies must keep the segment (no false negatives).
#[test]
fn core_prn_001_pruning_is_conservative() {
    // REQ: CORE-PRN-001
    let domain = -3..=6i64;
    for lo in domain.clone() {
        for hi in lo..=6 {
            let seg = segment("s", lo, hi);
            for v in lo..=hi {
                for c in -4..=7i64 {
                    let cases: Vec<(Predicate, bool)> = vec![
                        (Predicate::Eq("id".into(), json!(c)), v == c),
                        (Predicate::Gt("id".into(), json!(c)), v > c),
                        (Predicate::Lt("id".into(), json!(c)), v < c),
                        (Predicate::Gte("id".into(), json!(c)), v >= c),
                        (Predicate::Lte("id".into(), json!(c)), v <= c),
                        (
                            Predicate::And(
                                Box::new(Predicate::Gte("id".into(), json!(c))),
                                Box::new(Predicate::Lte("id".into(), json!(c + 1))),
                            ),
                            v >= c && v <= c + 1,
                        ),
                        (
                            Predicate::Or(
                                Box::new(Predicate::Lt("id".into(), json!(c))),
                                Box::new(Predicate::Gt("id".into(), json!(c + 1))),
                            ),
                            v < c || v > c + 1,
                        ),
                    ];
                    for (pred, row_matches) in cases {
                        if row_matches {
                            let kept =
                                SegmentPruner::prune_segments(std::slice::from_ref(&seg), &pred);
                            assert_eq!(
                                kept.len(),
                                1,
                                "false negative: seg [{lo},{hi}] row {v} pred {pred:?}"
                            );
                        }
                    }
                }
            }
        }
    }
}
