//! The producer's plan: which line goes out under which id at which time, and which messages
//! are deliberate duplicates.

use std::collections::{BTreeMap, BTreeSet};
use std::time::Duration;

use fusion_harness::plan::{DUP_LAG, PlanConfig, PlanError, plan};

/// The vendored sets' distinct line counts: Linux, OpenSSH, Apache, Mac.
const LENS: [usize; 4] = [2000, 2000, 1461, 1991];

const BASE: u64 = 7 << 22;

fn defaults() -> PlanConfig {
    PlanConfig::default()
}

#[test]
fn the_default_flags_are_accepted() {
    let config = defaults();
    assert_eq!(config.count, 100_000);
    assert_eq!(config.dup_percent, 30);
    let messages = plan(&config, &LENS, BASE).expect("defaults plan");
    assert_eq!(messages.len(), 100_000);
}

#[test]
fn the_same_seed_gives_the_same_plan() {
    let config = PlanConfig {
        count: 5_000,
        ..defaults()
    };
    assert_eq!(plan(&config, &LENS, BASE), plan(&config, &LENS, BASE));
    let other = PlanConfig { seed: 2, ..config };
    assert_ne!(plan(&config, &LENS, BASE), plan(&other, &LENS, BASE));
}

#[test]
fn ids_are_unique_and_under_the_base() {
    let messages = plan(&defaults(), &LENS, BASE).expect("plan");
    let ids: BTreeSet<_> = messages.iter().map(|m| m.id).collect();
    assert_eq!(ids.len(), messages.len());
    assert!(ids.iter().all(|id| id >> 22 == BASE >> 22));
}

#[test]
fn the_duplicate_share_is_the_requested_percent() {
    let messages = plan(&defaults(), &LENS, BASE).expect("plan");
    let dups = messages.iter().filter(|m| m.dup_of.is_some()).count();
    let share = dups as f64 / messages.len() as f64;
    assert!((share - 0.30).abs() < 0.01, "share {share}");
}

#[test]
fn a_duplicate_repeats_its_original_line_and_cycle_within_the_lag() {
    let messages = plan(&defaults(), &LENS, BASE).expect("plan");
    let by_id: BTreeMap<_, _> = messages.iter().map(|m| (m.id, m)).collect();
    for dup in messages.iter().filter(|m| m.dup_of.is_some()) {
        let original = by_id[&dup.dup_of.expect("a duplicate")];
        assert!(original.dup_of.is_none(), "a duplicate of an original");
        assert_eq!(
            (dup.set, dup.line, dup.cycle),
            (original.set, original.line, original.cycle)
        );
        assert!(dup.at > original.at);
        assert!(
            dup.at - original.at <= DUP_LAG,
            "lag {:?}",
            dup.at - original.at
        );
    }
}

#[test]
fn messages_are_sent_at_the_rate_in_order() {
    let config = PlanConfig {
        count: 1_000,
        rate: 100.0,
        ..defaults()
    };
    let messages = plan(&config, &LENS, BASE).expect("plan");
    assert!(messages.windows(2).all(|w| w[0].at <= w[1].at));
    assert_eq!(messages[100].at, Duration::from_secs(1));
}

#[test]
fn originals_go_round_robin_and_cycle_through_each_set() {
    let config = PlanConfig {
        count: 20_000,
        dup_percent: 0,
        rate: 500.0,
        ..defaults()
    };
    let messages = plan(&config, &LENS, BASE).expect("plan");
    let sets: Vec<_> = messages.iter().take(8).map(|m| m.set).collect();
    assert_eq!(sets, [0, 1, 2, 3, 0, 1, 2, 3]);
    // Apache has the fewest lines, so it is the first set into its second cycle.
    let apache: Vec<_> = messages.iter().filter(|m| m.set == 2).collect();
    assert_eq!((apache[1460].line, apache[1460].cycle), (1460, 0));
    assert_eq!((apache[1461].line, apache[1461].cycle), (0, 1));
}

#[test]
fn a_rate_that_repeats_a_body_too_soon_is_refused() {
    // Apache's 1,461 lines, one in four sends, 70% originals: at 4,000/s a body comes back
    // after about 2.1s, inside window + lag + margin.
    let config = PlanConfig {
        rate: 4_000.0,
        ..defaults()
    };
    match plan(&config, &LENS, BASE) {
        Err(PlanError::RateTooHigh { repeat, needed }) => {
            assert!(repeat < needed, "{repeat:?} < {needed:?}");
        }
        other => panic!("expected RateTooHigh, got {other:?}"),
    }
}

#[test]
fn out_of_range_flags_are_refused() {
    let cases = [
        PlanConfig {
            dup_percent: 51,
            ..defaults()
        },
        PlanConfig {
            rate: 0.0,
            ..defaults()
        },
        PlanConfig {
            count: 1 << 22,
            ..defaults()
        },
    ];
    for config in cases {
        assert!(plan(&config, &LENS, BASE).is_err(), "{config:?}");
    }
    assert!(plan(&defaults(), &[], BASE).is_err(), "no sets");
    assert!(plan(&defaults(), &[2000, 0], BASE).is_err(), "an empty set");
}
