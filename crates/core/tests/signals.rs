//! The closed sets the spec fixes for logs and traces (issue #12): the event kinds and their
//! severities, the span outcomes and the settlements. What the engine emits is tested through
//! the harness in the pipeline crate.

use fusion_core::events::{EventKind, Severity};
use fusion_core::trace::{Settlement, SpanOutcome};

/// Spec, Telemetry (issue #12): the event kinds, in this spelling, with these severities.
#[test]
fn event_kinds_and_severities_are_exactly_the_spec_sets() {
    let kinds: Vec<(&str, &str)> = EventKind::ALL
        .iter()
        .map(|k| (k.as_str(), k.severity().as_str()))
        .collect();
    assert_eq!(
        kinds,
        [
            ("stage_error", "error"),
            ("nak", "warn"),
            ("redelivery", "info"),
            ("dead_letter", "warn"),
            ("dead_letter_failed", "error"),
        ]
    );
    let severities: Vec<&str> = Severity::ALL.iter().map(|s| s.as_str()).collect();
    assert_eq!(severities, ["info", "warn", "error"]);
}

/// Spec, Telemetry (issue #12): a node span's `outcome` and a delivery span's `settlement`.
#[test]
fn span_outcomes_and_settlements_are_exactly_the_spec_sets() {
    let outcomes: Vec<&str> = SpanOutcome::ALL.iter().map(|o| o.as_str()).collect();
    assert_eq!(
        outcomes,
        [
            "pass",
            "routed",
            "split",
            "drop",
            "state_error_pass",
            "written",
            "error"
        ]
    );
    let settlements: Vec<&str> = Settlement::ALL.iter().map(|s| s.as_str()).collect();
    assert_eq!(settlements, ["ack", "nak"]);
}
