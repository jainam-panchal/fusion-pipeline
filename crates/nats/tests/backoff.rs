//! Nak backoff: the redelivery delay grows with the delivery count and is capped.

use std::time::Duration;

use fusion_nats::source::{MAX_NAK_DELAY, nak_delay};

#[test]
fn first_failure_waits_one_second_and_then_doubles() {
    assert_eq!(nak_delay(1), Duration::from_secs(1));
    assert_eq!(nak_delay(2), Duration::from_secs(2));
    assert_eq!(nak_delay(3), Duration::from_secs(4));
    assert_eq!(nak_delay(4), Duration::from_secs(8));
}

#[test]
fn delay_is_capped() {
    assert_eq!(MAX_NAK_DELAY, Duration::from_secs(30));
    assert_eq!(nak_delay(6), MAX_NAK_DELAY);
    assert_eq!(nak_delay(1_000), MAX_NAK_DELAY);
}

#[test]
fn a_delivery_count_the_server_did_not_report_counts_as_the_first() {
    assert_eq!(nak_delay(0), Duration::from_secs(1));
}
