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

mod final_delivery {
    use fusion_nats::source::{delivery_limit, is_final_delivery};

    #[test]
    fn the_delivery_that_reaches_the_limit_is_final() {
        assert!(is_final_delivery(5, 5));
        assert!(is_final_delivery(6, 5), "past the limit is final too");
        assert!(!is_final_delivery(4, 5));
        assert!(
            is_final_delivery(1, 1),
            "a limit of one makes the first final"
        );
    }

    #[test]
    fn a_consumer_without_a_positive_max_deliver_has_no_limit() {
        assert_eq!(delivery_limit(5), Some(5));
        assert_eq!(delivery_limit(1), Some(1));
        // The server stores 0 as -1, unlimited; async-nats reads a missing field as 0.
        assert_eq!(delivery_limit(0), None);
        assert_eq!(delivery_limit(-1), None);
    }
}
