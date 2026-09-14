//! The state store contract on the in-memory implementation: the four operations, the
//! `set_nx` claim, and expiry driven by the fake clock. The Dragonfly implementation runs
//! the same contract in its own crate.

use std::time::Duration;

use fusion_core::memory::MemoryStateStore;
use fusion_core::state::{StateStore, StateStoreFactory};

const TTL: Duration = Duration::from_secs(10);

#[test]
fn set_nx_claims_a_key_once_and_then_answers_with_the_value_that_holds_it() {
    let store = MemoryStateStore::new();

    assert_eq!(store.set_nx("k", b"101", TTL), Ok(None), "claimed");
    assert_eq!(
        store.set_nx("k", b"102", TTL),
        Ok(Some(b"101".to_vec())),
        "not claimed: the existing value comes back"
    );
    assert_eq!(store.get("k"), Ok(Some(b"101".to_vec())));
}

#[test]
fn set_overwrites_whatever_holds_the_key_and_restarts_its_ttl() {
    let store = MemoryStateStore::new();
    assert_eq!(store.set_nx("k", b"101", TTL), Ok(None));
    store.advance(TTL - Duration::from_millis(1));

    assert_eq!(store.set("k", b"102", TTL), Ok(()));

    assert_eq!(store.get("k"), Ok(Some(b"102".to_vec())));
    store.advance(TTL - Duration::from_millis(1));
    assert_eq!(
        store.get("k"),
        Ok(Some(b"102".to_vec())),
        "ttl restarted by set"
    );
    store.advance(Duration::from_millis(1));
    assert_eq!(store.get("k"), Ok(None));
}

#[test]
fn get_of_an_unknown_key_is_none() {
    let store = MemoryStateStore::new();

    assert_eq!(store.get("missing"), Ok(None));
}

#[test]
fn a_key_expires_after_its_ttl_and_can_be_claimed_again() {
    let store = MemoryStateStore::new();
    assert_eq!(store.set_nx("k", b"101", TTL), Ok(None));

    store.advance(TTL - Duration::from_millis(1));
    assert_eq!(
        store.set_nx("k", b"102", TTL),
        Ok(Some(b"101".to_vec())),
        "still inside the ttl"
    );

    store.advance(Duration::from_millis(1));
    assert_eq!(store.get("k"), Ok(None), "expired");
    assert_eq!(store.set_nx("k", b"103", TTL), Ok(None), "claimable again");
    assert_eq!(store.get("k"), Ok(Some(b"103".to_vec())));
}

#[test]
fn incr_counts_from_zero_and_applies_the_ttl() {
    let store = MemoryStateStore::new();

    assert_eq!(store.incr("n", 1, TTL), Ok(1));
    assert_eq!(store.incr("n", 5, TTL), Ok(6));
    assert_eq!(store.get("n"), Ok(Some(b"6".to_vec())));

    store.advance(TTL);
    assert_eq!(
        store.incr("n", 1, TTL),
        Ok(1),
        "counter expired and restarted"
    );
}

#[test]
fn del_removes_a_key() {
    let store = MemoryStateStore::new();
    assert_eq!(store.set_nx("k", b"101", TTL), Ok(None));

    assert_eq!(store.del("k"), Ok(()));

    assert_eq!(store.get("k"), Ok(None));
    assert_eq!(
        store.del("k"),
        Ok(()),
        "deleting an absent key is not an error"
    );
}

#[test]
fn every_connection_opened_from_one_store_shares_its_data() {
    let store = MemoryStateStore::new();
    let worker_a = store.open().expect("opens");
    let worker_b = store.open().expect("opens");

    assert_eq!(worker_a.set_nx("k", b"101", TTL), Ok(None));

    assert_eq!(worker_b.set_nx("k", b"102", TTL), Ok(Some(b"101".to_vec())));
    assert_eq!(worker_b.get("k"), Ok(Some(b"101".to_vec())));
    assert_eq!(store.keys(), vec!["k".to_owned()]);
}

#[test]
fn a_failing_store_returns_an_error_from_every_operation() {
    let store = MemoryStateStore::new();
    store.fail_all(true);

    assert!(store.set_nx("k", b"1", TTL).is_err());
    assert!(store.set("k", b"1", TTL).is_err());
    assert!(store.get("k").is_err());
    assert!(store.incr("n", 1, TTL).is_err());
    assert!(store.del("k").is_err());

    store.fail_all(false);
    assert_eq!(
        store.set_nx("k", b"1", TTL),
        Ok(None),
        "recovers when told to"
    );
}

#[test]
fn a_failing_store_still_opens_so_startup_and_outage_are_separate_faults() {
    let store = MemoryStateStore::new();
    store.fail_all(true);

    assert!(store.open().is_ok());
}
