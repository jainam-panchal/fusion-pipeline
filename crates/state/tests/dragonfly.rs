//! The state store contract against a real Dragonfly (`DRAGONFLY_URL`, default
//! `redis://127.0.0.1:6379`): the same operations the in-memory store is held to, plus the
//! things only a network store can get wrong. All but the unreachable-server test need
//! `deploy/compose.yaml` up and are ignored by default. URL resolution is in `url.rs`.

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use fusion_core::state::{StateStore, StateStoreFactory};
use fusion_state::Dragonfly;

fn unique(prefix: &str) -> String {
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    format!(
        "test:{prefix}:{}:{}",
        std::process::id(),
        COUNTER.fetch_add(1, Ordering::Relaxed)
    )
}

fn store() -> Box<dyn StateStore> {
    Dragonfly::from_env()
        .expect("url parses")
        .open()
        .expect("test can reach Dragonfly at DRAGONFLY_URL")
}

#[test]
fn an_unreachable_server_fails_at_open_naming_the_url() {
    let factory = Dragonfly::new("redis://127.0.0.1:1").expect("url parses");

    let err = match factory.open() {
        Ok(_) => panic!("nothing listens on port 1"),
        Err(err) => err,
    };

    assert!(err.message().contains("127.0.0.1:1"), "{err}");
}

#[test]
#[ignore = "needs Dragonfly at DRAGONFLY_URL"]
fn set_nx_claims_once_and_answers_with_the_holder_in_one_step() {
    let store = store();
    let key = unique("claim");

    assert_eq!(
        store.set_nx(&key, b"101 0", Duration::from_secs(10)),
        Ok(None)
    );
    assert_eq!(
        store.set_nx(&key, b"102 4", Duration::from_secs(10)),
        Ok(Some(b"101 0".to_vec()))
    );
    assert_eq!(store.get(&key), Ok(Some(b"101 0".to_vec())));
    assert_eq!(store.del(&key), Ok(()));
    assert_eq!(store.get(&key), Ok(None));
}

#[test]
#[ignore = "needs Dragonfly at DRAGONFLY_URL"]
fn set_overwrites_the_holder_and_restarts_the_ttl() {
    let store = store();
    let key = unique("set");
    assert_eq!(
        store.set_nx(&key, b"101 0", Duration::from_millis(300)),
        Ok(None)
    );
    std::thread::sleep(Duration::from_millis(200));

    assert_eq!(
        store.set(&key, b"102 10", Duration::from_millis(300)),
        Ok(())
    );

    std::thread::sleep(Duration::from_millis(200));
    assert_eq!(
        store.get(&key),
        Ok(Some(b"102 10".to_vec())),
        "still alive 400ms after the claim: set restarted the ttl"
    );
    let _ = store.del(&key);
}

#[test]
#[ignore = "needs Dragonfly at DRAGONFLY_URL"]
fn a_key_expires_after_its_ttl_and_can_be_claimed_again() {
    let store = store();
    let key = unique("ttl");

    assert_eq!(
        store.set_nx(&key, b"101", Duration::from_millis(200)),
        Ok(None)
    );
    std::thread::sleep(Duration::from_millis(400));

    assert_eq!(store.get(&key), Ok(None), "expired");
    assert_eq!(
        store.set_nx(&key, b"102", Duration::from_millis(200)),
        Ok(None),
        "claimable again"
    );
    let _ = store.del(&key);
}

#[test]
#[ignore = "needs Dragonfly at DRAGONFLY_URL"]
fn incr_counts_from_zero_atomically_with_its_ttl() {
    let store = store();
    let key = unique("incr");

    assert_eq!(store.incr(&key, 1, Duration::from_secs(10)), Ok(1));
    assert_eq!(store.incr(&key, 5, Duration::from_secs(10)), Ok(6));
    assert_eq!(store.get(&key), Ok(Some(b"6".to_vec())));

    assert_eq!(store.incr(&key, 1, Duration::from_millis(200)), Ok(7));
    std::thread::sleep(Duration::from_millis(400));
    assert_eq!(
        store.incr(&key, 1, Duration::from_secs(10)),
        Ok(1),
        "expired and restarted"
    );
    let _ = store.del(&key);
}

#[test]
#[ignore = "needs Dragonfly at DRAGONFLY_URL"]
fn incr_on_a_value_that_is_not_an_integer_is_an_error_not_a_panic() {
    let store = store();
    let key = unique("wrongtype");
    assert_eq!(
        store.set_nx(&key, b"not a number", Duration::from_secs(10)),
        Ok(None)
    );

    assert!(store.incr(&key, 1, Duration::from_secs(10)).is_err());
    let _ = store.del(&key);
}

#[test]
#[ignore = "needs Dragonfly at DRAGONFLY_URL"]
fn two_connections_from_one_factory_share_the_data() {
    let factory = Dragonfly::from_env().expect("url parses");
    let a = factory.open().expect("opens");
    let b = factory.open().expect("opens");
    let key = unique("shared");

    assert_eq!(a.set_nx(&key, b"from a", Duration::from_secs(10)), Ok(None));

    assert_eq!(
        b.set_nx(&key, b"from b", Duration::from_secs(10)),
        Ok(Some(b"from a".to_vec()))
    );
    let _ = a.del(&key);
}
