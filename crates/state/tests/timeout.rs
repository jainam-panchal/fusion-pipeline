//! A store that stops answering must become a `StateError` within the operation timeout,
//! and the connection must be reopened afterwards rather than reading the late reply as the
//! answer to the next command. Its own binary: `CLIENT PAUSE` stalls every client on the
//! server, which would fail any other test running alongside.

use std::time::{Duration, Instant};

use fusion_core::state::StateStoreFactory;
use fusion_state::{Dragonfly, url_from_env};

#[test]
#[ignore = "needs Dragonfly at DRAGONFLY_URL; pauses the server for 3s"]
fn a_paused_store_times_out_and_the_connection_is_reopened_afterwards() {
    let store = Dragonfly::from_env()
        .expect("url parses")
        .open()
        .expect("test can reach Dragonfly at DRAGONFLY_URL");
    let key = format!("test:timeout:{}", std::process::id());
    assert_eq!(
        store.set_nx(&key, b"before", Duration::from_secs(30)),
        Ok(None)
    );

    let admin = redis::Client::open(url_from_env()).expect("url");
    let mut admin = admin.get_connection().expect("admin connection");
    let paused_at = Instant::now();
    let _: String = redis::cmd("CLIENT")
        .arg("PAUSE")
        .arg(3000)
        .query(&mut admin)
        .expect("CLIENT PAUSE");

    let started = Instant::now();
    let during = store.get(&key);
    let took = started.elapsed();
    assert!(
        during.is_err(),
        "the paused store must fail, got {during:?}"
    );
    assert!(
        took >= Duration::from_secs(1) && took < Duration::from_millis(2900),
        "failed on the operation timeout, not the pause: {took:?}"
    );

    std::thread::sleep(Duration::from_millis(3200).saturating_sub(paused_at.elapsed()));
    assert_eq!(
        store.get(&key),
        Ok(Some(b"before".to_vec())),
        "reopened and answering the right question"
    );
    let _ = store.del(&key);
}
