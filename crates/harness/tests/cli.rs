//! The binaries' flag parsing.

use std::time::Duration;

use fusion_harness::cli::{self, Flags};

fn flags(args: &[&str]) -> Result<Flags, String> {
    cli::parse(
        args.iter().map(|a| (*a).to_owned()),
        &["--rate", "--datasets"],
    )
}

#[test]
fn known_flags_take_the_next_argument() {
    let f = flags(&["--rate", "10", "--datasets", "Linux,Mac"]).expect("parses");
    assert_eq!(f.value("--rate", 5.0_f64), Ok(10.0));
    assert_eq!(f.get("--datasets"), Some("Linux,Mac"));
    assert_eq!(f.value("--missing", 7_u64), Ok(7));
}

#[test]
fn unknown_repeated_or_valueless_flags_are_refused() {
    assert!(
        flags(&["--speed", "1"])
            .expect_err("refused")
            .contains("--speed")
    );
    assert!(flags(&["--rate"]).expect_err("refused").contains("--rate"));
    assert!(
        flags(&["--rate", "1", "--rate", "2"])
            .expect_err("refused")
            .contains("twice")
    );
    let f = flags(&["--rate", "fast"]).expect("parses");
    assert!(
        f.value("--rate", 1.0_f64)
            .expect_err("refused")
            .contains("fast")
    );
}

#[test]
fn durations_take_a_unit() {
    assert_eq!(cli::duration("2s"), Ok(Duration::from_secs(2)));
    assert_eq!(cli::duration("500ms"), Ok(Duration::from_millis(500)));
    assert_eq!(cli::duration("1.5s"), Ok(Duration::from_millis(1500)));
    assert!(cli::duration("2").is_err());
    assert!(cli::duration("-1s").is_err());
}

#[test]
fn both_binaries_share_the_expectations_path_and_nats_url_defaults() {
    let known = ["--expectations", "--nats-url"];
    let none = cli::parse(std::iter::empty(), &known).expect("parses");
    assert_eq!(
        none.expectations(),
        std::path::PathBuf::from(cli::DEFAULT_EXPECTATIONS)
    );
    let given = cli::parse(
        ["--expectations", "x.jsonl", "--nats-url", "nats://h:1"]
            .into_iter()
            .map(str::to_owned),
        &known,
    )
    .expect("parses");
    assert_eq!(given.expectations(), std::path::PathBuf::from("x.jsonl"));
    assert_eq!(given.nats_url(), "nats://h:1");
}
