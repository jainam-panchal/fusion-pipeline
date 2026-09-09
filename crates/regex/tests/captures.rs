//! Named capture extraction gives the same answer on both engines for shared syntax.
#![allow(clippy::unwrap_used)]

use fusion_regex::{Engine, EngineChoice, Options, Regex};

const LINUX_SYSLOG: &str = r"^(?<Month>[A-Z][a-z]{2}) +(?<Date>\d{1,2}) (?<Time>\d{2}:\d{2}:\d{2}) (?<Level>\S+) (?<Component>[^\[:]+)(?:\[(?<PID>\d+)\])?: (?<Content>.*)$";
const LINUX_LINE: &str =
    "Jun 14 15:16:01 combo sshd(pam_unix)[19939]: authentication failure; logname= uid=0";

fn on(engine: EngineChoice, pattern: &str) -> Regex {
    Regex::with_options(pattern, &Options { engine, ..Options::unchecked() }).unwrap()
}

fn named(re: &Regex, hay: &str) -> Vec<(String, String)> {
    let caps = re.captures(hay).unwrap().expect("should match");
    caps.named().map(|(k, v)| (k.to_owned(), v.to_owned())).collect()
}

#[test]
fn loghub_linux_line_extracts_identically_on_both_engines() {
    let linear = on(EngineChoice::Linear, LINUX_SYSLOG);
    let backtracking = on(EngineChoice::Backtracking, LINUX_SYSLOG);
    assert_eq!(linear.engine(), Engine::Linear);
    assert_eq!(backtracking.engine(), Engine::Backtracking);

    let expected: Vec<(String, String)> = [
        ("Month", "Jun"),
        ("Date", "14"),
        ("Time", "15:16:01"),
        ("Level", "combo"),
        ("Component", "sshd(pam_unix)"),
        ("PID", "19939"),
        ("Content", "authentication failure; logname= uid=0"),
    ]
    .into_iter()
    .map(|(k, v)| (k.to_owned(), v.to_owned()))
    .collect();

    assert_eq!(named(&linear, LINUX_LINE), expected);
    assert_eq!(named(&backtracking, LINUX_LINE), expected);
}

#[test]
fn optional_group_that_did_not_participate_is_absent_on_both_engines() {
    let line = "Jun  9 06:06:20 combo syslogd 1.4.1: restart.";
    for engine in [EngineChoice::Linear, EngineChoice::Backtracking] {
        let re = on(engine, LINUX_SYSLOG);
        let caps = re.captures(line).unwrap().unwrap();
        assert_eq!(caps.name("Date"), Some("9"), "{engine:?}");
        assert_eq!(caps.name("PID"), None, "{engine:?}");
        assert_eq!(caps.name("Component"), Some("syslogd 1.4.1"), "{engine:?}");
        assert!(!named(&re, line).iter().any(|(k, _)| k == "PID"), "{engine:?}");
    }
}

#[test]
fn unicode_classes_agree_on_both_engines() {
    for engine in [EngineChoice::Linear, EngineChoice::Backtracking] {
        let re = on(engine, r"(?<w>\w+)\s+(?<d>\d+)$");
        let caps = re.captures("naïve\u{2003}٣٤").unwrap().unwrap();
        assert_eq!(caps.name("w"), Some("naïve"), "{engine:?}");
        assert_eq!(caps.name("d"), Some("٣٤"), "{engine:?}");
        assert!(!re.is_match("x 1\n").unwrap(), "{engine:?}: $ must mean end of text");
    }
}

#[test]
fn no_match_is_none_not_error() {
    for engine in [EngineChoice::Linear, EngineChoice::Backtracking] {
        let re = on(engine, r"^\d+$");
        assert!(re.captures("abc").unwrap().is_none(), "{engine:?}");
    }
}

#[test]
fn capture_names_are_listed_in_group_order_on_both_engines() {
    for engine in [EngineChoice::Linear, EngineChoice::Backtracking] {
        let re = on(engine, r"(?<a>x)(y)(?<c>z)");
        let names: Vec<Option<&str>> = re.capture_names().collect();
        assert_eq!(names, vec![None, Some("a"), None, Some("c")], "{engine:?}");
    }
}
