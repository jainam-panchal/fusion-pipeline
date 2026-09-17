//! Pipeline headers through the crate's public helpers: what the sink writes for a `Meta`,
//! the arrival the source builds from a subject, headers and the JetStream message info,
//! and what the source writes on a dead letter.
//! The live round trip is in `jetstream.rs`.

use async_nats::HeaderMap;
use fusion_core::io::{Failure, FailureKind};
use fusion_core::meta::{Arrival, IngestionTime, Meta};
use fusion_core::record::{Kind, RecordId};
use fusion_nats::headers::{
    self, DLQ_REASON, DLQ_SUBJECT, DeadLetter, INGESTION_TIME, INGESTION_TIME_KIND, InvalidHeader,
    MSG_ID, REASON_CAP, RECORD_ID, RECORD_KIND, Received, TENANT,
};

fn meta(ingestion_time: IngestionTime) -> Meta {
    Meta {
        record_id: RecordId(7),
        tenant: "acme".into(),
        ingestion_time,
        delivery_count: 3,
    }
}

fn map(pairs: &[(&str, &str)]) -> HeaderMap {
    let mut headers = HeaderMap::new();
    for (name, value) in pairs {
        headers.insert(*name, *value);
    }
    headers
}

/// The arrival of a message on `subject` under the default `logs` prefix.
fn arrival_of(
    subject: &str,
    headers: Option<&HeaderMap>,
    published: Option<u64>,
    delivered: u64,
) -> (Arrival, Vec<InvalidHeader>) {
    headers::arrival(
        "logs",
        Received {
            subject,
            headers,
            published,
            delivered,
            bytes: 0,
        },
    )
}

fn value<'h>(headers: &'h HeaderMap, name: &str) -> Option<&'h str> {
    headers.get(name).map(|v| v.as_str())
}

#[test]
fn the_sink_writes_the_record_id_the_tenant_the_time_and_its_kind_and_nothing_else() {
    let written = headers::for_meta(&meta(IngestionTime::Reported(9_000_000_000)));
    assert_eq!(value(&written, RECORD_ID), Some("7"));
    assert_eq!(value(&written, TENANT), Some("acme"));
    assert_eq!(value(&written, INGESTION_TIME), Some("9000000000"));
    assert_eq!(value(&written, INGESTION_TIME_KIND), Some("reported"));
    assert_eq!(
        written.len(),
        4,
        "no kind (every walked record is a log), no delivery count"
    );
}

#[test]
fn a_clock_time_is_written_as_a_clock_time() {
    let written = headers::for_meta(&meta(IngestionTime::Clock(5)));
    assert_eq!(value(&written, INGESTION_TIME), Some("5"));
    assert_eq!(value(&written, INGESTION_TIME_KIND), Some("clock"));
}

#[test]
fn what_the_sink_writes_a_downstream_source_reads_back() {
    for time in [
        IngestionTime::Reported(9_000_000_000),
        IngestionTime::Clock(5),
    ] {
        let written = headers::for_meta(&meta(time));
        let (arrival, invalid) = arrival_of("processed.logs", Some(&written), Some(1), 1);
        assert_eq!(
            arrival,
            Arrival {
                record_id: Some(RecordId(7)),
                kind: None,
                tenant: Some("acme".to_owned()),
                ingestion_time: Some(time),
                delivery_count: 1,
                bytes: Some(0),
            }
        );
        assert!(invalid.is_empty());
    }
}

#[test]
fn a_message_with_no_headers_takes_the_subjects_tenant_and_the_publish_time() {
    let (arrival, invalid) = arrival_of("logs.acme.syslog", None, Some(9_000_000_000), 2);
    assert_eq!(
        arrival,
        Arrival {
            record_id: None,
            kind: None,
            tenant: Some("acme".to_owned()),
            ingestion_time: Some(IngestionTime::Reported(9_000_000_000)),
            delivery_count: 2,
            bytes: Some(0),
        }
    );
    assert!(invalid.is_empty());
}

#[test]
fn the_record_id_and_the_kind_are_the_headers() {
    for (name, kind) in [
        ("log", Kind::Log),
        ("metric", Kind::Metric),
        ("span", Kind::Span),
    ] {
        let headers = map(&[(RECORD_ID, "18446744073709551615"), (RECORD_KIND, name)]);
        let (arrival, invalid) = arrival_of("logs.acme.syslog", Some(&headers), None, 1);
        assert!(invalid.is_empty(), "{name}");
        assert_eq!(arrival.record_id, Some(RecordId(u64::MAX)), "{name}");
        assert_eq!(arrival.kind, Some(kind), "{name}");
    }
}

#[test]
fn the_subjects_tenant_wins_over_the_header() {
    let headers = map(&[(TENANT, "beta")]);
    let (arrival, invalid) = arrival_of("logs.acme.syslog", Some(&headers), None, 1);
    assert!(invalid.is_empty());
    assert_eq!(arrival.tenant.as_deref(), Some("acme"));
}

#[test]
fn the_header_tenant_is_used_when_the_subject_names_none() {
    let headers = map(&[(TENANT, "beta")]);
    let (arrival, invalid) = arrival_of("processed", Some(&headers), None, 1);
    assert!(invalid.is_empty());
    assert_eq!(arrival.tenant.as_deref(), Some("beta"));
}

#[test]
fn the_header_time_wins_over_the_publish_time() {
    let headers = map(&[(INGESTION_TIME, "5"), (INGESTION_TIME_KIND, "reported")]);
    let (arrival, invalid) = arrival_of("logs.acme.syslog", Some(&headers), Some(9), 1);
    assert!(invalid.is_empty());
    assert_eq!(arrival.ingestion_time, Some(IngestionTime::Reported(5)));
}

#[test]
fn a_header_that_does_not_parse_is_left_out_and_reported() {
    let cases: [(&[(&str, &str)], InvalidHeader); 14] = [
        (&[(RECORD_ID, "")], InvalidHeader::RecordId(String::new())),
        (
            &[(RECORD_ID, "+5")],
            InvalidHeader::RecordId("+5".to_owned()),
        ),
        (
            &[(RECORD_ID, "-1")],
            InvalidHeader::RecordId("-1".to_owned()),
        ),
        (
            &[(RECORD_ID, "7a")],
            InvalidHeader::RecordId("7a".to_owned()),
        ),
        (
            &[(RECORD_ID, "18446744073709551616")],
            InvalidHeader::RecordId("18446744073709551616".to_owned()),
        ),
        (
            &[(RECORD_KIND, "Log")],
            InvalidHeader::RecordKind("Log".to_owned()),
        ),
        (
            &[(RECORD_KIND, "trace")],
            InvalidHeader::RecordKind("trace".to_owned()),
        ),
        (&[(TENANT, "")], InvalidHeader::Tenant),
        (&[(TENANT, "a\tb")], InvalidHeader::Tenant),
        (
            &[(INGESTION_TIME, "soon"), (INGESTION_TIME_KIND, "reported")],
            InvalidHeader::Time("soon".to_owned()),
        ),
        (
            &[(INGESTION_TIME, "+5"), (INGESTION_TIME_KIND, "reported")],
            InvalidHeader::Time("+5".to_owned()),
        ),
        (
            &[(INGESTION_TIME, "5"), (INGESTION_TIME_KIND, "Reported")],
            InvalidHeader::Kind("Reported".to_owned()),
        ),
        (
            &[(INGESTION_TIME, "5")],
            InvalidHeader::Unpaired(INGESTION_TIME),
        ),
        (
            &[(INGESTION_TIME_KIND, "clock")],
            InvalidHeader::Unpaired(INGESTION_TIME_KIND),
        ),
    ];
    for (pairs, problem) in cases {
        let headers = map(pairs);
        let (arrival, invalid) = arrival_of("processed", Some(&headers), Some(9), 1);
        assert_eq!(invalid, vec![problem.clone()], "{pairs:?}");
        assert_eq!(arrival.record_id, None, "{pairs:?}");
        assert_eq!(arrival.kind, None, "{pairs:?}");
        assert_eq!(arrival.tenant, None, "{pairs:?}");
        assert_eq!(
            arrival.ingestion_time,
            Some(IngestionTime::Reported(9)),
            "{pairs:?}: the publish time stands"
        );
    }
}

#[test]
fn a_bad_time_and_a_bad_kind_are_both_reported() {
    let headers = map(&[(INGESTION_TIME, "x"), (INGESTION_TIME_KIND, "y")]);
    let (arrival, invalid) = arrival_of("processed", Some(&headers), None, 1);
    assert_eq!(
        invalid,
        vec![
            InvalidHeader::Time("x".to_owned()),
            InvalidHeader::Kind("y".to_owned())
        ]
    );
    assert_eq!(arrival.ingestion_time, None);
}

#[test]
fn a_header_given_twice_is_refused() {
    let mut headers = map(&[(TENANT, "acme"), (RECORD_ID, "7"), (RECORD_KIND, "log")]);
    headers.append(TENANT, "beta");
    headers.append(RECORD_ID, "7");
    headers.append(RECORD_KIND, "metric");
    let (arrival, invalid) = arrival_of("processed", Some(&headers), None, 1);
    assert_eq!(
        invalid,
        vec![
            InvalidHeader::Repeated(RECORD_ID),
            InvalidHeader::Repeated(RECORD_KIND),
            InvalidHeader::Repeated(TENANT),
        ]
    );
    assert_eq!(arrival.record_id, None);
    assert_eq!(arrival.kind, None);
    assert_eq!(arrival.tenant, None);
}

#[test]
fn a_time_header_given_twice_is_counted_once_and_its_partner_only_if_malformed() {
    let twice = |name, pairs: &[(&str, &str)]| {
        let mut headers = map(pairs);
        headers.append(name, "6");
        headers
    };
    let cases = [
        (
            twice(INGESTION_TIME, &[(INGESTION_TIME, "5")]),
            vec![InvalidHeader::Repeated(INGESTION_TIME)],
        ),
        (
            twice(
                INGESTION_TIME,
                &[(INGESTION_TIME, "5"), (INGESTION_TIME_KIND, "reported")],
            ),
            vec![InvalidHeader::Repeated(INGESTION_TIME)],
        ),
        (
            twice(
                INGESTION_TIME_KIND,
                &[(INGESTION_TIME, "5"), (INGESTION_TIME_KIND, "reported")],
            ),
            vec![InvalidHeader::Repeated(INGESTION_TIME_KIND)],
        ),
        (
            twice(
                INGESTION_TIME,
                &[(INGESTION_TIME, "5"), (INGESTION_TIME_KIND, "bogus")],
            ),
            vec![
                InvalidHeader::Repeated(INGESTION_TIME),
                InvalidHeader::Kind("bogus".to_owned()),
            ],
        ),
        (
            twice(
                INGESTION_TIME_KIND,
                &[(INGESTION_TIME, "soon"), (INGESTION_TIME_KIND, "clock")],
            ),
            vec![
                InvalidHeader::Time("soon".to_owned()),
                InvalidHeader::Repeated(INGESTION_TIME_KIND),
            ],
        ),
    ];
    for (headers, expected) in cases {
        let (arrival, invalid) = arrival_of("processed", Some(&headers), Some(9), 1);
        assert_eq!(invalid, expected, "{headers:?}");
        assert_eq!(arrival.ingestion_time, Some(IngestionTime::Reported(9)));
    }
}

#[test]
fn a_header_tenant_the_subject_overrides_is_not_read() {
    let mut repeated = map(&[(TENANT, "acme")]);
    repeated.append(TENANT, "beta");
    for headers in [map(&[(TENANT, "")]), repeated] {
        let (arrival, invalid) = arrival_of("logs.acme.syslog", Some(&headers), None, 1);
        assert!(
            invalid.is_empty(),
            "{headers:?}: ignored regardless, so not counted"
        );
        assert_eq!(arrival.tenant.as_deref(), Some("acme"));
    }
}

#[test]
fn a_tenant_that_cannot_be_a_header_is_never_written() {
    let mut meta = meta(IngestionTime::Reported(5));
    meta.tenant = "a\nb".into();
    let written = headers::for_meta(&meta);
    assert_eq!(value(&written, TENANT), None, "left out, not a panic");
    assert_eq!(value(&written, INGESTION_TIME), Some("5"));
}

mod dead_letter {
    use super::*;

    fn failure(error: &str) -> Failure {
        Failure {
            node: "out".to_owned(),
            record_id: Some(RecordId(7)),
            kind: FailureKind::SinkError,
            error: error.to_owned(),
        }
    }

    fn letter<'a>(headers: Option<&'a HeaderMap>, failure: &'a Failure) -> DeadLetter<'a> {
        DeadLetter {
            stream: "LOGS",
            stream_sequence: 42,
            subject: "logs.acme.syslog",
            headers,
            record_id: Some(RecordId(7)),
            kind: Some(Kind::Log),
            tenant: "acme",
            ingestion_time: Some(IngestionTime::Reported(9)),
            failure,
        }
    }

    #[test]
    fn names_the_node_and_error_the_subject_and_the_message() {
        let failure = failure("publish failed");
        let written = headers::for_dead_letter(&letter(None, &failure));
        assert_eq!(value(&written, DLQ_REASON), Some("out: publish failed"));
        assert_eq!(value(&written, DLQ_SUBJECT), Some("logs.acme.syslog"));
        assert_eq!(value(&written, MSG_ID), Some("LOGS:42"));
    }

    #[test]
    fn carries_the_meta_the_arrival_gave_so_a_replay_keeps_it() {
        let failure = failure("x");
        let written = headers::for_dead_letter(&letter(None, &failure));
        assert_eq!(value(&written, RECORD_ID), Some("7"));
        assert_eq!(value(&written, RECORD_KIND), Some("log"));
        assert_eq!(value(&written, TENANT), Some("acme"));
        assert_eq!(value(&written, INGESTION_TIME), Some("9"));
        assert_eq!(value(&written, INGESTION_TIME_KIND), Some("reported"));

        let mut bare = letter(None, &failure);
        bare.record_id = None;
        bare.kind = None;
        bare.ingestion_time = None;
        let written = headers::for_dead_letter(&bare);
        assert_eq!(value(&written, RECORD_ID), None);
        assert_eq!(value(&written, RECORD_KIND), None);
        assert_eq!(value(&written, INGESTION_TIME), None);
        assert_eq!(value(&written, INGESTION_TIME_KIND), None);
    }

    #[test]
    fn a_line_break_in_the_error_is_a_space_not_a_panic() {
        let failure = failure("bad\r\nthing\u{7}");
        let written = headers::for_dead_letter(&letter(None, &failure));
        assert_eq!(value(&written, DLQ_REASON), Some("out: bad  thing "));
    }

    #[test]
    fn a_long_error_is_cut_at_the_cap_on_a_character_boundary() {
        let failure = failure(&"é".repeat(10 * 1024));
        let written = headers::for_dead_letter(&letter(None, &failure));
        let reason = value(&written, DLQ_REASON).expect("reason");
        assert!(reason.len() <= REASON_CAP, "{}", reason.len());
        assert!(reason.len() > REASON_CAP - 2, "{}", reason.len());
        assert_eq!(REASON_CAP, 1024);
    }

    #[test]
    fn keeps_the_producers_headers_but_not_nats_or_its_own_pipeline_headers() {
        let original = map(&[
            ("traceparent", "00-abc-def-01"),
            ("Nats-Expected-Stream", "LOGS"),
            ("Nats-Msg-Id", "producer-1"),
            (TENANT, "spoofed"),
            (RECORD_ID, "99"),
            (RECORD_KIND, "not a kind"),
            (DLQ_REASON, "old reason"),
        ]);
        let failure = failure("x");
        let written = headers::for_dead_letter(&letter(Some(&original), &failure));
        assert_eq!(value(&written, "traceparent"), Some("00-abc-def-01"));
        assert_eq!(value(&written, "Nats-Expected-Stream"), None);
        assert_eq!(value(&written, MSG_ID), Some("LOGS:42"));
        assert_eq!(value(&written, TENANT), Some("acme"));
        assert_eq!(value(&written, RECORD_ID), Some("7"), "the arrival's");
        assert_eq!(value(&written, RECORD_KIND), Some("log"), "the arrival's");
        assert_eq!(value(&written, DLQ_REASON), Some("out: x"));
        assert_eq!(written.get_all(TENANT).count(), 1);
        assert_eq!(written.get_all(RECORD_ID).count(), 1);
    }
}
