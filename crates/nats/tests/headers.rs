//! Pipeline headers through the crate's public helpers: what the sink writes for a `Meta`,
//! and the arrival the source builds from a subject, headers and the JetStream message info.
//! The live round trip is in `jetstream.rs`.

use async_nats::HeaderMap;
use fusion_core::meta::{Arrival, IngestionTime, Meta};
use fusion_core::record::RecordId;
use fusion_nats::headers::{
    self, INGESTION_TIME, INGESTION_TIME_KIND, InvalidHeader, Message, TENANT,
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
fn arrival(
    subject: &str,
    headers: Option<&HeaderMap>,
    published: Option<u64>,
    delivered: u64,
) -> headers::Arrived {
    headers::arrival(Message {
        subject,
        tenant_prefix: "logs",
        headers,
        published,
        delivered,
    })
}

fn value<'h>(headers: &'h HeaderMap, name: &str) -> Option<&'h str> {
    headers.get(name).map(|v| v.as_str())
}

#[test]
fn the_sink_writes_the_tenant_the_time_and_its_kind_and_nothing_else() {
    let written = headers::for_meta(&meta(IngestionTime::Reported(9_000_000_000)));
    assert_eq!(value(&written, TENANT), Some("acme"));
    assert_eq!(value(&written, INGESTION_TIME), Some("9000000000"));
    assert_eq!(value(&written, INGESTION_TIME_KIND), Some("reported"));
    assert_eq!(written.len(), 3, "no record id, no delivery count");
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
        let arrived = arrival("processed.logs", Some(&written), Some(1), 1);
        assert_eq!(
            arrived.arrival,
            Arrival {
                tenant: Some("acme".to_owned()),
                ingestion_time: Some(time),
                delivery_count: 1,
            }
        );
        assert!(arrived.invalid.is_empty());
    }
}

#[test]
fn a_message_with_no_headers_takes_the_subjects_tenant_and_the_publish_time() {
    let arrived = arrival("logs.acme.syslog", None, Some(9_000_000_000), 2);
    assert_eq!(
        arrived.arrival,
        Arrival {
            tenant: Some("acme".to_owned()),
            ingestion_time: Some(IngestionTime::Reported(9_000_000_000)),
            delivery_count: 2,
        }
    );
    assert!(arrived.invalid.is_empty());
}

#[test]
fn the_subjects_tenant_wins_over_the_header() {
    let headers = map(&[(TENANT, "beta")]);
    let arrived = arrival("logs.acme.syslog", Some(&headers), None, 1);
    assert_eq!(arrived.arrival.tenant.as_deref(), Some("acme"));
}

#[test]
fn the_header_tenant_is_used_when_the_subject_names_none() {
    let headers = map(&[(TENANT, "beta")]);
    let arrived = arrival("processed", Some(&headers), None, 1);
    assert_eq!(arrived.arrival.tenant.as_deref(), Some("beta"));
}

#[test]
fn the_header_time_wins_over_the_publish_time() {
    let headers = map(&[(INGESTION_TIME, "5"), (INGESTION_TIME_KIND, "reported")]);
    let arrived = arrival("logs.acme.syslog", Some(&headers), Some(9), 1);
    assert_eq!(
        arrived.arrival.ingestion_time,
        Some(IngestionTime::Reported(5))
    );
}

#[test]
fn a_header_that_does_not_parse_is_left_out_and_reported() {
    let cases: [(&[(&str, &str)], InvalidHeader); 7] = [
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
        let arrived = arrival("processed", Some(&headers), Some(9), 1);
        assert_eq!(arrived.invalid, vec![problem.clone()], "{pairs:?}");
        assert_eq!(arrived.arrival.tenant, None, "{pairs:?}");
        assert_eq!(
            arrived.arrival.ingestion_time,
            Some(IngestionTime::Reported(9)),
            "{pairs:?}: the publish time stands"
        );
    }
}

#[test]
fn a_bad_time_and_a_bad_kind_are_both_reported() {
    let headers = map(&[(INGESTION_TIME, "x"), (INGESTION_TIME_KIND, "y")]);
    let arrived = arrival("processed", Some(&headers), None, 1);
    assert_eq!(
        arrived.invalid,
        vec![
            InvalidHeader::Time("x".to_owned()),
            InvalidHeader::Kind("y".to_owned())
        ]
    );
    assert_eq!(arrived.arrival.ingestion_time, None);
}

#[test]
fn a_header_given_twice_is_refused() {
    let mut headers = map(&[(TENANT, "acme")]);
    headers.append(TENANT, "beta");
    let arrived = arrival("processed", Some(&headers), None, 1);
    assert_eq!(arrived.invalid, vec![InvalidHeader::Repeated(TENANT)]);
    assert_eq!(arrived.arrival.tenant, None);
}

#[test]
fn a_tenant_that_cannot_be_a_header_is_never_written() {
    let mut meta = meta(IngestionTime::Reported(5));
    meta.tenant = "a\nb".into();
    let written = headers::for_meta(&meta);
    assert_eq!(value(&written, TENANT), None, "left out, not a panic");
    assert_eq!(value(&written, INGESTION_TIME), Some("5"));
}
